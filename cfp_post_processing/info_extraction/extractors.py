import re
import flair
import string
from collections import defaultdict
from flair.data import Sentence
from flair.models import SequenceTagger
from .ie_utils import Line, TxFn, full_clean


class BlockExtractor:
    """Extract groupings of relevant information chunks, with a singular role label
    """

    def __init__(self, cur, extraction_type):
        self.cur = cur
        # extraction_type of either 'gold' or 'dl_prediction'
        self.extract_type = extraction_type

    def get_relevant_lines(self, conf_id: int):
        """ Get lines with label != undefined for Conference
        """
        page_ids = self.cur.execute(
            'SELECT id FROM ConferencePages WHERE conf_id={}'.format(conf_id)).fetchall()
        page_ids = [p[0] for p in page_ids]  # Page_id retrieval is tuple
        all_lines = []
        for page_id in page_ids:
            if self.extract_type == 'dl_prediction':
                lines = self.cur.execute("SELECT * FROM PageLines WHERE \
                                  page_id=? AND line_text!='' AND (dl_prediction!=?) \
                                  ORDER BY id", (page_id, 'Undefined')).fetchall()
            elif self.extract_type == 'gold':
                lines = self.cur.execute("SELECT * FROM PageLines WHERE \
                                  page_id=? AND line_text!='' AND label!=? \
                                  ORDER BY id", (page_id, 'Undefined')).fetchall()
            else:  # Undefined extraction type
                return []
            all_lines += [Line(l) for l in lines]
        return all_lines

    def get_relevant_blocks(self, conf_id: int, indent_diff_thresh: int, lnum_diff_thresh: int):
        """ Provides a mapping of Role Labels to Person/Affiliations
        - Groups only for 'Role Label' within threshold of indentation or line_num difference
        - Returns dictionary of {role_label Line : List of Person/Aff Lines/Complex} for further processing
        """

        def within_threshold(line, prev_line, rl_line):
            """ Ensure threshold diffs 
            - between indentation of line and role_label
            - between line_num of line and prev_labelled
            """
            indent_thresh = abs(int(line.indentation) - int(rl_line.indentation)) < indent_diff_thresh
            lnum_thresh = abs(int(line.num) - int(prev_line.num)) < lnum_diff_thresh
            return indent_thresh and lnum_thresh

        relevant_lines = self.get_relevant_lines(conf_id)
        mapping = defaultdict(list)

        role_label: 'Line' = None
        prev_labelled: 'Line' = None  # Keeps track of last labelled line under current label

        for line in relevant_lines:
            label = line.label if self.extract_type == 'gold' else line.dl_prediction
            if label == "Role-Label":
                role_label = line
                prev_labelled = line
            elif role_label:
                if within_threshold(line, prev_labelled, role_label):
                    if mapping[role_label]:
                        mapping[role_label].append(line)
                    else:
                        mapping[role_label] = [line]
                    prev_labelled = line
            else:
                pass

        return mapping


class LineNERExtractor:
    """Process individual lines with the use of flair
    """
    def __init__(self):
        self.flair_tagger = SequenceTagger.load('ner')

    def split_line(self, line: 'Line'):
        """ Splits line into potential entity phrases
        - Iteratively split by bracketed text
        - Split by commas
        """
        b_text = re.compile('[\(](.*?)[\)]')  # Regex for brackets
        ltext = line.text
        split_text = []
        bracketed = b_text.search(ltext)
        start_idx, end_idx = None, None
        while bracketed:
            start_idx, end_idx = bracketed.span()[0], bracketed.span()[1]
            split_text += ltext[:start_idx].split(',')
            split_text += bracketed.group(1).split(',')
            ltext = ltext[end_idx:]
            bracketed = b_text.search(ltext)
        if end_idx:
            split_text += ltext[end_idx:].split(',')
        else:
            split_text += ltext.split(',')

        split_text = [s.strip() for s in split_text]
        split_text = list(filter(lambda s: s != '', split_text))
        return split_text

    def get_line_parts_flair(self, line: 'Line'):
        """ Split by comma since Flair is insensitive to commas
        """
        line_parts = defaultdict(lambda: None)
        for part in self.split_line(line):
            part = Sentence(part)
            self.flair_tagger.predict(part)
            for entity in part.get_spans('ner'):
                # Currently not saving for multiple ner extractions
                if not line_parts[entity.tag]:
                    line_parts[entity.tag] = entity.text
                print(f"{entity.text}, {entity.tag}| ", end="")
        print()
        return line_parts


class LineInfoExtractorBase:
    """ Base class for LineInfoExtractors for websites and proceedings
    """

    def __init__(self, cur, extract_type):
        self.cur = cur
        # extraction_type of either 'gold' or 'dl_predicted'
        self.extract_type = extract_type
        # Set during block processing
        self.conference = None
        # NER
        self.line_ner_extractor = LineNERExtractor()

    def add_person(self, person: str):
        self.cur.execute(
            "INSERT INTO Persons (name) VALUES (?)", (person,))
        sql_oid = self.cur.lastrowid
        return sql_oid

    def add_organization(self, org: str):
        self.cur.execute(
            "INSERT INTO Organizations (name) VALUES (?)", (org,))
        sql_oid = self.cur.lastrowid
        return sql_oid

    def add_affiliation_rel(self, person_id: 'Tuple', org_id: 'Tuple'):
        self.cur.execute("INSERT OR IGNORE INTO PersonOrganization (org_id, person_id)\
                VALUES (?, ?)", (org_id, person_id))

    def add_role_rel(self, person_id: 'Tuple', role: str):
        self.cur.execute("INSERT OR IGNORE INTO PersonRole (role_type, conf_id, person_id)\
                VALUES (?, ?, ?)", (role, self.sql_conf_id, person_id))

    def update_org_loc(self, org_id: int, loc: str):
        self.cur.execute(
            "UPDATE Organizations SET location=? WHERE id=?", (loc, org_id))


class LineInfoExtractor(LineInfoExtractorBase):
    """ Extract Person/Organization/Conference-Role relationships for individual conferences
    - TODO Retrieve Complex Line containing Role Information
    - TODO Spelling Correction for countries to prevent classification as organization
    - TODO Handle multiple Person/Organization/Role extraction for individual lines
    - TODO Save unprocessed affiliations?
    """

    def __init__(self, cur, extract_type):
        super(LineInfoExtractor, self).__init__(
            cur, extract_type)

    def get_line_parts(self, line: 'Line'):
        return self.line_ner_extractor.get_line_parts_flair(line)

    def process_complex(self, line: 'Line', role_label: 'Line'):
        """ Processes Complex Line
        - Adds Person to Conference
        """
        line_parts = self.get_line_parts(line)
        if line_parts['PER']:
            person_id = self.add_person(line_parts['PER'])
            self.add_role_rel(person_id, role_label.text)
        if line_parts['ORG']:
            org_id = self.add_organization(line_parts['ORG'])
            if line_parts['LOC']:
                self.update_org_loc(org_id, line_parts['LOC'])

        if line_parts['PER'] and line_parts['ORG']:  # Add affiliation relation
            self.add_affiliation_rel(person_id, org_id)

    def process_person(self, person: 'Line', affiliation: 'Line', role_label: 'Line'):
        """ Creates affiliation relation between Person and Organization
        - Adds Person to Conference
        """
        person_id = self.add_person(full_clean(person.text))
        self.add_role_rel(person_id, role_label.text)
        if affiliation: # None for processing of person only
            print("{}, PER| ".format(full_clean(person.text)), end='')
            line_parts = self.get_line_parts(affiliation)
            if line_parts['ORG']:
                org_id = self.add_organization(line_parts['ORG'])
                if line_parts['LOC']:
                    self.update_org_loc(org_id, line_parts['LOC'])
                self.add_affiliation_rel(person_id, org_id)
        else:
            print("{}, PER| ".format(full_clean(person.text)))

    def process_block(self, role_label: 'Line', content_lines: 'List[Line]'):
        """ Processes singular block of PageLine ids corresponding to role label and following content
        """
        print("================= {} =============".format(role_label.text))
        cur_idx = 0
        u_person, u_aff = None, None
        for cur_line in content_lines:

            label = cur_line.label if self.extract_type == 'gold' else cur_line.dl_prediction

            if label == 'Complex':  # Assume contains person and affiliation
                self.process_complex(cur_line, role_label)
            else:
                if label == 'Person':
                    if u_person:  # If there is already a person just add first
                        person_id = self.process_person(cur_line, None, role_label)
                    u_person = cur_line
                elif label == 'Affiliation':
                    u_aff = cur_line
                    if u_person:  # Should pair person with affiliation
                        self.process_person(u_person, u_aff, role_label)
                        u_person, u_aff = None, None
                else:
                    print("Unexpected Label: {} [{}]".format(
                        cur_line.label, cur_line.text))

            cur_idx += 1
            prev_line = cur_line

    def process_conference(self, conference: 'Conference'):
        """ Processes relevant retrieved from BlockExtractor
        """
        self.conference = conference
        self.sql_conf_id = conference.id
        # Process relevant blocks of conference
        for rl_id, content_ids in conference.blocks.items():
            self.process_block(rl_id, content_ids)


class LineInfoExtractor_P(LineInfoExtractorBase):
    """Line Information Extractor for PDF proceedings
    """

    def __init__(self, cur, extract_type):
        super(LineInfoExtractor_P, self).__init__(
            cur, extract_type)

    def valid_person_name(self, person: str):
        """Check for validity of person name, only allow English names for now
        """
        if re.findall(r'[\u4e00-\u9fff]+', person): # Check for non English characters
            return False
        if len((person).split(" ")) < 5 and len(person) < 20:
            return True
        return False

    def process_persons(self, lines: '[Line]', role_label: 'Line'):
        """ Process lines containing multiple persons (Assume comma separation and no organizations)
        """
        consolidated = " ".join(list(map(lambda l: l.text, lines)))
        persons = list(map(lambda p: full_clean(p), consolidated.split(", "))) # Split by commas

        # Split possibly errorneously extracted persons
        split_persons = []
        for person in persons:
            if len(person.split(" ")) > 3:
                # Concatenated lines may contain persons not separated by commas, blindly assume lines longer than usual
                # contain 2 person names for now
                tokens = person.split(" ")
                split_persons.append(" ".join(tokens[:2]))
                split_persons.append(" ".join(tokens[2:]))
            else:
                split_persons.append(person)

        # Clean Person name and add to database
        for person in split_persons:
            person = full_clean(person)
            if self.valid_person_name(person):
                person_id = self.add_person(person)
                self.add_role_rel(person_id, role_label.text)
                self.add
                print(person)

    def process_person(self, person: 'Line', role_label: 'Line'):
        """ Add Person and corresponding role to database
        """
        person = full_clean(person.text)
        if self.valid_person_name(person):
            person_id = self.add_person(person)
            self.add_role_rel(person_id, role_label.text)
            print(person)

    def process_complex(self, line: 'Line', role_label: 'Line'):
        """ Specialized processing of Complex Line for Proceedings
        - Assumes format of line falls within one of predefined types below
        """
        if re.match('[^,]+\(.+\)', line.text):
            """ Line is of format: <NAME> (<ORG>, <ORG/LOC>,* <LOC>)
            """
            person = line.text.split("(")[0]
            org_loc = re.search('\((.*?)\)', line.text).group(1)
            org = org_loc.split(",")[0]
        elif re.match('[^,]+,[^,]+\(.+\)', line.text):
            """ Line is of format: <LAST NAME>, <FIRST NAME> (<ORG>)
            """
            person_tokens = re.search('(.*?)\(', line.text).group(1)
            person = " ".join(reversed(person_tokens.split(",")))
            org_loc = re.search('\((.*?)\)', line.text).group(1)
            org = org_loc.split(",")[0]
        else:
            """ Line is of format: <NAME>, <ORG> (ORG-ABBREV)*
            - Assume structure of line to be: PER, ORG
            - Replace 'U' with 'University', e.g. U of xxx, xxx U
            - Remove bracketed abbreviation, e.g. Carnegie Mellon University (CMU)
            """
            subbed = re.sub('U ', 'University ', line.text)
            if subbed[-1] == "U":
                subbed = subbed + 'niversity'
            subbed = re.sub('\([(a-zA-Z0-9-)]*\)', '', subbed)

            if " - " in subbed:
                tokens = subbed.split("-")
            elif ", " in subbed:
                tokens = subbed.split(",")
            else:
                print("========= Missed: {}".format(line.text))
                return
            person, org = tokens[0], tokens[1]

        person = full_clean(person)
        if self.valid_person_name(person):
            person_id = self.add_person(person)
            org_id = self.add_organization(full_clean(org))
            self.add_role_rel(person_id, role_label.text)
            self.add_affiliation_rel(person_id, org_id)
            print(person, "| PER,", org, "| ORG")

    def process_block(self, role_label: 'Line', content_lines: 'List[Line]'):
        """ Processes singular block of PageLine ids corresponding to role label and following content
        """
        print("================= {} =============".format(role_label.text))
        cur_idx = 0
        person_lines = []
        for cur_line in content_lines:
            label = cur_line.label if self.extract_type == 'gold' else cur_line.dl_prediction

            if label != 'Person':  # Non person label, process consolidated person lines
                if person_lines:
                    self.process_persons(person_lines, role_label)
                    person_lines = []
                if label == 'Complex':  # Assume contains person and affiliation
                    self.process_complex(cur_line, role_label)
            else:  # Person label, just consolidate line
                if person_lines or len(cur_line.text.split(" ")) > 5:
                    person_lines.append(cur_line)
                else:
                    self.process_person(cur_line, role_label)

            cur_idx += 1
            prev_line = cur_line

    def process_conference(self, conference: 'Conference'):
        """ Processes relevant retrieved from BlockExtractor
        """
        self.conference = conference
        self.sql_conf_id = conference.id
        # Process relevant blocks of conference
        for rl_id, content_ids in conference.blocks.items():
            self.process_block(rl_id, content_ids)
