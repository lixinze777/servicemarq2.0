import re
import string


def clean(ltext: str):
    # Strip leading and trailing punctuation expect brackets for further processing
    ltext = ltext.strip(string.punctuation.replace(')', ''))
    # Replace tabs and newlines with spaces
    ltext = re.sub('\t+|\n+', ' ', ltext)
    ltext = re.sub(' +', ' ', ltext)  # Remove multiple spacing
    ltext = ltext.strip()
    return ltext

# Strip all trailing punctuation
def full_clean(ltext: str):
    return ltext.strip(string.punctuation)

class Conference:
    """ ORM for Conference
    """

    def __init__(self, conference, relevant_blocks: 'Dict[]'):
        """ Initialize Conference object with database tuple
        - relevant_blocks: dictionary of {role_label Line : List of Person/Aff Lines}
        """
        self.clean = clean
        self.id = conference[0]
        self.title = self.clean(conference[2])
        self.year = conference[5]
        self.n4j_attrs = {
            'name': self.title,
            'year': self.year
        }
        self.blocks = relevant_blocks


class Line:
    """ ORM for PageLine
    """

    def __init__(self, pageline):
        self.clean = clean
        self.id = pageline[0]
        self.page_id = pageline[1]
        self.num = pageline[2]
        self.text = self.clean(pageline[3])
        self.tag = pageline[4]
        self.indentation = pageline[5]
        self.label = pageline[6]
        self.dl_prediction = pageline[7]
        self.svm_prediction = pageline[8]

def consolidate_line_nums(relevant_blocks: 'Dict'):
    """Combine lines with the same number within relevant blocks,
    retaining the id, tag, indentation of the first line, only if Person/Affiliation match

    Args:
        relevant_blocks (Dict): Dictionary mapping of role to list of lines

    Returns:
        Dict: relevant_blocks with consolidated lines
    """
    def combine_lines(l1: 'Line', l2: 'Line'):
        assert(l1.num == l2.num)
        return Line(
            (l1.id, l1.page_id, l1.num,
            ', '.join([l1.text, l2.text]),
            l1.tag, l1.indentation,
            'Complex', 'Complex', 'Complex')
        )

    consolidated_blocks = {}
    for role, line_block in relevant_blocks.items():
        consolidated_lines = []
        prev_line = None
        for line in line_block:
            if not prev_line:
                prev_line = line
            elif prev_line.num == line.num and prev_line.dl_prediction == "Person" and line.dl_prediction != "Person":
                prev_line = combine_lines(prev_line, line)
            else:
                consolidated_lines.append(prev_line)
                prev_line = line
        consolidated_lines.append(prev_line) # Add in last line
        consolidated_blocks[role] = consolidated_lines
    return consolidated_blocks

class TxFn:
    """ Transaction Functions for neo4j session
    """
    @staticmethod
    def set_constraints(tx):
        """ Graph constraint settings
        - Person name non-unique, disambiguate
        - Organization name non-unique???, disambiguate
        - Conference name unique, keep
        """
        tx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE;")
        tx.run("CREATE CONSTRAINT ON (o:Organization) ASSERT o.name IS UNIQUE;")
        tx.run("CREATE CONSTRAINT ON (c:Conference) ASSERT c.name IS UNIQUE;")

    @staticmethod
    def create_person(tx, name):
        return tx.run("MERGE (p:Person {name:$name}) RETURN id(p)", name=name).single().value()

    @staticmethod
    def create_organization(tx, name):
        return tx.run("MERGE (o:Organization {name:$name}) RETURN id(o)", name=name).single().value()

    @staticmethod
    def create_conference(tx, attrs):
        return tx.run("MERGE (c:Conference {name:$name, year:$year}) RETURN id(c)",
                      name=attrs['name'], year=attrs['year']).single().value()

    @staticmethod
    def update_org_loc(tx, org_id, loc):
        tx.run("MATCH (o:Organization) WHERE id(o)=$o_id SET o.loc=$loc",
               o_id=org_id, loc=loc)

    @staticmethod
    def create_affiliation_rel(tx, person_id, org_id):
        tx.run("MATCH (p:Person),(o:Organization)\
                      WHERE id(p)=$p_id AND id(o)=$o_id\
                      MERGE (p)-[r:AFFILIATED]->(o)", p_id=person_id, o_id=org_id)

    @staticmethod
    def create_role_rel(tx, person_id, role, conf_id):
        tx.run("MATCH (p:Person),(c:Conference)\
                      WHERE id(p)=$p_id AND id(c)=$c_id\
                      MERGE (p)-[r:ROLE {type:$role}]->(c)",
               p_id=person_id, role=role, c_id=conf_id)

    @staticmethod
    def get_all_conference_info(tx, attrs):
        return tx.run("MATCH (c:Conference {name:$name, year:$year})-[role]-(p)-[aff]-(o) RETURN c,p,o",
                      name=attrs['name'], year=attrs['year']).value()
