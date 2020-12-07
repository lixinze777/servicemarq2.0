import editdistance
import scholarly
import requests
import xml.etree.ElementTree as ET


class Person:
    """Person information from external API search
    """

    def __init__(self, tuple_data):
        self.type = tuple_data[0]
        self.id = tuple_data[1]
        self.name = tuple_data[2]
        self.aff = tuple_data[3]
        self.topics = tuple_data[4]


class API:
    def __init__(self, orcid: bool, aminer: bool, gscholar: bool, dblp: bool):
        """API for extraction of external IDs

        Args:
            orcid (bool): Whether to extract orcid
            aminer (bool): Whether to extract aminer id
            gscholar (bool): Whether to extract gscholar id
            dblp (bool): Whether to extract dblp id
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3)\
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
        self.orcid = orcid
        self.aminer = aminer
        self.gscholar = gscholar
        self.dblp = dblp

    def aminer_person(self, name: str, num_results):
        """Retrieve aminer results
        """
        response = requests.get(f"https://api.aminer.org/api/search/person?query={name}",
                                headers=self.headers)
        result = response.json()['result']

        collated_data = []
        for person_data in result[0: num_results]:
            person_id = person_data['id']
            name = person_data['name']
            affiliation = person_data['aff']
            if 'desc' in affiliation.keys():
                affiliation = [affiliation['desc']]
            elif 'desc_zh' in affiliation.keys():
                affiliation = [affiliation['desc_zh']]
            else:
                affiliation = ["Not Available"]
            if 'tags' in person_data.keys():
                tags = [tag['t'] for tag in person_data['tags']]
            else:
                tags = []
            collated_data.append((person_id, name, affiliation, tags))

        return collated_data

    def gscholar_person(self, name: str, num_results: int):
        """Retrieve google scholar results
        """
        authors = scholarly.search_author(name)
        i = 0
        author = next(authors, None)
        collated_data = []
        while author and i < num_results:
            author_id = author.id
            name = author.name
            affiliation = [author.affiliation]
            interests = author.interests
            collated_data.append((author_id, name, affiliation, interests))
            author = next(authors, None)
            i += 1

        return collated_data

    def orcid_person(self, name: str, num_results):
        """ Retrieves orcId results
        """
        start_idx, end_idx = 0, num_results  # Limit to first 3 searches due to time constraint
        search_res = requests.get(f"https://pub.orcid.org/v3.0/search/?q={name}\
                                    &start={start_idx}&rows={end_idx}")
        search_root = ET.fromstring(search_res.text)

        namespaces = {'common': 'http://www.orcid.org/ns/common',
                      'person': 'http://www.orcid.org/ns/person',
                      'personal-details': 'http://www.orcid.org/ns/personal-details',
                      'activities': 'http://www.orcid.org/ns/activities',
                      'keyword': 'http://www.orcid.org/ns/keyword'}

        potential_matches = {}

        collated_data = []
        for el in search_root.findall('*//common:path', namespaces):
            orcid = el.text
            data = requests.get(f"https://pub.orcid.org/v3.0/{orcid}")
            data_root = ET.fromstring(data.text)
            given_name_el = data_root.find(
                '*//personal-details:given-names', namespaces)
            given_name = given_name_el.text if given_name_el != None else ""
            family_name_el = data_root.find(
                '*//personal-details:family-name', namespaces)
            family_name = family_name_el.text if family_name_el != None else ""
            affiliations = data_root.findall(
                '*//activities:affiliation-group//common:name', namespaces)
            affiliations = list(set([aff.text for aff in affiliations]))
            keywords = data_root.findall('*//keyword:content', namespaces)
            keywords = list(set([kw.text for kw in keywords]))

            collated_data.append(
                (orcid, f"{given_name} {family_name}", affiliations, keywords))

        return collated_data

    def dblp_person(self, name: str, num_results):
        """ Retrieves dblp results
        """
        search_res = requests.get(f"http://dblp.org/search/author/api?q={name}&h={num_results}",
                                  headers=self.headers)
        search_root = ET.fromstring(search_res.text)

        collated_data = []
        for el in search_root.findall('*//hit'):
            dblp_url = el.find('./info/url').text
            name = el.find('*//author').text

            collated_data.append((dblp_url, name, [], []))

        return collated_data

    def get_person_results(self, person_id: int, name: str, org: str, num_to_search: int, logger: 'Logger'):
        """Get Person information from external API

        Args:
            person_id (int): Database ID of person
            name (str): Name of person
            org (str): Organization of person
            num_to_search (int): Maximum number of results to retrieve from APIs
            logger (logger.Logger): Logger to keep track of failed extractions

        Returns:
            [Person]: List of Person
        """

        results = []
        if self.orcid:
            orcid_results = self.orcid_person(name, num_to_search)
            results += [('orcid', *orcid_result) for orcid_result in orcid_results]
        if self.aminer:
            aminer_results = self.aminer_person(name, num_to_search)
            results += [('aminer_id', *aminer_result) for aminer_result in aminer_results]
        if self.dblp:
            dblp_results = self.dblp_person(name, num_to_search)
            results += [('dblp_id', *dblp_result) for dblp_result in dblp_results]
        if self.gscholar:
            try:
                gscholar_results = self.gscholar_person(name, num_to_search)
                results += [('gscholar_id', *gscholar_result) for gscholar_result in gscholar_results]
            except:
                logger.warn('Failed gscholar retrieval on {}'.format(person_id))
        return [Person(result) for result in results]

    @staticmethod
    def similarity(name1, name2):
        """Computes similarity between the extracted name from external API (1) and
           original name in database (2) based on editdistance
        - Permute name to account for difference in First/Last name ordering
        - Check that tokens in original is subset of extracted (assume names from external API are more complete)
        """
        n1_tokens = name1.split(" ")
        n2_tokens = name2.split(" ")
        # First token to last position
        permute1 = " ".join(n1_tokens[1:] + [n1_tokens[0]])
        # Last token to first position
        permute2 = " ".join([n1_tokens[-1]] + n1_tokens[:-1])

        if len(n2_tokens) > 1 and set(n2_tokens).issubset(set(n1_tokens)): # Assume name match if token subset
            return 1
        return min(
            editdistance.eval(name1.lower(), name2.lower()),
            editdistance.eval(permute1.lower(), name2.lower()),
            editdistance.eval(permute2.lower(), name2.lower())
        )
