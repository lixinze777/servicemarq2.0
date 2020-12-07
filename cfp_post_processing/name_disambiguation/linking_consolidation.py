import argparse
import logging
import pickle
import sqlite3
from external_api import API
from tfidf_clustering import Clustering
from threading import Thread
from nd_utils import clean_punctuation, DatabaseHelper


class Consolidator:

    def __init__(self, original_db_cnx, consolidated_db_cnx, clustering, logger):
        self.original_db_cnx = original_db_cnx
        self.consolidated_db_cnx = consolidated_db_cnx
        self.clustering = clustering
        self.logger = logger

    def org_cluster_rep(self, ent: str):
        """ Retrieves the cluster representative of the given organization
        """
        ent = clean_punctuation(ent)
        org_idx = self.clustering.ent_to_idx[ent]
        org_cluster = self.clustering.idx_to_cluster[org_idx]
        cluster_rep_idx = self.clustering.cluster_to_idx[org_cluster][0]
        cluster_rep = self.clustering.idx_to_ent[cluster_rep_idx]
        return cluster_rep

    def remove_duplicates(self, person_info: 'List'):
        """ Processes org to corresponding cluster_rep and keep only distinct Person-Org-Role tuples
        """
        person_info = [(person, self.org_cluster_rep(org), role)
                       for person, org, role in person_info]
        deduplicated = []
        for p_tuple in person_info:
            if p_tuple not in deduplicated:
                deduplicated.append(p_tuple)
        return deduplicated

    def disambiguate_organizations(self):
        """Consolidate Person/Org data post TF-IDF disambiguation of Organizations
        """
        original_db_cur = self.original_db_cnx.cursor()
        consolidated_db_cur = self.consolidated_db_cnx.cursor()

        conference_ids = original_db_cur.execute(
            "SELECT id FROM WikicfpConferences ORDER BY id").fetchall()

        for conf_id in conference_ids:
            conf_id = conf_id[0]

            # Process Persons and Organizations
            persons_info = DatabaseHelper.get_persons_info(
                original_db_cur, conf_id)
            for person, org, role in self.remove_duplicates(persons_info):
                org_id = consolidated_db_cur.execute(
                    "SELECT id FROM Organizations WHERE name=?", (org,)).fetchone()
                if not org_id:
                    consolidated_db_cur.execute(
                        "INSERT INTO Organizations (name) VALUES (?)", (org,))
                    org_id = consolidated_db_cur.lastrowid
                else:
                    org_id = org_id[0]  # Fetch tuple

                person_id = consolidated_db_cur.execute(
                    "SELECT id FROM Persons WHERE name=? AND org_id=?", (person, org_id)).fetchone()
                if not person_id:
                    consolidated_db_cur.execute(
                        "INSERT INTO Persons (name, org_id) VALUES (?, ?)", (person, org_id))
                    person_id = consolidated_db_cur.lastrowid
                else:
                    person_id = person_id[0]
                consolidated_db_cur.execute("INSERT INTO PersonRole (role_type, conf_id, person_id) VALUES (?, ?, ?)",
                                            (role, conf_id, person_id))

            self.consolidated_db_cnx.commit()

    def retrieve_external_ids(self, person_id: int, name: str, org: str, num_to_search: int, similarity_threshold: int, results: 'List', index: int):
        """Retrieve IDs from external APIs for Person

        Args:
            person_id (int): Database ID of person
            name (str): Name of Person
            org (str): Organization of Person
            num_to_search (int): Number of results to return
            similarity_threshold (int): Editdistance similarity between database name and retrieved names
            results (List): Mutable list to enable saving of result from thread
            index (int): Index in results to save retrieved information
        """
        retrieved_persons = self.api.get_person_results(
            person_id, name, org, num_to_search, self.logger)
        retrieved_persons = list(filter(lambda p: self.api.similarity(
            p.name, name) < similarity_threshold, retrieved_persons))
        results[index] = retrieved_persons

    def save_external_ids(self, cur: 'sqlite3.cursor', person_id: int, results: 'List'):
        """Save retrieved results for person to database

        Args:
            cur (sqlite3.cursor): Database connection cursor
            person_id (int): Person ID in database
            results (List): Retrieved results
        """
        for person in results:
            existing_id = cur.execute("SELECT {} FROM Persons WHERE id=?".format(
                person.type), (person_id,)).fetchone()[0]
            if not existing_id:
                cur.execute("UPDATE Persons SET {}=? WHERE id=?".format(
                    person.type), (person.id, person_id))

    def process_external_ids(self, num_to_search: int, similarity_threshold: int, num_threads: int):
        """Retrieve IDs from external APIs, amenable to multi-threading

        Args:
            num_to_search (int): Max number of results to be retrieved for each person
            similarity_threshold (int): Editdistance similarity between database name and retrieved name
            num_threads (int): Number of threads to run retrieval concurrently
        """
        consolidated_db_cur = self.consolidated_db_cnx.cursor()
        person_ids = consolidated_db_cur.execute(
            "SELECT id FROM Persons ORDER BY id").fetchall()
        threads = [None for i in range(num_threads)]
        thread_results = [[] for i in range(num_threads)]
        for person_id_index in range(0, len(person_ids), num_threads):
            # Create threads for each person's retrieval
            for thread_index in range(num_threads):
                if (person_id_index + thread_index) >= len(person_ids):  # Break if idx exceeds
                    break
                person_id = person_ids[person_id_index + thread_index][0]
                name, org = consolidated_db_cur.execute("SELECT p.name, o.name FROM Persons p\
                    JOIN Organizations o ON p.org_id=o.id WHERE p.id=?", (person_id,)).fetchone()
                print(person_id, name, org)
                threads[thread_index] = Thread(target=self.retrieve_external_ids,
                                               args=(person_id, name, org, num_to_search, similarity_threshold, thread_results, thread_index))
                threads[thread_index].start()
            # Join threads
            for thread_index in range(num_threads):
                if (person_id_index + thread_index) >= len(person_ids):  # Break if idx exceeds
                    break
                threads[thread_index].join()
                person_id = person_ids[person_id_index + thread_index][0]
                self.save_external_ids(
                    consolidated_db_cur, person_id, thread_results[thread_index])

            self.consolidated_db_cnx.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('clustering_filepath', type=str,
                        help="Pickled Clustering")
    parser.add_argument('original_db_filepath', type=str,
                        help="Database file to disambiguate (No alteration to this db)")
    parser.add_argument('consolidated_db_filepath', type=str,
                        help="New consolidated database file save location")
    args = parser.parse_args()

    # Database connections and Clustering object
    original_db_cnx = sqlite3.connect(args.original_db_filepath)
    consolidated_db_cnx = sqlite3.connect(args.consolidated_db_filepath)
    with open(args.clustering_filepath, 'rb') as cluster_file:
        clustering = pickle.load(cluster_file)

    # Setup logger
    logging.basicConfig(filename='./linking.log',
                        filemode='a', level=logging.WARN)
    logger = logging.getLogger(__name__)

    # Create necessary tables for consolidated data in restructured database
    DatabaseHelper.create_tables(consolidated_db_cnx)

    MOVE_CONFERENCES = False
    DISAMBIGUATE_ORGS = False
    EXTERNAL_ID_RETRIEVAL = False
    print("Process and populate Conferences: {}\
           \nDisambiguate Organizations and populate Person/Organizations: {}\
           \nRetrieve Person External IDs: {}".format(MOVE_CONFERENCES, DISAMBIGUATE_ORGS, EXTERNAL_ID_RETRIEVAL))
    consolidator = Consolidator(original_db_cnx, consolidated_db_cnx, clustering, logger)
    if MOVE_CONFERENCES:
        DatabaseHelper.move_conferences_table(original_db_cnx, consolidated_db_cnx)
    # Populate Person and Organization information after organization disambiguation
    if DISAMBIGUATE_ORGS:
        consolidator.disambiguate_organizations()
    # Population of external IDs
    if EXTERNAL_ID_RETRIEVAL:
        # Specify which IDs to extract
        ORCID, AMINER, GSCHOLAR, DBLP = False, False, False, False
        consolidator.api = API(ORCID, AMINER, GSCHOLAR, DBLP)
        NUM_TO_SEARCH = 3
        SIMILARITY_THRESHOLD = 3
        NUM_THREADS = 32
        consolidator.process_external_ids(NUM_TO_SEARCH, SIMILARITY_THRESHOLD, NUM_THREADS)

    # Close connections
    original_db_cnx.close()
    consolidated_db_cnx.close()
