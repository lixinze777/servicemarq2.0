import argparse
import numpy as np
import sqlite3


class PageRankScorer:

    def __init__(self, cnx):
        self.cnx = cnx
        self.add_pr_score_col()

    def person_role_score(self, role_type: str):
        """ Compute the score of role of person
        """
        return 2 if "chair" in role_type.lower() else 1

    def add_pr_score_col(self):
        cur = self.cnx.cursor()
        try:
            cur.execute("ALTER TABLE Persons ADD pr_score REAL")
        except:
            print("ALTER TABLE FAILED: Person pr_score column already exists")
        try:
            cur.execute("ALTER TABLE WikicfpConferences ADD pr_score REAL")
        except:
            print("ALTER TABLE FAILED: WikicfpConferences pr_score column already exists")
        try:
            cur.execute("ALTER TABLE Organizations ADD pr_score REAL")
        except:
            print("ALTER TABLE FAILED: Organizations pr_score column already exists")

    def compute_matrices(self):
        """Compute the necessary matrices for Persons and Conferences
        Adjacency matrix of dimension max(conference_ids)+max(person_ids)
        Adjacency list for faster computation since matrix is sparse
        """
        cur = self.cnx.cursor()
        conf_ids = cur.execute("SELECT id FROM WikicfpConferences ORDER BY id").fetchall()
        conf_ids = [conf_id[0] for conf_id in conf_ids]
        person_ids = cur.execute("SELECT id, name FROM Persons ORDER BY id").fetchall()
        person_ids = [person_id[0] for person_id in person_ids]
        num_nodes = max(conf_ids) + max(person_ids) + 1  # Db start index is 1
        adj_list = [[] for i in range(num_nodes)]
        adj_matrix = np.zeros((num_nodes, num_nodes))

        # Values for outbound links of conferences --> persons
        for conf_id in conf_ids:
            conf_persons = cur.execute("SELECT p.id, pr.role_type FROM WikicfpConferences wc\
                                    JOIN PersonRole pr ON wc.id=pr.conf_id\
                                    JOIN Persons p ON p.id=pr.person_id WHERE wc.id=?", (conf_id,)).fetchall()
            # Get outbound scores for each person
            total_outbound = sum([self.person_role_score(p[1])
                                  for p in conf_persons])
            conf_persons = [(
                p[0]+max(conf_ids), p[1], self.person_role_score(p[1]) / total_outbound
            ) for p in conf_persons]
            # Adj list and matrix update
            for p in conf_persons:
                adj_list[conf_id] = [p[0] for p in conf_persons]
                adj_matrix[conf_id][p[0]] = p[2] + \
                    adj_matrix[conf_id][p[0]]  # Multiple roles

        # Values for outbound links of persons --> conferences
        for person_id in person_ids:
            person_confs = cur.execute("SELECT wc.id FROM Persons p\
                                    JOIN PersonRole pr ON p.id=pr.person_id\
                                    JOIN WikicfpConferences wc ON wc.id=pr.conf_id WHERE p.id=?", (person_id,)).fetchall()
            # Get outbound scores for each conference
            total_outbound = len(person_confs)
            person_confs = [(
                conf[0], 1/total_outbound
            ) for conf in person_confs]
            # Adj list and matrix update
            for conf in person_confs:
                person_index = person_id + max(conf_ids)
                adj_list[person_index] = [conf[0] for conf in person_confs]
                adj_matrix[person_index][conf[0]] = conf[1] + \
                    adj_matrix[person_index][conf[0]]

        # Save matrices for computation
        self.max_conf_id = max(conf_ids)
        self.num_nodes = num_nodes
        self.adj_list = adj_list
        self.adj_matrix = adj_matrix

    def compute_scores(self, damping_factor: float, num_iterations: int):
        """Update scores using scores computed from Pagerank iteration for Persons and Conferences

        Args:
            damping_factor (float): Pagerank damping factor
            iterations (int): Number of iterations to run pagerank algorithm
        """
        cur = self.cnx.cursor()
        score_matrix = np.ones((self.num_nodes, 1))
        # Computation of scores for Conferences and Persons
        for iteration in range(num_iterations):
            print("Running iteration {}".format(iteration + 1))
            for index, inbound_indices in enumerate(self.adj_list):
                # Prevent duplicated indices due to multiple roles
                inbound_indices = list(set(inbound_indices))
                inbound_scores = [self.adj_matrix[inbound_index][index] * score_matrix[inbound_index]
                                  for inbound_index in inbound_indices]
                score_matrix[index] = (1 - damping_factor) + \
                    damping_factor * sum(inbound_scores)
            print("Total score: {}".format(np.sum(score_matrix)))

        # Update database Persons and Conferences pr_scores
        for index, score in enumerate(score_matrix):
            if index == 0:  # Skip, Not a valid database index
                continue
            if index <= self.max_conf_id:
                conf_id = index
                cur.execute("UPDATE WikicfpConferences SET pr_score=? WHERE id=?", (score.item(), conf_id))
            else:
                person_id = index - self.max_conf_id
                cur.execute("UPDATE Persons SET pr_score=? WHERE id=?", (score.item(), person_id))
        self.cnx.commit()

        # Single iteration computation of organization score
        org_ids = cur.execute( "SELECT id FROM Organizations ORDER BY id").fetchall()
        org_ids = [org_id[0] for org_id in org_ids]
        org_scores = np.zeros((max(org_ids) + 1, 1))
        for org_id in org_ids:
            org_persons = cur.execute("SELECT p.id FROM Persons p\
                                    JOIN Organizations o ON p.org_id=o.id\
                                    WHERE o.id=?", (org_id,)).fetchall()
            org_scores[org_id] = (1 - damping_factor) + damping_factor * sum(
                [score_matrix[person_index[0] + self.max_conf_id] for person_index in org_persons])

        # Update database Organization pr_scores
        for org_index, org_score in enumerate(org_scores):
            if index == 0: # Skip, Not a valid database index
                continue
            cur.execute("UPDATE Organizations SET pr_score=? WHERE id=?", (org_score.item(), org_index))
        self.cnx.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('db_filepath', type=str,
                        help="Database file to entity rank")
    parser.add_argument('num_iterations', type=int,
                        help="Number of iterations to run pagerank algorithm")
    args = parser.parse_args()
    cnx = sqlite3.connect(args.db_filepath)
    pagerank_scorer = PageRankScorer(cnx)
    pagerank_scorer.compute_matrices()
    pagerank_scorer.compute_scores(0.85, args.num_iterations)
    cnx.close()
