import re
import string
import unicodedata


def clean_punctuation(ltext: str):
    # Strip leading and trailing punctuation
    ltext = ltext.strip(string.punctuation)
    # Normalize string to unicoded to remove splitting errors
    ltext = unicodedata.normalize("NFKD", ltext)
    # Replace tabs and newlines with spaces
    ltext = re.sub('\t|\r|\n|\(|\)|\"|\'', ' ', ltext)
    ltext = re.sub(' +', ' ', ltext)  # Remove multiple spacing
    ltext = ltext.strip()
    return ltext


class DatabaseHelper:

    @staticmethod
    def create_tables(cnx):
        """Create tables for consolidated Person/Org/Conf

        Args:
            cnx (sqlite3.Connection): Connection to database
        """
        cur = cnx.cursor()

        cur.execute("CREATE TABLE IF NOT EXISTS Series (\
            id INTEGER NOT NULL PRIMARY KEY,\
            title TEXT NOT NULL UNIQUE,\
            score REAL);")

        cur.execute("CREATE TABLE IF NOT EXISTS WikicfpConferences (\
            id INTEGER NOT NULL PRIMARY KEY,\
            series_id INTEGER REFERENCES Series(id),\
            title TEXT NOT NULL UNIQUE,\
            url TEXT,\
            timetable TEXT,\
            year INTEGER,\
            wayback_url TEXT,\
            accessible TEXT,\
            crawled TEXT,\
            score REAL);")

        cur.execute("CREATE TABLE IF NOT EXISTS Topics (\
            id INTEGER NOT NULL PRIMARY KEY,\
            topic TEXT NOT NULL UNIQUE);")

        cur.execute("CREATE TABLE IF NOT EXISTS ConferenceTopics (\
            id INTEGER NOT NULL PRIMARY KEY,\
            conf_id INTEGER NOT NULL REFERENCES WikicfpConferences(id),\
            topic_id INTEGER NOT NULL REFERENCES Topics(id),\
            CONSTRAINT c_t UNIQUE(conf_id, topic_id)\
            );")


        cur.execute("CREATE TABLE IF NOT EXISTS ConferencePages (\
            id INTEGER NOT NULL PRIMARY KEY,\
            conf_id INTEGER NOT NULL REFERENCES WikicfpConferences(id),\
            url TEXT NOT NULL UNIQUE,\
            content_type TEXT,\
            processed TEXT);")

        cur.execute("CREATE TABLE IF NOT EXISTS Organizations (\
            id INTEGER NOT NULL PRIMARY KEY,\
            name TEXT UNIQUE,\
            score REAL);")

        cur.execute("CREATE TABLE IF NOT EXISTS Persons (\
            id INTEGER NOT NULL PRIMARY KEY, name TEXT,\
            org_id REFERENCES Organizations(id),\
            orcid TEXT, gscholar_id TEXT,\
            aminer_id TEXT, dblp_id TEXT,\
            score REAL,\
            CONSTRAINT p_o UNIQUE(name, org_id)\
            );")

        cur.execute("CREATE TABLE IF NOT EXISTS PersonRole(\
            role_id INTEGER PRIMARY KEY NOT NULL,\
            role_type TEXT NOT NULL,\
            conf_id INTEGER REFERENCES WikicfpConferences(id),\
            person_id INTEGER REFERENCES Persons(id),\
            CONSTRAINT u_p_c_role UNIQUE(person_id, conf_id, role_type)\
            );")

        cnx.commit()

    @staticmethod
    def move_conferences_table(original_db_cnx: 'sqlite3.cursor', consolidated_db_cnx: 'sqlite3.cursor'):
        """Extract and process Conference information from original database, update new database

        Args:
            original_db_cnx (sqlite3.cursor): Connection to original database
            consolidated_db_cnx (sqlite3.cursor): Connection to new database
        """
        original_db_cur = original_db_cnx.cursor()
        consolidated_db_cur = consolidated_db_cnx.cursor()
        # Copy over table of conferences and conference pages
        conference_ids = original_db_cur.execute("SELECT id FROM WikicfpConferences ORDER BY id").fetchall()
        for conf_id in conference_ids:
            # Move WikicfpConference table information
            conf_id = conf_id[0]
            conf = original_db_cur.execute("SELECT id, title, url, timetable, year, wayback_url, accessible, crawled FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
            consolidated_db_cur.execute("INSERT INTO WikicfpConferences\
                        (id, title, url, timetable, year, wayback_url, accessible, crawled)\
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)", conf)

            # Process Conference series
            series = original_db_cur.execute("SELECT series FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
            series = series[0].strip()
            series_id = consolidated_db_cur.execute("SELECT id FROM Series WHERE title=?", (series,)).fetchone()
            if series_id == None:
                consolidated_db_cur.execute("INSERT INTO Series (title) VALUES (?)", (series,))
                series_id = consolidated_db_cur.lastrowid
            else:
                series_id = series_id[0]
            consolidated_db_cur.execute("UPDATE WikicfpConferences SET series_id=? WHERE id=?", (series_id, conf_id))

            # Process Conference topics
            topics = original_db_cur.execute("SELECT categories FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
            topics = eval(topics[0])
            for topic in topics:
                topic_id = consolidated_db_cur.execute("SELECT id FROM Topics WHERE topic=?", (topic,)).fetchone()
                if topic_id == None:
                    consolidated_db_cur.execute("INSERT INTO Topics (topic) VALUES (?)", (topic,))
                    topic_id = consolidated_db_cur.lastrowid
                else:
                    topic_id = topic_id[0]
                try:
                    consolidated_db_cur.execute("INSERT INTO ConferenceTopics (conf_id, topic_id) VALUES (?, ?)", (conf_id, topic_id))
                except:
                    pass # Duplicate (conf_id, topic_id), ignore for constraint failure

            # Process Conference pages
            conf_pages = original_db_cur.execute("SELECT id, conf_id, url, content_type, processed\
                                        FROM ConferencePages WHERE conf_id=?", (conf_id,)).fetchall()
            for conf_page in conf_pages:
                consolidated_db_cur.execute("INSERT INTO ConferencePages\
                            (id, conf_id, url, content_type, processed)\
                            VALUES (?, ?, ?, ?, ?)", conf_page)
            consolidated_db_cnx.commit()

    @staticmethod
    def get_persons_info(cur, conf_id):
        return cur.execute("SELECT p.name, o.name, pr.role_type FROM Persons p\
            JOIN PersonOrganization po ON po.person_id=p.id\
            JOIN Organizations o ON po.org_id=o.id\
            JOIN PersonRole pr ON pr.person_id=p.id\
            WHERE pr.conf_id=?", (conf_id,)).fetchall()
