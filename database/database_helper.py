import sqlite3
from typing import List


class DatabaseHelper:

    @staticmethod
    def create_db(dbpath):

        """ Create the necessary tables for the conference """

        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS WikicfpConferences (\
            id INTEGER NOT NULL PRIMARY KEY,\
            series TEXT NOT NULL,\
            title TEXT NOT NULL UNIQUE,\
            url TEXT,\
            timetable TEXT,\
            year INTEGER,\
            wayback_url TEXT,\
            categories TEXT,\
            accessible TEXT,\
            crawled TEXT\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS ConferencePages (\
            id INTEGER NOT NULL PRIMARY KEY,\
            conf_id INTEGER NOT NULL REFERENCES Urls(id),\
            url TEXT NOT NULL UNIQUE,\
            html TEXT,\
            content_type TEXT,\
            processed TEXT\
        );")


        """ Create the necessary tables for the journal """

        cur.execute("CREATE TABLE IF NOT EXISTS journalInfo (\
            id INTEGER NOT NULL UNIQUE,\
            title TEXT NOT NULL UNIQUE,\
            publisher TEXT NOT NULL,\
            url TEXT PRIMARY KEY\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS corpus (\
            type TEXT NOT NULL,\
            content TEXT NOT NULL UNIQUE\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS journalLine (\
            publisher TEXT NOT NULL,\
            title TEXT NOT NULL,\
            _line TEXT NOT NULL UNIQUE\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS taggedLine (\
            publisher TEXT NOT NULL,\
            title TEXT NOT NULL,\
            _line TEXT NOT NULL UNIQUE\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS crawled (\
            publisher TEXT NOT NULL,\
            title TEXT NOT NULL,\
            role  TEXT NOT NULL,\
            name  TEXT NOT NULL,\
            affiliation TEXT NOT NULL\
        );")

        conn.commit()
        cur.close()
        conn.close()


    @staticmethod
    def create_db_post(dbpath):
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()

        cur.execute("CREATE TABLE IF NOT EXISTS PageLines (\
            id INTEGER NOT NULL PRIMARY KEY,\
            page_id INTEGER NOT NULL REFERENCES Urls(id),\
            line_num INTEGER,\
            line_text TEXT,\
            tag TEXT,\
            indentation TEXT,\
            label TEXT,\
            dl_prediction TEXT,\
            svm_prediction TEXT\
        );")

        # Persons and Organizations table
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Persons (\
            id INTEGER NOT NULL PRIMARY KEY,\
            name TEXT\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS Organizations (\
            id INTEGER NOT NULL PRIMARY KEY, \
            name TEXT, \
            location TEXT \
        );")

        # Relationship Tables
        cur.execute("CREATE TABLE IF NOT EXISTS PersonOrganization(\
            affiliation_id INTEGER PRIMARY KEY NOT NULL,\
            org_id INTEGER REFERENCES Organizations(id),\
            person_id INTEGER REFERENCES Persons(id),\
            CONSTRAINT u_org_p UNIQUE(org_id, person_id)\
        );")

        cur.execute("CREATE TABLE IF NOT EXISTS PersonRole(\
            role_id INTEGER PRIMARY KEY NOT NULL,\
            role_type TEXT NOT NULL,\
            doc_type TEXT NOT NULL,\
            doc_id INTEGER NOT NULL,\
            person_id INTEGER REFERENCES Persons(id)\
        );")

        conn.commit()
        cur.close()
        #CONSTRAINT u_p_c_role UNIQUE(person_id, doc_id, role_type)\


    @staticmethod
    def add_wikicfp_conf(conference: 'WikiConferenceItem', dbpath: str):
        """ Adds Conference information scraped from wikicfp """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO WikicfpConferences\
            (series, title, url, timetable, year, wayback_url, categories, accessible, crawled) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(conference['series']),
                str(conference['title']),
                str(conference['url']),
                str(conference['timetable']),
                str(conference['year']),
                str(conference['wayback_url']),
                str(conference['categories']),
                str(conference['accessible']),
                str(conference['crawled'])
            )
        )
        conf_id = cur.lastrowid
        conn.commit()
        cur.close()
        conn.close()
        return conf_id

    @staticmethod
    def mark_accessibility(conf_id: int, access_status: str, dbpath: str):
        """ Marks the accessibility attribute of a Conference url retrieved from wikicfp """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute(
            'UPDATE WikicfpConferences SET accessible = "{}" WHERE id = "{}"'.format(
                access_status, conf_id)
        )
        conn.commit()
        cur.close()
        conn.close()

    @staticmethod
    def mark_crawled(conf_id: int, dbpath: str):
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute(
            'UPDATE WikicfpConferences SET crawled = "{}" WHERE id = "{}"'.format(
                'Yes', conf_id)
        )
        conn.commit()
        cur.close()
        conn.close()

    @staticmethod
    def add_page(conference_page: 'ConferencePageItem', dbpath: str):
        """ Adds page of Conference """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO ConferencePages\
            (conf_id, url, html, content_type)\
            VALUES (?, ?, ?, ?)",
            (
                str(conference_page['conf_id']),
                str(conference_page['url']),
                str(conference_page['html']),
                str(conference_page['content_type'])
            )
        )
        page_id = cur.lastrowid
        conn.commit()
        cur.close()
        conn.close()
        return page_id

    @staticmethod
    def page_saved(page_url: str, dbpath: str):
        """ Check if page has already been saved """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        page_with_url = cur.execute(
            "SELECT count(*) FROM ConferencePages\
            WHERE url=?", (page_url,)
        ).fetchone()
        cur.close()
        conn.close()
        return page_with_url[0] > 0

    @staticmethod
    def addJournal(journal_info: 'JournalInfoItem', dbpath:str):
        """ Add a journal information including its publisher, name and URL """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        _id = cur.execute("SELECT count(*) FROM journalInfo").fetchone()[0]
        cur.execute("INSERT OR REPLACE INTO journalInfo\
            (id, title, publisher, url)\
            VALUES(?,?,?,?)",
            (
            _id,
            str(journal_info['title']),
            str(journal_info['publisher']),
            str(journal_info['url']),
            )
        )

        conn.commit()
        cur.close()
        conn.close()

    @staticmethod
    def addLine(journal_line: 'JournalLineItem', dbpath:str):
        """ Add lines of journal eitorial board """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO journalLine\
            (publisher, title, _line)\
            VALUES(?,?,?)",
            (
            str(journal_line['publisher']),
            str(journal_line['title']),
            str(journal_line['line']),
            )
        )

        conn.commit()
        cur.close()
        conn.close()

    @staticmethod
    def addTaggedLine(tagged_line: 'TaggedLineItem', dbpath:str):
        """ Add lines of journal editorial board that is tagged by pretrained model (to generate data-set) """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO taggedLine\
            (publisher, title, _line)\
            VALUES(?,?,?)",
            (
            str(tagged_line['publisher']),
            str(tagged_line['title']),
            str(tagged_line['line']),
            )
        )

        conn.commit()
        cur.close()
        conn.close()

    @staticmethod
    def addCrawledItem(crawled: 'CrawledItem', dbpath:str):
        """ Add crawled information from journal editorial boards """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO crawled\
            (publisher, title, role, name, affiliation)\
            VALUES(?,?,?,?,?)",
            (
            str(crawled['publisher']),
            str(crawled['title']),
            str(crawled['role']),
            str(crawled['name']),
            str(crawled['affiliation']),
            )
        )

        conn.commit()
        cur.close()
        conn.close()


    @staticmethod
    def addCorpus(dbpath, _type, _content):
        """ Add lexicon that assists data generation or data noise removal """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO corpus\
            (type, content)\
            VALUES(?,?)",
            (
            _type,
            _content
            )
        )

        conn.commit()
        cur.close()
        conn.close()


    @staticmethod
    def getJournalUrls(dbpath, _publisher):
        """ Get Urls of all the journals """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        url = cur.execute("SELECT url FROM journalInfo WHERE publisher == '" + _publisher + "'").fetchall()

        conn.commit()
        cur.close()
        conn.close()

        return url


    @staticmethod
    def getLines(dbpath, _publisher):
        """ Get all the lines from editorial boards """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        lines = cur.execute("SELECT publisher, title, _line FROM journalLine WHERE publisher == '" + _publisher + "'").fetchall()

        conn.commit()
        cur.close()
        conn.close()

        return lines

    @staticmethod
    def getLines(dbpath):
        """ Get all the lines from editorial boards """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        lines = cur.execute("SELECT publisher, title, _line FROM journalLine").fetchall()

        conn.commit()
        cur.close()
        conn.close()

        return lines

    @staticmethod
    def getTaggedLines(dbpath, _publisher):
        """ Get all the lines that were labelled by the pre-trained model """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        lines = cur.execute("SELECT publisher, title, _line FROM taggedLine WHERE publisher == '" + _publisher + "'").fetchall()

        conn.commit()
        cur.close()
        conn.close()

        return lines

    @staticmethod
    def getTaggedLines(dbpath):
        """ Get all the lines that were labelled by the pre-trained model """
        conn = sqlite3.connect(str(dbpath))
        cur = conn.cursor()
        lines = cur.execute("SELECT publisher, title, _line FROM taggedLine").fetchall()

        conn.commit()
        cur.close()
        conn.close()

        return lines