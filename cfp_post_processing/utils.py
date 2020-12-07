def create_tables(cnx):
    """Create tables for PageLines and preliminary Information Extraction

    Args:
        cnx (sqlite3.Cursor): Connection to database
    """
    cur = cnx.cursor()

    # Pagelines
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
            id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS Organizations (\
        id INTEGER NOT NULL PRIMARY KEY, name TEXT, location TEXT)")
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
        conf_id INTEGER REFERENCES WikicfpConferences(id),\
        person_id INTEGER REFERENCES Persons(id),\
        CONSTRAINT u_p_c_role UNIQUE(person_id, conf_id, role_type)\
        );")

    cnx.commit()
    cur.close()
