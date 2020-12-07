import sqlite3
import sys
sys.path.append("..") 

from database.database_config import DB_FILEPATH
from database.database_helper import DatabaseHelper

from line_ner.data_generation import preTagging
from line_ner.data_generation import generateTestCase
from data_augmentation.data_refine import removeDataNoise
from data_augmentation.data_refine import dataAugment
import ner_tagging.ner_tagger
from entity_extraction.entity_extractor import *


SET_CORPUS = False
GENERATE_DATA = False
PREDICT_LINES = False
EXTRACT_INFO = True

DatabaseHelper.create_db_post(DB_FILEPATH)

if SET_CORPUS:
	# set corpus for names
	with open("lexicon/names.txt", encoding="utf8", errors='ignore') as f1:
		for line in f1:
			name = line[:-1]
			DatabaseHelper.addCorpus(DB_FILEPATH, "names", name)

	# set corpus for universities
	with open("lexicon/university.txt", encoding="utf8", errors='ignore') as f2:
		for line in f2:
			university = line[:-1]
			DatabaseHelper.addCorpus(DB_FILEPATH, "university", university)

	# set corpus for surnames
	f = open("lexicon/surnames.txt", "r")
	for line in f:
		surname = line[:-1].lower()
		if surname != " ":
			DatabaseHelper.addCorpus(DB_FILEPATH, "surname", surname)

""" Generate Test Data 
- Uses "ALL" if you want to mix data from all different publishers, otherwise specify publisher name
"""
if GENERATE_DATA:
	publisher = "ALL"
	taggedData = preTagging(publisher)
	TEST_THRESHOLD = 0.1 # 0.1 means 0.1 of the data will be used as test data
	AUGMENT_THRESHOLD = 1 # 0 means no augmentation, 1 means 100% augmentation
	''' data generation'''
	generateTestCase("ALL", taggedData, TEST_THRESHOLD)
	''' data noise removal: correction layer'''
	removeDataNoise("surname", "line_ner/datatrain_"+publisher+".txt", "traintemp.txt")
	removeDataNoise("surname", "line_ner/datatest_"+publisher+".txt", "testtemp.txt")
	removeDataNoise("surname", "line_ner/datadev_"+publisher+".txt", "devtemp.txt")
	''' data noise removal: rectification layer'''
	removeDataNoise("org", "traintemp.txt", "data_augmentation/refinedatatrain_"+publisher+".txt")
	removeDataNoise("org", "testtemp.txt", "data_augmentation/refinedatatest_"+publisher+".txt")
	removeDataNoise("org", "devtemp.txt", "data_augmentation/refinedatadev_"+publisher+".txt")
	''' data augmentation'''
	dataAugment("data_augmentation/refinedatatrain_"+publisher+".txt", AUGMENT_THRESHOLD)
	dataAugment("data_augmentation/refinedatatest_"+publisher+".txt", AUGMENT_THRESHOLD)
	dataAugment("data_augmentation/refinedatadev_"+publisher+".txt", AUGMENT_THRESHOLD)


""" Predict Lines
- Adds prediction of line information. Use "ALL" to predict for all publishers
"""
if PREDICT_LINES:
	publisher = "ALL"
	tagger = ner_tagging.ner_tagger.NER_Tagger()
	tagger.tag_sentences(publisher) 

""" Extraction of Journal - Person -Affiliation information
"""
if EXTRACT_INFO:
	publisher = "ALL"
	entity_extractor(publisher)

	conn = sqlite3.connect(DB_FILEPATH)
	cur = conn.cursor()
	crawled = cur.execute("SELECT * FROM crawled").fetchall()

	per_id = int(cur.execute("SELECT count(*) FROM Persons").fetchone()[0])
	org_id = int(cur.execute("SELECT count(*) FROM Organizations").fetchone()[0])
	aff_id = int(cur.execute("SELECT count(*) FROM PersonOrganization").fetchone()[0])
	role_id = int(cur.execute("SELECT count(*) FROM PersonRole").fetchone()[0])

	for pub, jour, role, per, org in crawled:

		if per is None:
			continue

		''' insert into Persons table'''
		try:
			hasper = cur.execute("SELECT id FROM Persons WHERE name == '"+per+"'").fetchone()
			if hasper is None and per != "":
				cur.execute("INSERT INTO Persons VALUES ('"+str(per_id)+"', '"+per+"')")
				per_id = per_id + 1
				conn.commit()
		except:
			pass

		''' insert into Organizations table '''
		try:
			hasorg = cur.execute("SELECT id FROM Organizations WHERE name = '"+org+"'").fetchone()
			if hasorg is None and org != "":
				cur.execute("INSERT INTO Organizations VALUES ('"+str(org_id)+"', '"+org+"', null)")
				org_id = org_id + 1
				conn.commit()
		except:
			pass

		''' insert into PersonOrganization table '''
		try:
			if hasper is None and per != "": # the person has not been encountered before:
				if hasorg is None:
					cur.execute("INSERT INTO PersonOrganization VALUES ('"+str(aff_id)+"', '"+str(int(org_id)-1)+"', '"+str(int(per_id)-1)+"')")
				else:
					cur.execute("INSERT INTO PersonOrganization VALUES ('"+str(aff_id)+"', '"+str(hasorg[0])+"', '"+str(int(per_id)-1)+"')")
				aff_id = aff_id + 1
				conn.commit()
		except:
			passpyth

		''' insert into PersonRole table'''
		

		if jour[-1] == " ":
			jour = jour[:-1]
		doc_id = cur.execute("SELECT id FROM journalInfo WHERE title = '"+jour+"'").fetchone()[0]

		if hasper is None: 
			cur.execute("INSERT INTO PersonRole VALUES ("+str(role_id)+", '"+role+"', 'journal', '"+str(doc_id)+"', '"+str(int(per_id)-1)+"')")
		else:
			cur.execute("INSERT INTO PersonRole VALUES ("+str(role_id)+", '"+role+"', 'journal', '"+str(doc_id)+"', '"+str(hasper[0])+"')")
		role_id = role_id + 1
		conn.commit()
		

	cur.close()
	conn.close()

