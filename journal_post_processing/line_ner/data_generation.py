import re
import random
import sqlite3
from flair.data import Sentence
from flair.models import SequenceTagger

import sys
sys.path.append("..") 

from database.database_config import DB_FILEPATH


def preTagging(publisher):

	roleTable = {}
	roleTable['co-editor'] = 'ROLE'
	roleTable['co-editors'] = 'ROLE'
	roleTable['book-review-committee'] = 'ROLE'
	roleTable['book-review-editor'] = 'ROLE'
	roleTable['review-committee'] = 'ROLE'
	roleTable['review-board'] = 'ROLE'
	roleTable['advisory-council'] = 'ROLE'
	roleTable['advisory-board'] = 'ROLE'
	roleTable['managing-board'] = 'ROLE'
	roleTable['coordinating-editor'] = 'ROLE'
	roleTable['coordinating-editors'] = 'ROLE'
	roleTable['founding-editor'] = 'ROLE'
	roleTable['founding-editors'] = 'ROLE'
	roleTable['department-editor'] = 'ROLE'
	roleTable['department-editors'] = 'ROLE'
	roleTable['action-editor'] = 'ROLE'
	roleTable['action-editors'] = 'ROLE'
	roleTable['section-editor'] = 'ROLE'
	roleTable['section-editors'] = 'ROLE'
	roleTable['managing-editor'] = 'ROLE'
	roleTable['managing-editors'] = 'ROLE'
	roleTable['senior-associate-editors'] = 'ROLE'
	roleTable['senior-associate-editor'] = 'ROLE'
	roleTable['editor-in-chief'] = 'ROLE'
	roleTable['editors-in-chief'] = 'ROLE'
	roleTable['associate-editors'] = 'ROLE'
	roleTable['associate-editor'] = 'ROLE'
	roleTable['editorial-board'] = 'ROLE'
	roleTable['editorialboard'] = 'ROLE'
	roleTable['editor-emeritus'] = 'ROLE'
	roleTable['editors'] = 'ROLE'
	roleTable['editor'] = 'ROLE'
	roleTable['executive'] = 'B-ROLE'
	roleTable['deputy'] = 'B-ROLE'
	roleTable['honorable'] = 'B-ROLE'
	roleTable['coordinating'] = 'B-ROLE'
	roleTable['action'] = 'B-ROLE'
	roleTable['section'] = 'B-ROLE'
	roleTable['founding'] = 'B-ROLE'
	roleTable['managing'] = 'B-ROLE'
	roleTable['senior'] = 'B-ROLE'
	roleTable['editorial'] = 'B-ROLE'
	roleTable['book'] = 'B-ROLE'
	roleTable['advisory'] = 'BI-ROLE'
	roleTable['review'] = 'BI-ROLE'
	roleTable['associate'] = 'BI-ROLE'
	roleTable['board'] = 'E-ROLE'
	roleTable['committee'] = 'E-ROLE'
	roleTable['council'] = 'E-ROLE'
	roleTable['in'] = 'I-ROLE'
	roleTable['chief'] = 'E-ROLE'
	roleTable['emeritus'] = 'E-ROLE'

	tagger = SequenceTagger.load('ner')
	taggedData = []

	conn = sqlite3.connect(DB_FILEPATH)
	cur = conn.cursor()

	if publisher == "ALL":
		data = cur.execute("SELECT _line FROM journalLine WHERE _line != \"\"").fetchall()
	else:
		data = cur.execute("SELECT _line FROM journalLine WHERE publisher = '"+publisher+"' AND _line != \"\"").fetchall()

	counter = 0
	for row in data:
		output = ""
		if row[0] == "": # skip empty lines 
			continue
		sentence = Sentence(row[0])
		tagger.predict(sentence)
		tagged = sentence.to_tagged_string()

		for word in tagged.split():
			output = output + word + " "
			if roleTable.__contains__(word.lower()):
				output = output + " <" + roleTable[word.lower()] + "> "
		
		taggedData.append(output)
		counter = counter + 1

	return taggedData

def generateTestCase(publisher, taggedData, TEST_THRESHOLD):

	train = []
	dev = []
	test = []

	tagword = [
		"<B-PER>",
		"<I-PER>",
		"<E-PER>",
		"<S-PER>",
		"<B-ORG>",
		"<I-ORG>",
		"<E-ORG>",
		"<S-ORG>",
		"<B-LOC>",
		"<I-LOC>",
		"<E-LOC>",
		"<S-LOC>",
		"<B-ROLE>",
		"<I-ROLE>",
		"<E-ROLE>",
		"<BI-ROLE>",
		"<ROLE>"
	]

	for row in taggedData:
		line = row.replace("</ ","</").replace("< ","<").replace(" >",">")
		temp = []
		wordcache = ""
		prevtoken = False # true indicates that previous token is word, false indicates that previous token is tag

		for token in line.split():
			if token in tagword: # this token is a tag
				tag = token[1:-1]
				if prevtoken == True: # if previous token is word
					temp.append(wordcache + " " + tag + "\n")
				prevtoken = False
			else: # this token is a word
				if prevtoken == True: # previous token is a word
					temp.append(wordcache + " O\n")
				wordcache = token
				prevtoken = True
		temp.append("\n")

		rand = random.random()

		if rand >= 1-TEST_THRESHOLD:
			dev.append(temp)
		elif rand <= TEST_THRESHOLD:
			test.append(temp)
		else:
			train.append(temp)

	file1 = open("line_ner/datatrain_"+publisher+".txt", "a")
	for item in train:
		file1.writelines(item)
	file1.close()

	file2 = open("line_ner/datadev_"+publisher+".txt", "a")
	for item in dev:
		file2.writelines(item)
	file2.close()

	file3 = open("line_ner/datatest_"+publisher+".txt", "a")
	for item in test:
		file3.writelines(item)
	file3.close()


if __name__ == "__main__":

	taggedData = preTagging("ALL")
	TEST_THRESHOLD = 0.1
	generateTestCase("ALL", taggedData, TEST_THRESHOLD)
