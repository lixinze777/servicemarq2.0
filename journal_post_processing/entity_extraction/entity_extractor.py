import sys
sys.path.append("..") 
sys.path.append("../../journal_crawl/journal_spider")
sys.path.append("../journal_crawl/journal_spider")

from database.database_config import DB_FILEPATH
from database.database_helper import DatabaseHelper
import items
import re


def role_extractor(tagRole, data):
	
	current_role = "editor"
	all_content = [] # a list of all content, each item is a tri-pair of title, role, content

	for publisher, title, content in data:
		mylist = content.split()
		rolenum = []
		roletagnum = []
		current_sent = ""
		tempeditor = []

		for i in range(len(mylist)):
			if mylist[i] in tagRole:
				roletagnum.append(i)
				rolenum.append(i-1)

		for i in range(len(mylist)):
			if i in rolenum:
				if i != 0: #not the first word
					all_content.append((publisher, title, current_role, current_sent))
					current_sent = ""
				if i + 2 in rolenum: # next word still editor word
					tempeditor.append(mylist[i])
				else: # this is the last editor word:
					current_role = ""
					for word in tempeditor:
						current_role = current_role + word + " "
					current_role = current_role + mylist[i]
					tempeditor = []
			elif i in roletagnum: # <role>
				pass
			else:
				current_sent = current_sent + mylist[i] + " "
				if i == len(mylist) - 1: # last word
					all_content.append((publisher, title, current_role, current_sent))
					current_sent = ""

	return all_content


def entity_extractor(publisher):

	if publisher == "ALL":
		data = DatabaseHelper.getTaggedLines(DB_FILEPATH)
	else:
		data = DatabaseHelper.getTaggedLinesTwo(DB_FILEPATH, publisher)

	tagRole = ['<B-ROLE>', '<I-ROLE>', '<E-ROLE>', '<ROLE>', '<BI-ROLE>']
	tagPer = ['<B-PER>', '<I-PER>', '<E-PER>', '<S-PER>']
	tagOrg = ['<B-ORG>', '<I-ORG>', '<E-ORG>', '<S-ORG>']
	remained_person = ""

	all_content = role_extractor(tagRole, data) # all_content is a tri-pair of title, role, content

	for crawledpublisher, title, role, content in all_content:

		thisJournal = title
		thisRole = role

		hasPer = False
		hasOrg = False

		ready_to_input, info_tuple, remained_person = intepreter(content, tagPer, tagOrg, remained_person)

		if ready_to_input:
			for name, affiliation in info_tuple:
				name = name.replace("  "," ")
				affiliation = affiliation.replace("  "," ")	
				DatabaseHelper.addCrawledItem(items.CrawledItem(publisher=crawledpublisher, title=title, role=role, name=name, affiliation = affiliation), DB_FILEPATH)


def intepreter(content, tagPer, tagOrg, remained_person):
	
	info_tuple = []
	ready_to_input = False # initialize the state as not ready to input value to database

	pernum = []
	orgnum = []
	mylist = content.split()
	for i in range(len(mylist)):
		if mylist[i] in tagPer:
			pernum.append(i-1)
		elif mylist[i] in tagOrg:
			orgnum.append(i-1)

	if len(pernum)+len(orgnum) == 0:
		return ready_to_input, info_tuple, remained_person # no useful entity found in the process

	entity_list = [] # a list of entities extracted, in form of entity followed by type 

	if remained_person != "":
		entity_list.append((remained_person, "per"))

	per_buffer = ""
	org_buffer = ""
	for i in range (len(mylist)):
		if i in pernum:
			if i + 2 in pernum: # next token still for the same person
				per_buffer = per_buffer + mylist[i] + " "
			else: # this is the last token
				per_buffer = per_buffer + mylist[i]
				entity_list.append((per_buffer, "per"))
				per_buffer = ""
		elif i in orgnum:
			if i + 2 in orgnum: # next token still for the same organisation
				org_buffer = org_buffer + mylist[i] + " "
			else: # this is the last token
				org_buffer = org_buffer + mylist[i]
				entity_list.append((org_buffer, "org"))
				org_buffer = ""

	for j in range(len(entity_list)):
		if j == len(entity_list)-1 and entity_list[j][1] == "per": # the last entity is per
			remained_person = entity_list[j][0]
		elif j < len(entity_list)-1 and entity_list[j][1] == "per" and entity_list[j+1][1] == "org":
			ready_to_input = True
			info_tuple.append((entity_list[j][0], entity_list[j+1][0]))
			remained_person = ""

	return ready_to_input, info_tuple, remained_person
