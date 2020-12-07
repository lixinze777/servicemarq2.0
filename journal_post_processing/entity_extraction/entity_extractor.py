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
		data = DatabaseHelper.getTaggedLines(DB_FILEPATH, publisher)

	tagRole = ['<B-ROLE>', '<I-ROLE>', '<E-ROLE>', '<ROLE>', '<BI-ROLE>']
	tagPer = ['<B-PER>', '<I-PER>', '<E-PER>', ' <S-PER>']
	tagOrg = ['<B-ORG>', '<I-ORG>', '<E-ORG>', ' <S-ORG>']
	hasPer = False 
	hasOrg = False
	perReady = False # true indicates that the name field is ready to input
	orgReady = False # true indicates that the org field is ready to input

	journal = ""
	role = ""
	name = ""
	affiliation = ""

	all_content = role_extractor(tagRole, data) # all_content is a tri-pair of title, role, content

	for crawledpublisher, title, role, content in all_content:

		thisJournal = title
		thisRole = role

		hasPer = False
		hasOrg = False

		for item in tagPer:
			if item in content:
				hasPer = True
				break

		for item in tagOrg:
			if item in content:
				hasOrg = True
				break

		perReady, orgReady, journal, role, name, affiliation = Intepreter(perReady, orgReady, hasPer, hasOrg, tagPer, tagOrg, thisJournal, thisRole, journal, role, name, affiliation, content)

		if perReady and orgReady: #both field are ready to input
			name = name.replace("  "," ")
			affiliation = affiliation.replace("  "," ")	
			DatabaseHelper.addCrawledItem(items.CrawledItem(publisher=crawledpublisher, title=journal, role = role, name=name, affiliation = affiliation), DB_FILEPATH)
			perReady = False # reset per field state
			orgReady = False # reset org field state

def Intepreter(perReady, orgReady, hasPer, hasOrg, tagPer, tagOrg, thisJournal, thisRole, journal, role, name, affiliation, content):

	if thisJournal != journal or thisRole != role: # not under the field for the same person
		perReady = False
		orgReady = False
		journal = thisJournal
		role = thisRole

	if orgReady: 
		orgReady = False # org normally cannot be in front

	per = ""
	org = ""

	mylist = content.split()
	pernum = []
	pertagnum = []
	orgnum = []
	orgtagnum = []
	tempper = []
	temporg = []

	if hasPer and hasOrg:
		for i in range(len(mylist)):
			if mylist[i] in tagPer:
				pertagnum.append(i)
				pernum.append(i-1)
			if mylist[i] in tagOrg:
				orgtagnum.append(i)
				orgnum.append(i-1)

		for i in range(len(mylist)):

			if perReady and orgReady:
				break
			elif i in pernum:
				if i + 2 in pernum: # next word still editor word
					tempper.append(mylist[i])
				else: # this is the last editor word:
					for word in tempper:
						per = per + word + " "
					per = per + mylist[i]
					perReady = True
			elif i in orgnum:
				if i + 2 in orgnum: # next word still editor word
					temporg.append(mylist[i])
				else: # this is the last editor word:
					for word in temporg:
						org = org + word + " "
					org = org + mylist[i]
					orgReady = True
			elif i in orgtagnum: # <org>
				pass
			elif i in pertagnum: # <per>
				pass

	elif hasPer:
		for i in range(len(mylist)):
			if mylist[i] in tagPer:
				pertagnum.append(i)
				pernum.append(i-1)

		for i in range(len(mylist)):
			if i in pernum:
				if i + 2 in pernum: # next word still editor word
					tempper.append(mylist[i])
				else: # this is the last editor word:
					for word in tempper:
						per = per + word + " "
					per = per + mylist[i]
					break
			elif i in pertagnum: # <per>
				pass
		perReady = True

	elif hasOrg:
		for i in range(len(mylist)):
			if mylist[i] in tagOrg:
				orgtagnum.append(i)
				orgnum.append(i-1)

		for i in range(len(mylist)):
			if i in orgnum:
				if i + 2 in orgnum: # next word still editor word
					temporg.append(mylist[i])
				else: # this is the last editor word:
					for word in temporg:
						org = org + word + " "
					org = org + mylist[i]
					break
			elif i in orgtagnum: # <per>
				pass
		orgReady = True

	else:
		pass

	name = per
	affiliation = org
	return perReady, orgReady, journal, role, name, affiliation

if __name__ == "__main__":
    entity_extractor("ALL")