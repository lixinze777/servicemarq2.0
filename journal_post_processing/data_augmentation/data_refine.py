import re
from random import randrange
import random
import sqlite3
import os

import sys
sys.path.append("..") 

from database.database_config import DB_FILEPATH

def removeDataNoise(refine_type, input_file, output_file):

	''' prepare keywords that assists noise removal'''
	conn = sqlite3.connect(DB_FILEPATH)
	cur = conn.cursor()
	surname = cur.execute("select content from corpus where type == \"surname\"").fetchall()
	surnames = []
	for line in surname:
		if line[0] != " ":
			surnames.append(line[0])
	org = ["univerisity", "institute"]

	output = []
	log = []
	btag = ""
	itag = ""
	etag = ""
	if refine_type == "surname":
		btag = "B-PER"
		itag = "I-PER"
		etag = "E-PER"
		refine = surnames
	elif refine_type == "org":
		btag = "B-ORG"
		itag = "I-ORG"
		etag = "E-ORG"
		refine = org
	else:
		print("unseen refine_type: "+refine_type)
	log.append(refine_type + " " + output_file + " start-----------------------\n")


	f = open(input_file,"r")
	paragraph = [] # a list of lines that belong to the same senetence
	per_range = [] # range of the names detected
	for line in f:
		if line != "\n":
			paragraph.append(line)
		else: 
			for rownum in range(len(paragraph)):
				if paragraph[rownum].split()[0].lower() in refine:
					per_range.append(rownum)

			for rownum in per_range:
				tempnum = rownum
				while tempnum > 0:
					tempnum = tempnum - 1
					if tempnum not in per_range and paragraph[tempnum].split()[0].isalpha(): #check the prev token(s)
						per_range.append(tempnum)
					else:
						break

				tempnum = rownum
				while tempnum < len(paragraph) - 1:
					tempnum = tempnum + 1
					if tempnum not in per_range and paragraph[tempnum].split()[0].isalpha(): #check the further token(s)
						per_range.append(tempnum)
					else:
						break

			per_range.sort()

			for rownum in range(len(paragraph)):
				if rownum in per_range: #this token is a PER
					if rownum+1 in per_range:	#next token is a PER
						if rownum-1 in per_range: # prev token is a PER
							output.append(paragraph[rownum].split()[0] + " "+itag+"\n")
							if paragraph[rownum].split()[1] != itag:
								log.append(paragraph[rownum] + " -> <"+itag+">\n")
						else: # prev token is not PER
							output.append(paragraph[rownum].split()[0] + " "+btag+"\n")
							if paragraph[rownum].split()[1] != btag:
								log.append(paragraph[rownum] + " -> <"+btag+">\n")
					else: #next token is not PER
						output.append(paragraph[rownum].split()[0] + " "+etag+"\n")
						if paragraph[rownum].split()[1] != etag:
							log.append(paragraph[rownum] + " -> <"+etag+">\n")

				else:
					output.append(paragraph[rownum])
			output.append("\n")
			paragraph.clear()
			per_range.clear()
	file = open(output_file, "a")
	for item in output:
		file.writelines(item)
	file.close()
	log.append(refine_type + " " + output_file + " processed-----------------------\n")

	file1 = open("data_augmentation/refinetest.log", "a")
	for item in log:
		file1.writelines(item)
	file1.close()


def dataAugment(file_name, AUGMENT_THRESHOLD):
	
	'''prepare keyword needed for data augmentation'''
	conn = sqlite3.connect(DB_FILEPATH)
	cur = conn.cursor()
	name = cur.execute("select content from corpus where type == \"names\"").fetchall()
	organisation = cur.execute("select content from corpus where type == \"university\"").fetchall()

	names = []
	for line in name:
		if line[0] != " ":
			names.append(line[0])

	unis = []
	for line in organisation:
		if line[0] != " ":
			unis.append(line[0])

	output = []

	per_tags = ['B-PER','I-PER','E-PER','S-PER']
	org_tags = ['B-ORG','I-ORG','E-ORG','S-ORG']

	with open(file_name, encoding="utf8", errors='ignore') as f:
		paragraph = [] # a list of lines that belong to the same senetence
		has_per = False
		has_org = False
		per_start = -1 # the start line num of PER
		org_start = -1 # the start line num of ORG
		for line in f:
			if line != "\n":
				paragraph.append(line)
			else: 
				# Controls the amplitudeaugmentation
				if random.random() > AUGMENT_THRESHOLD: 
					has_per = False # reset
					has_org = False # reset
					per_start = -1 # reset
					org_start = -1 # reset
					paragraph.clear()
					continue

				for rownum in range(len(paragraph)):
					if not has_per:
						tag =""
						taglist = paragraph[rownum].split()
						if len(taglist) >= 2:
							tag = taglist[1] 
						if tag in per_tags:
							per_start = rownum
							has_per = True
					if not has_org:
						tag =""
						taglist = paragraph[rownum].split()
						if len(taglist) >= 2:
							tag = taglist[1] 
						if tag in org_tags:
							org_start = rownum
							has_org = True
					if has_org and has_org:
						break

				
				for rownum in range(len(paragraph)):			
					if rownum == per_start:
						if paragraph[rownum].split()[1] in per_tags:
							per_start = per_start + 1
						else: 
							namelist = names[randrange(len(names))].split()
							for i in range(len(namelist)):
								if i == 0:
									output.append(namelist[i]+ " B-PER\n")
								elif i == len(namelist)-1:
									output.append(namelist[i]+ " E-PER\n")
								else:
									output.append(namelist[i]+ " I-PER\n")
							output.append(paragraph[rownum])
					elif rownum == org_start:
						if paragraph[rownum].split()[1] in org_tags:
							org_start = org_start + 1
						else: 
							unilist = unis[randrange(len(unis))].split()
							for i in range(len(unilist)):
								if i == 0:
									output.append(unilist[i]+ " B-ORG\n")
								elif i == len(unilist)-1:
									output.append(unilist[i]+ " E-ORG\n")
								else:
									output.append(unilist[i]+ " I-ORG\n")
							output.append(paragraph[rownum])
					else:
						if random.random() > 0.8:
							output.append(paragraph[rownum])
				
				output.append("\n")
				has_per = False # reset
				has_org = False # reset
				per_start = -1 # reset
				org_start = -1 # reset
				paragraph.clear()

		
		file = open(file_name, "a")
		for item in output:
			file.writelines(item)
		file.close()
		print(file_name + " augmented\n")

		try:
			os.system("rm traintemp.txt")
			os.system("rm testtemp.txt")
			os.system("rm devtemp.txt")
		except:
			pass


if __name__ == "__main__":

	publisher = "ALL"
	removeDataNoise("surname", "line_ner/datatrain_"+publisher+".txt", "traintemp.txt")
	removeDataNoise("surname", "line_ner/datatest_"+publisher+".txt", "testtemp.txt")
	removeDataNoise("surname", "line_ner/datadev_"+publisher+".txt", "devtemp.txt")
	removeDataNoise("org", "traintemp.txt", "data_augmentation/refinedatatrain_"+publisher+".txt")
	removeDataNoise("org", "testtemp.txt", "data_augmentation/refinedatatest_"+publisher+".txt")
	removeDataNoise("org", "devtemp.txt", "data_augmentation/refinedatadev_"+publisher+".txt")


	AUGMENT_THRESHOLD = 1 # 0 means no augmentation, 1 means 100% augmentation

	dataAugment("data_augmentation/refinedatatrain_"+publisher+".txt", AUGMENT_THRESHOLD)
	dataAugment("data_augmentation/refinedatatest_"+publisher+".txt", AUGMENT_THRESHOLD)
	dataAugment("data_augmentation/refinedatadev_"+publisher+".txt", AUGMENT_THRESHOLD)