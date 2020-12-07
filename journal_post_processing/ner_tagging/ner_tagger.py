import sys
sys.path.append("..") 
sys.path.append("../../journal_crawl/journal_spider")
sys.path.append("../journal_crawl/journal_spider")

from database.database_config import DB_FILEPATH
from database.database_helper import DatabaseHelper
import items

from flair.models import SequenceTagger
from flair.data import Sentence

class NER_Tagger:

	def __init__(self):
		# load the model you trained
		self.model = SequenceTagger.load('data_augmentation/resources/taggers/bpe-mix/best-model.pt')

	def tag_sentences(self, publisher):

		if publisher == "ALL":
			lines = DatabaseHelper.getLines(DB_FILEPATH)
		else:
			lines = DatabaseHelper.getLines(DB_FILEPATH, publisher)

		counter = 0
		for crawledpublisher, title, sentence in lines:
			sentence = Sentence(sentence)
			self.model.predict(sentence)
			predicted = sentence.to_tagged_string()
			DatabaseHelper.addTaggedLine(items.TaggedLineItem(publisher=crawledpublisher, title=title, line=predicted), DB_FILEPATH)
			counter = counter + 1
		#print(str(counter)+" rows predicted")

		print("--------Finished Predicting-----------")

if __name__ == "__main__":
	tagger = NER_Tagger()
	tagger.tag_sentences("Springer")