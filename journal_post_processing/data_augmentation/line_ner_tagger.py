from flair.data import Corpus
from flair.embeddings import TokenEmbeddings, WordEmbeddings, StackedEmbeddings, BytePairEmbeddings
from flair.datasets import ColumnCorpus
from typing import List


# 1. get the corpus
#corpus: Corpus = UD_ENGLISH().downsample(0.1)
#corpus = corpus.downsample(0.06125)
columns = {0:'text', 1:'ner'}
data_folder = '/home/wing.nus/xinze/journal-mining/journal_crawl/post_processing/data_augmentation'
corpus: Corpus = ColumnCorpus(data_folder, columns, train_file = 'refinedatatrain.txt', test_file = 'refinedatatest.txt', dev_file = 'refinedatadev.txt')
print(corpus)

# 2. tag do to predict?
tag_type = 'ner'

# 3. make the tag dictionary from the corpus
tag_dictionary = corpus.make_tag_dictionary(tag_type=tag_type)
print(tag_dictionary)

# 4. initialize embeddings
embedding_types: List[TokenEmbeddings] = [
    #WordEmbeddings('glove'),
    BytePairEmbeddings('en')
]


embeddings: StackedEmbeddings = StackedEmbeddings(embeddings=embedding_types)

# 5. initialize sequence tagger
from flair.models import SequenceTagger

tagger: SequenceTagger = SequenceTagger(hidden_size=256,
                                        embeddings=embeddings,
                                        tag_dictionary=tag_dictionary,
                                        tag_type=tag_type,
                                        use_crf=True)

# 6. initialize trainer
from flair.trainers import ModelTrainer

trainer: ModelTrainer = ModelTrainer(tagger, corpus)

# 7. start training
trainer.train('resources/taggers/bpe',
              learning_rate=0.1,
              mini_batch_size=32,
              max_epochs=100)
