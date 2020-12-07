import csv
import re
import string
import sqlite3
import traceback
import torch
import torch.nn as nn
import texar.torch as tx
from texar.torch.data import Vocab, DataIterator, Embedding
from texar.torch.modules import WordEmbedder, BidirectionalRNNEncoder, FeedForwardNetwork


class Dataset(tx.data.DatasetBase):
    def __init__(self, data_path, vocab, label_vocab, tag_vocab, hparams=None, device=None):
        source = tx.data.TextLineDataSource(data_path)
        self.vocab = vocab
        self.label_vocab = label_vocab
        self.tag_vocab = tag_vocab
        super().__init__(source, hparams, device)

    def process(self, raw_example):
        """ Process tokenized lines from file
        """
        pg_id = raw_example[0]
        label = [raw_example[1]]
        tag = [raw_example[2]]
        indentation = [int(raw_example[3])]
        text = [t.lower() for t in raw_example[4:]]
        return {
            "pg_id": pg_id,
            "label": label,
            "tag": tag,
            "indentation": indentation,
            "text": text,
            "label_id": self.label_vocab.map_tokens_to_ids_py(label),
            "tag_id": self.tag_vocab.map_tokens_to_ids_py(tag),
            "text_ids": self.vocab.map_tokens_to_ids_py(text) if text else []
        }

    def collate(self, examples):
        """ Batches processed examples
        """
        pg_ids = [ex["pg_id"] for ex in examples]
        text = [ex["text"] for ex in examples]
        label = [ex["label"] for ex in examples]

        text_ids, lengths = tx.data.padded_batch(
            [ex["text_ids"] for ex in examples])
        # -3 for ridding of <BOS> <EOS> <PAD> idxs
        label_ids = [(ex["label_id"] - 3) for ex in examples]
        tag_ids = [(ex["tag_id"] - 3) for ex in examples]
        indentations = [ex["indentation"] for ex in examples]

        return tx.data.Batch(
            len(examples),
            text=text,
            label=label,
            pg_ids=pg_ids,
            text_ids=torch.from_numpy(text_ids),
            lengths=torch.tensor(lengths),
            label_ids=torch.tensor(label_ids),
            tag_ids=torch.tensor(tag_ids),
            indentations=torch.tensor(indentations)
        )


class LineClassifier(nn.Module):

    def __init__(self, vocab: 'Vocabulary'):
        super().__init__()
        glove_embedding = Embedding(
            vocab.token_to_id_map_py,
            hparams={
                "file": "./dl_line_classification/glove.6B.50d.txt",
                "read_fn": "load_glove",
                "dim": 50
            }
        )
        self.embedder = WordEmbedder(init_value=glove_embedding.word_vecs)
        self.encoder = BidirectionalRNNEncoder(input_size=50)
        self.linear_classifier = FeedForwardNetwork(hparams={
            "layers": [
                {"type": "Linear", "kwargs": {
                    "in_features": 514, "out_features": 5}
                 }
            ]
        })

    def get_parameters(self):
        return list(self.encoder.parameters()) + list(self.linear_classifier.parameters())

    def forward(self, batch):
        outputs, final_state = self.encoder(
            inputs=self.embedder(batch['text_ids']),
            sequence_length=batch['lengths'])
        ht, ct = final_state

        # Generate input to linear classification layer
        concated = torch.cat([ht[0], ht[-1]], dim=1)
        # Add in additional features
        concated = torch.cat(
            [concated, batch['tag_ids'].type(torch.float)], dim=1)
#         concated = torch.cat([concated, batch['indentations'].type(torch.float)], dim=1)
        lengths = batch['lengths'].unsqueeze(dim=1)
        concated = torch.cat([concated, lengths.type(torch.float)], dim=1)

        logits = self.linear_classifier(concated)
        return logits


class RNNLinePredictor:
    """ Loads Bidirectional LSTM Line Predictor for labelling of PageLines
    """

    def __init__(self, model_filepath, vocab_filepath, label_vocab_filepath, tag_vocab_filepath):
        self.vocab = Vocab(vocab_filepath)
        self.tag_vocab = Vocab(tag_vocab_filepath)
        self.label_vocab = Vocab(label_vocab_filepath)
        self.line_classifier = torch.load(model_filepath)

    def get_label(self, logits: 'Tensor'):
        """ Retrieve label from logit output of model
        """
        # +3 taking into consideration <BOS> <EOS> <PAD> idxs (See dataset collate method)
        predicted_idx = logits.argmax(0).item() + 3
        predicted_label = self.label_vocab.id_to_token_map_py[predicted_idx]
        return 'Undefined' if predicted_label == '<UNK>' else predicted_label

    def create_lines_file(self, lines: 'List[Tuple]'):
        """ Create temporary file for processing of current pagelines
        - line tuple of (page_id, label, tag, indentation, line_text)
        """
        # lines = list(filter(lambda l: self.clean(l[4]), lines))
        lines = list(  # Replace label with - since it cannot be None
            map(lambda l: (l[0], '-', l[2], l[3], self.clean(l[4])), lines))
        # Create temporary text file containing data
        with open('./lines.txt', 'w') as lines_file:
            writer = csv.writer(lines_file, quoting=csv.QUOTE_NONE,
                                delimiter='\t', quotechar='', escapechar='\\')
            writer.writerows(lines)

    def clean(self, ltext: str):
        # Strip leading and trailing punctuation
        ltext = ltext.strip(string.punctuation)
        # Replace tabs and newlines with spaces
        ltext = re.sub('\t|\r|\n|\(|\)', ' ', ltext)
        ltext = re.sub(' +', ' ', ltext)  # Remove multiple spacing
        ltext = ltext.strip()
        return ltext

    def predict_lines(self, cur, lines: 'List[Tuple]'):
        """ Updates dl_prediction for lines retrieved from database
        - Processes into batches given lines
        """
        self.create_lines_file(lines)

        data = Dataset('./lines.txt', self.vocab, self.label_vocab,
                       self.tag_vocab, hparams={'batch_size': 32})
        data_iterator = DataIterator(data)
        for batch in data_iterator:  # Batch predictions
            lines_logits = self.line_classifier(batch)
            for i, logits in enumerate(lines_logits):
                page_id = batch.pg_ids[i]
                prediction = self.get_label(logits)
                cur.execute(
                    "UPDATE PageLines SET dl_prediction=? WHERE id=?", (prediction, page_id))


def rnn_predict_lines(cnx, model_filepath,
                      vocab_filepath, label_vocab_filepath, tag_vocab_filepath,
                      conf_ids):
    """ Predict pagelines for Conference and saves to database
    """
    cur = cnx.cursor()
    for conf_id in conf_ids:
        accessibility = cur.execute("SELECT accessible FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
        accessibility = accessibility[0] if accessibility else ""
        if 'Accessible' in accessibility:
            try:
                print("=========================== RNN Predicting for Conference {} =================================".format(conf_id))
                line_predictor = RNNLinePredictor(
                    model_filepath, vocab_filepath, label_vocab_filepath, tag_vocab_filepath)
                confpages = cur.execute(
                    "SELECT id, url FROM ConferencePages WHERE conf_id={}".format(conf_id)).fetchall()
                for confpage in confpages:
                    confpage_id = confpage[0]
                    lines = cur.execute(
                        "SELECT id, label, tag, indentation, line_text FROM PageLines WHERE page_id=?", (confpage_id,)).fetchall()
                    if lines:
                        line_predictor.predict_lines(cur, lines)
                    cnx.commit()
            except Exception as e:
                print(traceback.format_exc())
        else:
            print("=========================== Inaccessible Conference {} =================================".format(conf_id))
    cur.close()
