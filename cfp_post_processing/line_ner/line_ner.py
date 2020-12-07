import numpy as np
import sqlite3
import torch
import torch.nn as nn
import texar.torch as tx
from functools import reduce
from torchcrf import CRF
from texar.torch.modules import WordEmbedder, UnidirectionalRNNEncoder, FeedForwardNetwork
from texar.torch.data import Embedding
from texar.torch.core import default_rnn_cell_hparams
from texar.torch.data import TextLineDataSource, DatasetBase, Vocab, DataIterator, Embedding
from ner_utils import get_entities


class NERDataset(DatasetBase):
    def __init__(self, datapath, char_vocab, tag_vocab,
                 hparams=None, device=None):
        self.char_vocab = char_vocab
        self.tag_vocab = tag_vocab
        source = TextLineDataSource(datapath)
        super().__init__(source, hparams, device)

    def process(self, raw_example):
        """ Process individual lines of format TOKEN TAG TOKEN TAG ...
        """
        tokens = raw_example[::2]
        tags = raw_example[1::2]

        # Generate line_ids, List[np.arr()] for each line
        line_ids = []
        for token in tokens:
            chars = list(token)
            token_ids = np.array(self.char_vocab.map_tokens_to_ids_py(chars))
            line_ids.append(token_ids)
        # Generate tag_ids
        tag_ids = self.tag_vocab.map_tokens_to_ids_py(tags)
        return {
            'text': tokens,
            'line_ids': line_ids,
            'tags': tags,
            'tag_ids': tag_ids
        }

    def collate(self, examples):
        text = [ex['text'] for ex in examples]
        tags = [ex['tags'] for ex in examples]
        line_ids = [ex['line_ids'] for ex in examples]
        tag_ids = [ex['tag_ids'] - 3 for ex in examples]
        return tx.data.Batch(
            len(examples),
            text=text,
            tags=tags,
            line_ids=line_ids,
            tag_ids=tag_ids
        )


class NERModel(nn.Module):

    def __init__(self, char_vocab, num_tags):
        super().__init__()
        self.num_tags = num_tags
        char_embedding = Embedding(char_vocab.token_to_id_map_py)
        self.embedder = WordEmbedder(init_value=char_embedding.word_vecs)
        self.char_encoder = UnidirectionalRNNEncoder(input_size=50)
        self.token_encoder = UnidirectionalRNNEncoder(input_size=256)
        self.token_classifier = FeedForwardNetwork(hparams={
            "layers": [
                {"type": "Linear", "kwargs": {
                    "in_features": 256, "out_features": num_tags}
                 }
            ]
        })
        self.crf = CRF(num_tags)

    def encode(self, batch):
        batch_logits = torch.tensor([])

        max_len = 0
        for line_char_ids in batch['line_ids']:
            max_len = max(max_len, len(line_char_ids))

        for line_char_ids in batch['line_ids']:
            line, lengths = tx.data.padded_batch(line_char_ids)
            line = torch.tensor(line)

            outputs, final_state = self.char_encoder(
                inputs=self.embedder(line),
                sequence_length=lengths)
            ht, ct = final_state
            token_embeddings = ht.view([len(ht), 1, 256])
            token_lengths = [1] * len(ht)
            outputs, final_state = self.token_encoder(
                token_embeddings, sequence_length=token_lengths)
            logits = self.token_classifier(outputs)

            # Pad instance to max len
            logit_zeros = torch.zeros(max_len - len(logits), 1, self.num_tags)
            padded_logits = torch.cat((logits, logit_zeros))
            batch_logits = torch.cat((batch_logits, padded_logits), dim=1)

        return batch_logits

    def nll_loss(self, batch):
        batch_logits = self.encode(batch)
        loss = self.crf_forward(batch_logits, batch['tag_ids'])
        return -loss

    def crf_forward(self, batch_logits, list_batch_tags):

        max_len = max([len(l) for l in list_batch_tags])
        batch_tags = torch.tensor([], dtype=torch.long)
        batch_mask = torch.tensor([], dtype=torch.uint8)

        for i, tags in enumerate(list_batch_tags):
            tags = list_batch_tags[i]
            tags = torch.tensor(tags).view((len(tags), 1))

            tag_zeros = torch.zeros((max_len - len(tags), 1), dtype=torch.long)
            padded_tags = torch.cat((tags, tag_zeros))
            batch_tags = torch.cat((batch_tags, padded_tags), dim=1)

            mask = torch.zeros((max_len, 1), dtype=torch.uint8)
            mask[:len(tags), :] = torch.ones((len(tags), 1), dtype=torch.uint8)
            batch_mask = torch.cat((batch_mask, mask), dim=1)

        return self.crf(batch_logits, batch_tags, batch_mask)

    def forward(self, batch):
        batch_logits = self.encode(batch)
        return self.crf.decode(batch_logits)


class LineNER:
    def __init__(self, ner_model: 'NERModel'):
        # UNK included to accomodate Texar default vocab
        tags = ['UNK', 'B-PER', 'I-PER', 'B-ORG',
                'I-ORG', 'B-ROLE', 'I-ROLE', 'O']
        self.char_vocab = Vocab('./char_vocab.txt')
        self.tag_vocab = Vocab('./tag_vocab.txt')
        self.ner_model = ner_model

    def create_lines_file(self, lines: 'List[str]'):
        """ Create temporary file for processing of current lines
        """
        tagged_lines = []
        with open('./lines.txt', 'w') as line_file:
            for line in lines:
                tokens = line.strip().split(' ')
                tagged_line = [val for pair in zip(
                    tokens, ['UNK'] * len(tokens)) for val in pair]  # Add UNK tag for dataset processing
                tagged_line = ' '.join(tagged_line)
                line_file.write(f"{tagged_line}\n")

    def get_line_ents(self, lines: 'List[str]'):
        """ Extracts list of entities from lines retrieved from database
        """
        self.create_lines_file(lines)  # To accomodate dataset generation
        dataset = NERDataset('./lines.txt', self.char_vocab, self.tag_vocab)
        data_iterator = DataIterator(dataset)
        line_tokens, decoded_tags = [], []

        for batch in data_iterator:
            line_tokens += batch['text']
            tag_ids = self.ner_model(batch)
            batch_decoded_tags = []
            for line_tag_ids in tag_ids:
                line_decoded_tags = []
                for decoded_id in line_tag_ids:
                    line_decoded_tags.append(
                        self.tag_vocab.id_to_token_map_py[decoded_id + 3])
                batch_decoded_tags.append(line_decoded_tags)
            line_tokens += batch['text']
            decoded_tags += batch_decoded_tags

        # Ensure decoded tags are of same sequence length as tokens
        tagged_lines = zip(line_tokens, decoded_tags)
        tagged_lines = [(tokens, tags[:len(tokens)])
                        for tokens, tags in tagged_lines]

        entities = []
        for tagged_line in tagged_lines:
            entities += get_entities(tagged_line)
        return entities
