import csv
import re
import random
import string
import sqlite3
import unicodedata


class DataGenerator:

    def __init__(self, cur):
        self.cur = cur

    def clean(self, ltext: str):
        """Strips surrounding punctuation and replaces all tabs and consecutive spaces

        Args:
            ltext (str): text to clean

        Returns:
            ltext: cleaned text
        """
        # Strip leading and trailing punctuation
        ltext = ltext.strip(string.punctuation)
        # Replace tabs and newlines with spaces
        ltext = re.sub('\t|\r|\n|\(|\)', ' ', ltext)
        ltext = re.sub(' +', ' ', ltext)  # Remove multiple spacing
        ltext = ltext.strip()
        return ltext

    def generate(self, conf_ids, train_ratio, val_ratio):
        """ Generate dataset for rnn line classification training

        Args:
            conf_ids (List): List of conference ids to generate labelled lines for
            train_ratio (float): ratio of data generated for training
            val_ratio (float): ratio of data generated for validation
        """
        lines = []
        for conf_id in conf_ids:
            page_ids = cur.execute(
                "SELECT id FROM ConferencePages WHERE conf_id=?", (conf_id,)).fetchall()
            for page_id in page_ids:
                page_id = page_id[0]
                page_lines = cur.execute(
                    "SELECT label, tag, indentation, line_text FROM PageLines WHERE label NOT NULL AND page_id=?", (page_id,)).fetchall()
                page_lines = list(
                    filter(lambda l: self.clean(l[3]), page_lines))
                page_lines = list(map(
                    lambda l: (l[0].replace(' ', '-'), l[1], l[2], self.clean(l[3])), page_lines)
                )
                lines += page_lines

        split_value = int(
            len(lines) * (train_ratio / (train_ratio + val_ratio)))

        # Create train,val,test datasets
        with open('./train.tsv', 'w') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONE,
                                delimiter='\t', quotechar='', escapechar='\\')
            writer.writerows(lines[0:split_value])
        with open('./val.tsv', 'w') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONE,
                                delimiter='\t', quotechar='', escapechar='\\')
            writer.writerows(lines[split_value:])

    def generate_vocab(self):
        """ Generate line_tag, label and glove vocab
        """
        with open('./glove.6B.50d.txt', 'r') as glove:
            with open('./vocab.txt', 'w') as vocab:
                for line in glove:
                    vocab.write("{}\n".format(line.split(" ")[0]))

        labels = ['Person', 'Affiliation', 'Complex', 'Role-Label']
        with open('./label_vocab.txt', 'w') as label_v:
            for l in labels:
                label_v.write("{}\n".format(l))

        tags = cur.execute('SELECT DISTINCT tag FROM PageLines WHERE label NOT NULL AND id<?', (max_id,)).fetchall()
        tags = [t[0] for t in tags]
        with open('./tag_vocab.txt', 'w') as tag_v:
            for t in tags:
                tag_v.write("{}\n".format(t))
