import argparse
import sqlite3

from process_lines import add_page_lines
from svm_line_classification.svm_predict_lines import svm_predict_lines
from dl_line_classification.rnn_predict_lines import rnn_predict_lines, LineClassifier
from info_extraction.extraction import extract_line_information
from utils import create_tables

parser = argparse.ArgumentParser(description='')
parser.add_argument('db_filepath', type=str,
                    help="Specify database file to predict lines")
args = parser.parse_args()

cnx = sqlite3.connect(args.db_filepath)
cur = cnx.cursor()
create_tables(cnx)

# Indexes of accessible conferences to process
PROCESS_LINES = True
PREDICT_LINES_DL = True
EXTRACT_INFO = True
CONF_IDS = [1]

""" Process Lines
- Processes HTML of each page to lines, ordered by conference_id
"""
if PROCESS_LINES:
    add_page_lines(cnx, CONF_IDS)
    cnx.commit()

""" Predict Lines
- Adds prediction of line information, ordered by conference_id
"""

VOCAB_FILEPATH = "./dl_line_classification/vocab.txt"
LABEL_VOCAB_FILEPATH = "./dl_line_classification/label_vocab.txt"
TAG_VOCAB_FILEPATH = "./dl_line_classification/tag_vocab.txt"
MODEL_FILEPATH = "./dl_line_classification/rnn_classifier"
if PREDICT_LINES_DL:
    rnn_predict_lines(cnx, MODEL_FILEPATH,
                      VOCAB_FILEPATH, LABEL_VOCAB_FILEPATH, TAG_VOCAB_FILEPATH,
                      CONF_IDS)
    cnx.commit()

""" Extraction of Conference - Person - Affiliation information
"""
EXTRACT_FROM = 'websites' # Type of content for extraction: websites / proceedings
EXTRACT_TYPE = 'dl_prediction' # Type of label: dl_prediction / gold
INDENT_DIFF_THRESHOLD = 12
LINENUM_DIFF_THRESHOLD = 10
if EXTRACT_INFO:
    extract_line_information(cnx, EXTRACT_FROM, EXTRACT_TYPE,
                             INDENT_DIFF_THRESHOLD, LINENUM_DIFF_THRESHOLD,
                             CONF_IDS)

cur.close()
cnx.close()
