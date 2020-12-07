import functools
import numpy as np
import pandas as pd
import pickle
from typing import Dict
from sklearn.preprocessing import LabelEncoder


class SVMLinePredictor:
    """ Loads SVM Line Predictor for labelling of PageLines
    """

    def __init__(self, svm_path: str, tfidfvec_path: str):
        with open(svm_path, 'rb') as svm_file:
            self.svm = pickle.load(svm_file)
        with open(tfidfvec_path, 'rb') as vec_file:
            self.tfidf_vectorizer = pickle.load(vec_file)

        self.custom_features = {
            'num_tokens': True,
            'percent_cap': True,
            'tag': True
        }
        self.labels = ['Affiliation', 'Complex',
                       'Person', 'Role-Label', 'Undefined']

    def get_predicted_vecs(self, linesvec):
        """ Takes in transformed lines from tfidf_vectorizer and returns class probabilities and confidences
        """
        # Platt scaling used for probability, does not correspond to actual prediction
        probabilities = self.svm.predict_proba(linesvec)
        # Confidence used for predicted label instead
        confidences = self.svm.decision_function(linesvec)
        predicted = np.argmax(confidences, axis=1)
        return probabilities, confidences, predicted

    def add_custom_features(self, tfidf_vec: 'ndarray', df):
        """ Combines already generated tfidf vector and appends custom features using df
        Hand crafted features
            - Number of tokens
            - Capitalization of each word
            - Tag
        """
        def num_tokens(line):
            return len(line.split(" "))

        def percent_cap(line):
            tokens = list(filter(lambda x: x != "", line.split(" ")))
            num_cap = functools.reduce(
                lambda acc, el: acc + (1 if el[0].isupper() else 0), tokens, 0)
            return num_cap / len(tokens)

        def concat_features(data_vec, data):
            for feature, use in self.custom_features.items():
                if use:
                    if feature == 'num_tokens':
                        feature_vec = data['line_text'].apply(num_tokens)
                    elif feature == 'percent_cap':
                        feature_vec = data['line_text'].apply(percent_cap)
                    elif feature == 'tag':
                        tag_encoder = LabelEncoder()
                        feature_vec = tag_encoder.fit_transform(data['tag'])
                    else:
                        raise Exception("Invalid custom feature")
                    data_vec = np.c_[data_vec, feature_vec]

            return data_vec

        return concat_features(tfidf_vec, df)

    def predict_lines(self, cur, df, confidence_threshold):
        """ Assigns predicted labels to Page Lines if above confidence threshold else skips label for line
        """
        tfidf_vec = self.tfidf_vectorizer.transform(df['line_text']).toarray()
        lines_vec = self.add_custom_features(tfidf_vec, df)

        predicted_probabilities, predicted_confidences, predicted = self.get_predicted_vecs(
            lines_vec)

        for i, predicted_index in enumerate(predicted):
            predicted_probability = predicted_probabilities[i][predicted_index]
            if predicted_probability > confidence_threshold:
                cur.execute("UPDATE PageLines SET (svm_prediction)=(?) WHERE id={}".format(df.iloc[i][0]),
                            (self.labels[predicted_index],))
            else:
                pass


def svm_predict_lines(cnx, svm_filepath, tfidf_filepath,
                      conf_ids, confidence_thresh=0.8):
    """ Predict pagelines for Conference and saves to database
    """
    cur = cnx.cursor()
    for conf_id in conf_ids:
        accessibility = cur.execute("SELECT accessible FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
        accessibility = accessibility[0] if accessibility else ""
        if 'Accessible' in accessibility:
            print("=========================== SVM Predicting for Conference {} =================================".format(conf_id))
            line_predictor = SVMLinePredictor(svm_filepath, tfidf_filepath)
            confpages = cur.execute(
                "SELECT id, url FROM ConferencePages WHERE conf_id={}".format(conf_id)).fetchall()
            for confpage in confpages:
                confpage_id = confpage[0]
                pagelines_df = pd.read_sql(
                    "SELECT * FROM PageLines WHERE page_id={}".format(confpage_id,), cnx)
                pagelines_df['line_text'] = pagelines_df['line_text'].str.strip()
                pagelines_df = pagelines_df[pagelines_df['line_text'] != ""]
                if len(pagelines_df) > 0:
                    line_predictor.predict_lines(
                        cur, pagelines_df, confidence_thresh)
                else:
                    print("Empty DataFrame")
        else:
            print("=========================== Inaccessible Conference {} =================================".format(conf_id))
    cur.close()
