import re
import random
import string
import unicodedata


class DataGenerator:

    def __init__(self, cur: 'sqlite3.Cursor'):
        """ DataGenerator generates BIO-tagged data for NER training
        - cur (sqlite3.Cursor): Cursor to database
        """
        self.cur = cur
        # Thresholds represent length that constitutes valid entities and values are inclusive
        self.per_lower_thres, self.per_upper_thres = 1, 3
        self.org_lower_thres, self.org_upper_thres = 1, 4
        self.role_lower_thres, self.role_upper_thres = 1, 3
        # Chance of entity being added to line
        self.per_chance, self.org_chance, self.role_chance = 1.0, 0.8, 0.2

    def clean(self, ltext: str):
        """ Clean text
        """
        # Strip leading and trailing punctuation
        ltext = ltext.strip(string.punctuation)
        # Replace tabs and newlines with spaces
        ltext = re.sub('\t|\r|\n|\(|\)', ' ', ltext)
        # Normalize string to remove splitting errors
        ltext = unicodedata.normalize("NFKD", ltext)
        ltext = re.sub(' +', ' ', ltext)  # Remove multiple spacing
        ltext = ltext.strip()
        return ltext

    def get_valid_entities(self):
        """ Get valid PER / ORG / ROLE tokens
        - Length that constitutes valid entities as in settings
        """
        # Roles
        roles = cur.execute(
            "SELECT pr.role_type FROM PersonRole pr GROUP BY role_type HAVING COUNT(*) > 100").fetchall()
        roles = set([role[0].strip() for role in roles])
        valid_roles = set(filter(lambda p: len(p.split(' '))
                                 < self.role_upper_thres, roles))
        # Persons
        persons = cur.execute("SELECT name FROM Persons").fetchall()
        persons = set([person[0].strip() for person in persons])
        valid_pers = set(filter(lambda p: len(p.split(' '))
                                < self.per_upper_thres, persons))
        # Organizations
        organizations = cur.execute(
            "SELECT name FROM Organizations").fetchall()
        organizations = set([org[0].strip() for org in organizations])
        valid_orgs: 'Set' = set(filter(lambda o: len(o.split(' ')) > self.org_lower_thres and len(
            o.split(' ')) < self.org_upper_thres, organizations))

        return valid_pers, valid_orgs, valid_roles

    def generate_vocab(self):
        # Character vocab
        with open('./char_vocab.txt', 'w') as char_file:
            chars = []
            with open('./train.txt') as train:
                for line in train:
                    line_chars = set(list(line))
                    chars = chars.union(line_chars)
            for char in chars:
                char_file.write(f"{char}\n")

        tags = ['B-PER', 'I-PER', 'B-ORG', 'I-ORG', 'B-ROLE', 'I-ROLE', 'O']
        with open('./tag_vocab.txt', 'w') as tag_file:
            for tag in tags:
                tag_file.write(f"{tag}\n")

    def generate(self, train_size, val_size):
        """ Generation of data files
        """
        valid_pers, valid_orgs, valid_roles = self.get_valid_entities()
        with open('./train.txt', 'w') as po_file:
            for i in range(train_size):
                per = random.sample(valid_pers, 1)[0]
                org = random.sample(valid_orgs, 1)[0]
                role = random.sample(valid_roles, 1)[0]
                po_file.write(self.format_line(per, org, role))

        with open('./val.txt', 'w') as po_file:
            for i in range(val_size):
                per = random.sample(valid_pers, 1)[0]
                org = random.sample(valid_orgs, 1)[0]
                role = random.sample(valid_roles, 1)[0]
                po_file.write(self.format_line(per, org, role))

    def format_line(self, per, org, role):
        """ Assigns BIO tagging to tokens
        - Chance of having PER, ORG and ROLE in line as in settings
        """
        tokens = []
        tags = []

        if random.random() < self.per_chance:
            per_tokens = self.clean(per).split(' ')
            per_tags = ['B-PER'] + (len(per_tokens) - 1) * ['I-PER']
            tokens.append(per_tokens)
            tags.append(per_tags)

        if random.random() < self.org_chance:
            org_tokens = self.clean(org).split(' ')
            org_tags = ['B-ORG'] + (len(org_tokens) - 1) * ['I-ORG']
            tokens.append(org_tokens)
            tags.append(org_tags)

        if random.random() < self.role_chance:
            role_tokens = self.clean(role).split(' ')
            role_tags = ['B-ROLE'] + (len(role_tokens) - 1) * ['I-ROLE']
            tokens.append(role_tokens)
            tags.append(role_tags)

        # Shuffle person/organization/role
        zipped = list(zip(tokens, tags))  # Pair tokens with tags
        random.shuffle(zipped)  # Shuffle
        tokens, tags = zip(*zipped)  # Unpair to tokens and tags

        def flatten(l): return [item for sublist in l for item in sublist]
        line = list(zip(flatten(tokens), flatten(tags)))
        line = [' '.join([token, tag]) for token, tag in line]
        return f"{' '.join(line)}\n"
