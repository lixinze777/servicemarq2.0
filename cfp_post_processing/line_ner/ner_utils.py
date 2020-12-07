class Entity:
    def __init__(self, ent_type: str, ent_string: str):
        self.type = ent_type
        self.string = ent_string


def get_entities(tagged_line: 'Tuple[List, List]'):
    """ Retrieves entities from tagged line
    - tagged_line is a tuple of list of tokens and list of tags
    """
    entities = []
    cur_entity = None
    tokens, tags = tagged_line

    for token, tag in zip(tokens, tags):
        if tag[0] == 'B':
            entities.append(cur_entity)
            cur_entity = Entity(tag[2:], token)
        if tag[0] == 'I':
            cur_entity.string = ' '.join([cur_entity.string, token])
        else:
            pass
    entities.append(cur_entity)

    return list(filter(lambda x: x, entities))
