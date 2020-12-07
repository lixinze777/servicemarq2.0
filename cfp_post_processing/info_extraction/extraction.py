from .ie_utils import Conference, consolidate_line_nums
from .extractors import BlockExtractor, LineInfoExtractor, LineInfoExtractor_P


def extract_line_information(cnx, extract_from, extract_type,
                             indent_diff, linenum_diff, conf_ids):
    cur = cnx.cursor()
    block_extractor = BlockExtractor(cur, extract_type)
    if extract_from == "proceedings":
        lineinfo_extractor = LineInfoExtractor_P(cur, extract_type)
    elif extract_from == "websites":
        lineinfo_extractor = LineInfoExtractor(cur, extract_type)
    else:
        raise Exception("Undefined extract_from type, must be either proceedings or websites")
        return
    for conf_id in conf_ids:
        accessibility = cur.execute("SELECT accessible FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
        accessibility = accessibility[0] if accessibility else ""
        if 'Accessible' in accessibility:
            conf_title = cur.execute("SELECT title FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
            print("=========================== Info extraction for Conference {} =================================".format(conf_id))
            print("=========================== {} ===========================".format(conf_title[0]))
            conf_tuple = cur.execute(
                "SELECT * FROM WikicfpConferences WHERE id=?", (conf_id,)).fetchone()
            relevant_blocks = block_extractor.get_relevant_blocks(
                conf_id, indent_diff, linenum_diff)
            relevant_blocks = consolidate_line_nums(relevant_blocks)
            conference: 'Conference' = Conference(conf_tuple, relevant_blocks)
            lineinfo_extractor.process_conference(conference)
            cnx.commit()
        else:
            print("=========================== Inaccessible Conference {} =================================".format(conf_id))

    cur.close()
