

import json
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def check_json_rows_number(path):
    logger.info("Start loading json file")
    with open(path, 'r', encoding='utf8')as fp:
        dictData = json.load(fp)
        logger.info("The total number of dict is %s", len(dictData))


def check_if_path_exist_or_mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def check_if_in_ddr(word):

    return True