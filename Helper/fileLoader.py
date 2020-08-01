import codecs
import csv
import logging
import json
import os
from logtidueToCity import convert_lat_long_to_city, get_conn, update_city_info

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


#nameForSaving = "/first{}Output2020-01GeoInfo.json".format(maxiMum)
#nameForSaving = "/first{}Output2020-03.json".format(maxiMum)

def csvLoader(path, maxiMum, mode, GEOINFO):

    logger.info("Start loading csv files in %s with mode %s, geoinfo only?: %s", path, mode, GEOINFO)
    contentInCsv = []
    countNumberOfWbWithGeoInfo = 0
    count = 0
    with open(path, 'r') as f:
        reader = csv.reader(f)

        for row in reader:
            count = count + 1
            if count == maxiMum and mode == "limited":
                break
            rowDic = {}
            rowDic['geo_info'] = row[9]
            rowDic['_id'] = row[0]
            rowDic['user_id'] = row[1]
            rowDic['crawl_time'] = row[2]
            rowDic['created_at'] = row[3]
            rowDic['like_num'] = row[4]
            rowDic['repost_num'] = row[5]
            rowDic['comment_num'] = row[6]
            rowDic['content'] = row[7]
            rowDic['origin_weibo'] = row[8]
            if not GEOINFO:
                contentInCsv.append(rowDic)

            if len(rowDic['geo_info']) >= 2:
                countNumberOfWbWithGeoInfo = countNumberOfWbWithGeoInfo +1
                if GEOINFO:
                    contentInCsv.append(rowDic)
    logger.info("The total number of weibos in {} is {}, {} found with geoinfo({}%)".format(path, count, countNumberOfWbWithGeoInfo, countNumberOfWbWithGeoInfo/count*100))
    return contentInCsv


def save_list_to_json(dict, sub_dir, name):
    dir = os.path.abspath(os.path.join(os.getcwd(), "..")) + "/" + sub_dir + "/"
    if not os.path.exists(dir):
        logger.info("Creating dir %s", dir)
        os.makedirs(dir)
    logger.info("Start saving list to json, the size is %s", len(dict))
    path = dir + name + '.json'
    with open(path, "w") as f:
        ouput = {}
        count = 0
        for item in dict:
            ouput[count] = item
            count = count + 1
        f.write(json.dumps(ouput, ensure_ascii=False))



def save_dict_to_csv(dict_list, sub_dir, name):
    dir = os.path.abspath(os.path.join(os.getcwd(), "..")) + "/" + sub_dir + "/"
    if not os.path.exists(dir):
        logger.info("Creating dir %s", dir)
        os.makedirs(dir)
    logger.info("Start saving dict to csv, the size is %s", len(dict_list))
    path = dir + name + ".csv"
    f = codecs.open(path, 'w', 'utf_8_sig')
    writer = csv.writer(f)
    flag = True
    count = 0
    for item in dict_list:
        if flag:
            keys = item.keys()
            writer.writerow(keys)
            flag = False
        else:
            writer.writerow(list(item.values()))
        count = count + 1
    logger.info("Finish saving files to csv, the size is %s", count)
    f.close()

def save_dict_to_csv_for_cleaning(dict,sub_dir, name):
    dir = os.path.abspath(os.path.join(os.getcwd(), "..")) + "/" + sub_dir
    if not os.path.exists(dir):
        logger.info("Creating dir %s", dir)
        os.makedirs(dir)
    logger.info("Start saving files to csv, the size is %s", len(dict))
    path = dir + name
    f = codecs.open(path, 'w', 'utf_8_sig')
    writer = csv.writer(f)
    flag = True
    for item in dict:
        if flag:
            keys = ['geo_info', '_id', 'user_id', 'crawl_time', 'created_at', 'like_num','repost_num', 'comment_num', 'content', 'origin_weibo', 'city', 'emotion', '##', 'at']
            writer.writerow(keys)
            flag = False
        else:
            writer.writerow(list(item.values()))
    f.close()


def saveDicToJson(sourceFile, name, sub_dir):

    logger.info("Start saving to Json: %s ", name)
    ouput = {}
    count = 0
    for row in sourceFile:
        ouput[count] = row
        count = count+1
    dir = os.path.abspath(os.path.join(os.getcwd(), "..")) + "/" + sub_dir
    if not os.path.exists(dir):
        logger.info("Creating dir %s", dir)
        os.makedirs(dir)
    try:
        finalPath = dir + name
        logger.info("Final path to save is %s", finalPath)
        with open(finalPath, "w") as f:
            f.write(json.dumps(ouput, ensure_ascii=False))
    except FileNotFoundError as e:
        logger.error(e)

def loadJsonToDict(path):
    logger.info("Start loading json file %s", path)
    with open(path, 'r', encoding='utf8')as fp:
        dictData = json.load(fp)
        return dictData

def split(path):
    logger.info("Start spliting")
    dict = loadJsonToDict(path)

    logger.info("The total number of dict is %s", len(dict))

    total = len(dict)

    dict_1, dict_2, dict_3 = [], [], []

    i = 1
    i_1 = 0
    i_2 = 0
    i_3 = 0

    while True:


        if dict.get(str(i)) ==  None:
            logger.info("The ending number is %s, the total number is %s", i, total)
            break

        if i <= total/3:
            logger.info("dict_1 %s", i_1)
            dict_1.append(dict.get(str(i)))
            i_1 = i_1 + 1
            i = i + 1
            continue

        if i <= total*2/3:
            logger.info("dict_2 %s", i_2)
            dict_2.append(dict.get(str(i)))
            i_2 = i_2 + 1
            i = i + 1
            continue

        if i <= total:
            logger.info("dict_3 %s", i_3)
            dict_3.append(dict.get(str(i)))
            i_3 = i_3 + 1
            i = i + 1
            continue

    logger.info("The splittled dict is in size of %s, %s, %s, the sum is %s, the total is %s", len(dict_1),  len(dict_2), len(dict_3), len(dict_1)+len(dict_2)+len(dict_3), total)
    return dict_1, dict_2, dict_3


def convert_json_to_csv(path_to_json, sub_dir, name):
    dict_file = loadJsonToDict(path_to_json)
    dict_list = []
    max = len(dict_file)
    count = 0
    while count <= max:
        line = dict_file.get(str(count))
        if line == None:
            break

        dict_list.append(line)

        count = count + 1

    save_dict_to_csv(dict_list, sub_dir, name)





def combine_multiple_to_one(files_list, sub_dir, name):


    dic_data = []
    for path in files_list:
        logger.info("start combining %s into the combo %s", path, name)
        dict_file = loadJsonToDict(path)
        max = len(dict_file)
        count = 0
        while count <= max:
            line = dict_file.get(str(count))
            if line == None:
                break
            dic_data.append(line)
            count = count + 1
    logger.info("Finish combining all %s files, the size of the combo is %s", len(files_list), len(dic_data))
    logger.info("Start saving combo %s", name)
    save_list_to_json(dic_data, sub_dir, name)


    return dic_data



