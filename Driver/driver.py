# coding=utf-8
import jieba
import ddr
from DDR import load_content_from_csv, get_jieba_cut_results, load_ddr_from_csv, get_emotion_words_from_ddr, \
    extract_emotion_library, calculation_sim, get_words_not_in_ddr, sort_values, load_binary_to_model
from DDR import load_binary_to_model
from check import check_json_rows_number
from cleaner import clean_each_line, clean, clean_again
from config import CORE_NUMBER, COMBO_FPR_SIM
from fileLoader import saveDicToJson, csvLoader, loadJsonToDict, save_dict_to_csv, save_list_to_json, split, \
    combine_multiple_to_one, convert_json_to_csv
from logtidueToCity import convert_lat_long_to_city, get_conn, update_city_info, update_city_info_normal
import logging

from preprocessing import tweets_cleaning

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import multiprocessing as mp

def threading(dicData, total):

    pool = mp.Pool()

    mydict = mp.Manager().list()
    lock=mp.Manager().Lock()
    for i in range(1,CORE_NUMBER+1):
        pool.apply_async(update_city_info, (dicData, i, total, CORE_NUMBER, mydict, lock))
    pool.close()
    pool.join()
    return mydict


def main():
    """
    #进行去除没有地理信息的
    contentInCsv = csvLoader(sourFilePath, maxiMum, UNLIMITED, GEOINFO)
    saveDicToJson(contentInCsv, nameForSaving)


    #进行城市信息更新
    path_for_json ="/Users/xingwenpeng/PycharmProjects/nlp/Output/full2020-01Geoinfo.json"
    dicData = loadJsonToDict(path_for_json)
    total = len(dicData)

    logger.info("The total number of info is %s", total)
    #updated_dict = update_city_info(dicData)
    updated_dict = update_city_info_normal(dicData, total)
    #updated_dict = threading(dicData, total)
    save_dict_to_csv(updated_dict, "Output", "2020-01CityInfo")
    save_list_to_json(updated_dict, "Output", "2020-01CityInfo")
    logger.info("The total number of info is %s", total)


    #进行拆分
    path_for_json = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-01-03Geoinfo.json"
    dict_1, dict_2, dict_3 = split(path_for_json)
    save_dict_to_csv(dict_1, "Output", "2020-02-03-01Geoinfo")
    save_list_to_json(dict_1, "Output", "2020-02-03-01Geoinfo")
    save_dict_to_csv(dict_2, "Output", "2020-02-03-02Geoinfo")
    save_list_to_json(dict_2, "Output", "2020-02-03-02Geoinfo")
    save_dict_to_csv(dict_3, "Output", "2020-02-03-03Geoinfo")
    save_list_to_json(dict_3, "Output", "2020-02-03-03Geoinfo")

    #检查数量
    path_for_json_city = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03Cityinfo.json"
    path_for_json_geo = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03Geoinfo.json"
    check_json_rows_number(path_for_json_city)
    check_json_rows_number(path_for_json_geo)


    #清洗
    path_for_json = "/Users/xingwenpeng/PycharmProjects/nlp/Combo/07-31.json"
    dicData = loadJsonToDict(path_for_json)
    updated_dict = clean(dicData)
    save_dict_to_csv(updated_dict, "Cleaned", "07-31-Cleaned")
    save_list_to_json(updated_dict, "Cleaned", "07-31-Cleaned")

    #预处理-清洗
    csv_to_process = '/Users/xingwenpeng/PycharmProjects/nlp/Cleaned/07-29-Cleaned.csv'
    tweets_cleaning(csv_to_process)

    #合并
    files_list = ["/Users/xingwenpeng/PycharmProjects/nlp/Output/2019-12Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-01Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-01Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-02-01Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-02-02Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-02-03Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03-01Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03-02Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03-03Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-01Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-02Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-03-01Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-03-02Cityinfo.json",
                  "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-03-03Cityinfo.json"
                  ]
    name = "07-31"
    combine_multiple_to_one(files_list,  "Combo", name)
    path_to_json = "/Users/xingwenpeng/PycharmProjects/nlp/Combo/" + name + ".json"
    convert_json_to_csv(path_to_json, "Combo", name)

    #将json转为csv存储
    path_to_json = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-01Cityinfo.json"
    convert_json_to_csv(path_to_json, "CityInfo", "2020-01Cityinfo")


    # 再次清洗
    path_for_csv = "/Users/xingwenpeng/PycharmProjects/nlp/Files/CleanedTweets_XUWEI.csv"
    clean_again(path_for_csv)

    #获得情感字典中缺少的词
    wors_not_in_ddr = get_words_not_in_ddr()



    #获得微博内容的分词
    csv_to_load = "/Users/xingwenpeng/PycharmProjects/nlp/Files/cleaned_again.csv"
    words = get_jieba_cut_results(csv_to_load, 'results_after_jieba_cut', 'jieba_2020-08-01')

    #保存模型
    load_binary_to_model()

    #从情感字典分类提取词汇
    emotion_words_dict = extract_emotion_library()

    #从jiebacutresult读取words
    path = '/Users/xingwenpeng/PycharmProjects/nlp/results_after_jieba_cut/jieba_2020-08-01.csv'
    words = load_content_from_csv( path, 'jieba')

    #计算词汇与字典相似度
    calculation_sim(emotion_words_dict, words)
    """


    #对字典进行排序
    path = '/Users/xingwenpeng/PycharmProjects/nlp/SIM/combo_sim_2020-08-01.csv'
    data = load_content_from_csv(path)
    emotion_words_dict = extract_emotion_library()
    sort_values(data, emotion_words_dict)


















if __name__ == "__main__":
    # execute only if run as a script
    main()