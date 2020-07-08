from cleaner import clean_each_line, clean
from fileLoader import saveDicToJson, csvLoader, loadJsonToDict, save_dict_to_csv, save_list_to_json, split

from logtidueToCity import convert_lat_long_to_city, get_conn, update_city_info
import logging
maxiMum = 1000
core_number = 9
UNLIMITED = "unlimited"
LIMITED = "limited"
GEOINFO = True

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

sourFilePath = "/Volumes/Xing Wenpeng/weibo/2020-02.csv"
nameForSaving = "/full2020-02Geoinfo.json"
import multiprocessing as mp

def threading(dicData, total):

    pool = mp.Pool()

    mydict = mp.Manager().list()
    lock=mp.Manager().Lock()
    for i in range(1,core_number+1):
        pool.apply_async(update_city_info, (dicData, i, total, core_number, mydict,lock))
    pool.close()
    pool.join()
    return mydict


def main():
    """
    #进行去除没有地理信息的
    contentInCsv = csvLoader(sourFilePath, maxiMum, UNLIMITED, GEOINFO)
    saveDicToJson(contentInCsv, nameForSaving)


    #进行城市信息更新
    path_for_json ="/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-01Geoinfo.json"
    path_to_save_csv = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-01CityInfo.csv"
    path_to_save_json = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-03-01CityInfo.json"

    dicData = loadJsonToDict(path_for_json)
    total = len(dicData)
    logger.info("The total number of info is %s", total)
    #updated_dict = update_city_info(dicData)
    updated_dict = threading(dicData, total)
    save_dict_to_csv(updated_dict, path_to_save_csv)
    save_list_to_json(updated_dict, path_to_save_json)
    logger.info("The total number of info is %s", total)


    #进行拆分
    path_for_json = "/Users/xingwenpeng/PycharmProjects/nlp/Output/full2020-02Geoinfo.json"

    path_to_save_csv_1 = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-01Geoinfo.csv"
    path_to_save_json_1 = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-01Geoinfo.json"

    path_to_save_csv_3 = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03Geoinfo.csv"
    path_to_save_json_3 = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-03Geoinfo.json"

    path_to_save_csv_2 = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-02Geoinfo.csv"
    path_to_save_json_2 = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-02Geoinfo.json"

    dict_1, dict_2, dict_3 = split(path_for_json)

    save_dict_to_csv(dict_1, path_to_save_csv_1)
    save_list_to_json(dict_1, path_to_save_json_1)

    save_dict_to_csv(dict_2, path_to_save_csv_2)
    save_list_to_json(dict_2, path_to_save_json_2)

    save_dict_to_csv(dict_3, path_to_save_csv_3)
    save_list_to_json(dict_3, path_to_save_json_3)

    """

    #清洗

    path_for_json = "/Users/xingwenpeng/PycharmProjects/nlp/Output/full2020-02Geoinfo.json"
    path_to_save_csv = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-01Cleaned.csv"
    path_to_save_json = "/Users/xingwenpeng/PycharmProjects/nlp/Output/2020-02-01Cleaned.json"

    dicData = loadJsonToDict(path_for_json)
    updated_dict = clean(dicData)
    save_dict_to_csv(updated_dict, path_to_save_csv)
    save_list_to_json(updated_dict, path_to_save_json)

    #line = "#2019MMA#日本老头儿都是秋名山车神吧？❤开车无一例外都那么猛！�叮～按时长大  我一没喝多的下了车都快飘了！！@武汉红十字基金会 晕死！！！[开心] 日本·东京 显示地图"


if __name__ == "__main__":
    # execute only if run as a script
    main()