from fileLoader import saveDicToJson, csvLoader, loadJsonToDict

from logtidueToCity import convert_lat_long_to_city, get_conn, update_city_info

maxiMum = 1000

UNLIMITED = "unlimited"
LIMITED = "limited"
GEOINFO = True

sourFilePath = "/Volumes/Xing Wenpeng/weibo/2019-12.csv"
nameForSaving = "/full2019-12Geoinfo.json"

def main():




    #contentInCsv = csvLoader(sourFilePath, maxiMum, UNLIMITED, GEOINFO)

    #saveDicToJson(contentInCsv, nameForSaving)









    path_for_json ="/Users/xingwenpeng/PycharmProjects/nlp/Output/full2019-12Geoinfo.json"

    dicData = loadJsonToDict(loadJsonToDict(path_for_json))
    update_city_info(dicData)

    #convert_lat_long_to_city(conn, lat, long)



if __name__ == "__main__":
    # execute only if run as a script
    main()