import codecs
import csv
import operator
import os
import re
import shutil

import jieba
import pandas as pd
import ddr
import logging
from gensim.models import Word2Vec
from check import check_if_path_exist_or_mkdir
from cleaner import update_progress
from config import EMOTION_CSV_TO_LOAD, PATH_TP_CONTENT_AFTER_JIEBA, COMBO_FPR_SIM, BASE_PATH, PATH_TO_MODEL, \
    WORD_NOT_IN_DDR, WORD_WITHOUT_VALUE, PATH_TO_BINARY, PATH_TO_MODEL_WEIBO

logger = logging.getLogger(__name__)
#logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

def load_content_from_csv(csv_to_load, column=None):
    logger.info("Start loading csv: %s", csv_to_load)
    df = pd.read_csv(csv_to_load, header=0, index_col=False, low_memory=False)
    if column:
        content = df[column]
        logger.info("The size of the %s is %s", csv_to_load, len(content))
        return content
    else:
        return df

def del_file(filepath):

    logger.info("Deleting files in %s", filepath)
    del_list = os.listdir(filepath)
    for f in del_list:
        file_path = os.path.join(filepath, f)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


def get_jieba_cut_results(csv_to_load, sub_dir, name):

    content = load_content_from_csv(csv_to_load, 'content')
    all_content = ''.join(str(i) for i in content)
    logger.info("Start jieba cut for %s", csv_to_load)
    words = list(set(jieba.lcut(''.join(re.findall('[\u4e00-\u9fa5]', all_content)), cut_all=True)))

    jiebacut_results = pd.DataFrame()

    jiebacut_results['jieba'] = words
    path_to_save = '/Users/xingwenpeng/PycharmProjects/nlp/' + sub_dir + '/' + name + '.csv'
    logger.info("Saving jieba cur results to csv %s", path_to_save)
    jiebacut_results.to_csv(path_to_save, encoding='utf-8-sig', index=False)
    return words

def load_ddr_from_csv(csv_to_load):

    logger.info("Start loading csv %s", csv_to_load)
    df = pd.read_csv(csv_to_load, header=0, index_col=False, low_memory=False)
    return df


def get_emotion_words_from_ddr(data, emotion, jixing):

    results = data[data.情感分类.isin([emotion]) & data.极性.isin([jixing])]
    return results['词语']



def ab(df):
    return ' '.join(df)

def extract_emotion_library():
    # 读取字典

    emotion_library = load_ddr_from_csv(EMOTION_CSV_TO_LOAD)
    dict = {}
    logger.info("Start extracting emotion words from library...")
    # 分别得到不同情感的词汇
    # 愤怒(NA)
    fennu0 = get_emotion_words_from_ddr(emotion_library, 'NGG', 0)
    dict['fennu0'] = fennu0

    fennu1 = get_emotion_words_from_ddr(emotion_library, 'NGG', 1)
    dict['fennu1'] = fennu1

    fennu2 = get_emotion_words_from_ddr(emotion_library, 'NGG', 2)
    dict['fennu2'] = fennu2

    # 快乐(PA)
    kuaile0 = get_emotion_words_from_ddr(emotion_library, 'PA', 0)
    dict['kuaile0'] = kuaile0
    kuaile1 = get_emotion_words_from_ddr(emotion_library, 'PA', 1)
    dict['kuaile1'] = kuaile1
    kuaile2 = get_emotion_words_from_ddr(emotion_library, 'PA', 2)
    dict['kuaile2'] = kuaile2

    # 安心(PE)
    anxin0 = get_emotion_words_from_ddr(emotion_library, 'PE', 0)
    dict['anxin0'] = anxin0
    anxin1 = get_emotion_words_from_ddr(emotion_library, 'PE', 1)
    dict['anxin1'] = anxin1
    anxin2 = get_emotion_words_from_ddr(emotion_library, 'PE', 2)
    dict['anxin2'] = anxin2

    # 尊敬(PD)
    zunjing0 = get_emotion_words_from_ddr(emotion_library, 'PD', 0)
    dict['zunjing0'] = zunjing0
    zunjing1 = get_emotion_words_from_ddr(emotion_library, 'PD', 1)
    dict['zunjing1'] = zunjing1
    zunjing2 = get_emotion_words_from_ddr(emotion_library, 'PD', 2)
    dict['zunjing2'] = zunjing2

    # 赞扬(PH)
    zanyang0 = get_emotion_words_from_ddr(emotion_library, 'PH', 0)
    dict['zanyang0'] = zanyang0
    zanyang1 = get_emotion_words_from_ddr(emotion_library, 'PH', 1)
    dict['zanyang1'] = zanyang1
    zanyang2 = get_emotion_words_from_ddr(emotion_library, 'PH', 2)
    dict['zanyang2'] = zanyang2

    # 相信(PG)
    xiangxin0 = get_emotion_words_from_ddr(emotion_library, 'PG', 0)
    dict['xiangxin0'] = xiangxin0
    xiangxin1 = get_emotion_words_from_ddr(emotion_library, 'PG', 1)
    dict['xiangxin1'] = xiangxin1
    xiangxin2 = get_emotion_words_from_ddr(emotion_library, 'PG', 2)
    dict['xiangxin2'] = xiangxin2

    # 喜爱(PB)
    xiai0 = get_emotion_words_from_ddr(emotion_library, 'PB', 0)
    dict['xiai0'] = xiai0
    xiai1 = get_emotion_words_from_ddr(emotion_library, 'PB', 1)
    dict['xiai1'] = xiai1
    xiai2 = get_emotion_words_from_ddr(emotion_library, 'PB', 2)
    dict['xiai2'] = xiai2

    # 祝愿(PK)
    zhuyuan0 = get_emotion_words_from_ddr(emotion_library, 'PK', 0)
    dict['zhuyuan0'] = zhuyuan0
    zhuyuan1 = get_emotion_words_from_ddr(emotion_library, 'PK', 1)
    dict['zhuyuan1'] = zhuyuan1
    zhuyuan2 = get_emotion_words_from_ddr(emotion_library, 'PK', 2)
    dict['zhuyuan2'] = zhuyuan2

    # 悲伤(NB)
    beishang0 = get_emotion_words_from_ddr(emotion_library, 'NB', 0)
    dict['beishang0'] = beishang0
    beishang1 = get_emotion_words_from_ddr(emotion_library, 'NB', 1)
    dict['beishang1'] = beishang1
    beishang2 = get_emotion_words_from_ddr(emotion_library, 'NB', 2)
    dict['beishang2'] = beishang2

    # 失望(NJ)
    shiwang0 = get_emotion_words_from_ddr(emotion_library, 'NJ', 0)
    dict['shiwang0'] = shiwang0
    shiwang1 = get_emotion_words_from_ddr(emotion_library, 'NJ', 1)
    dict['shiwang1'] = shiwang1
    shiwang2 = get_emotion_words_from_ddr(emotion_library, 'NJ', 2)
    dict['shiwang2'] = shiwang2

    # 疚(NH)
    neijiu0 = get_emotion_words_from_ddr(emotion_library, 'NH', 0)
    dict['neijiu0'] = neijiu0
    neijiu1 = get_emotion_words_from_ddr(emotion_library, 'NH', 1)
    dict['neijiu1'] = neijiu1
    neijiu2 = get_emotion_words_from_ddr(emotion_library, 'NH', 2)
    dict['neijiu2'] = neijiu2

    # 思(PF)
    si0 = get_emotion_words_from_ddr(emotion_library, 'PF', 0)
    dict['si0'] = si0
    si1 = get_emotion_words_from_ddr(emotion_library, 'PF', 1)
    dict['si1'] = si1
    si2 = get_emotion_words_from_ddr(emotion_library, 'PF', 2)
    dict['si2'] = si2

    # 慌(NI)
    huang0 = get_emotion_words_from_ddr(emotion_library, 'NI', 0)
    dict['huang0'] = huang0
    huang1 = get_emotion_words_from_ddr(emotion_library, 'NI', 1)
    dict['huang1'] = huang1
    huang2 = get_emotion_words_from_ddr(emotion_library, 'NI', 2)
    dict['huang2'] = huang2

    # 恐惧(NC)
    kongju0 = get_emotion_words_from_ddr(emotion_library, 'NC', 0)
    dict['kongju0'] = kongju0
    kongju1 = get_emotion_words_from_ddr(emotion_library, 'NC', 1)
    dict['kongju1'] = kongju1
    kongju2 = get_emotion_words_from_ddr(emotion_library, 'NC', 2)
    dict['kongju2'] = kongju2

    # 羞(NG)
    xiu0 = get_emotion_words_from_ddr(emotion_library, 'NG', 0)
    dict['xiu0'] = xiu0
    xiu1 = get_emotion_words_from_ddr(emotion_library, 'NG', 1)
    dict['xiu1'] = xiu1
    xiu2 = get_emotion_words_from_ddr(emotion_library, 'NG', 2)
    dict['xiu2'] = xiu2

    # 烦闷(NE)
    fanmen0 = get_emotion_words_from_ddr(emotion_library, 'NE', 0)
    dict['fanmen0'] = fanmen0
    fanmen1 = get_emotion_words_from_ddr(emotion_library, 'NE', 1)
    dict['fanmen1'] = fanmen1
    fanmen2 = get_emotion_words_from_ddr(emotion_library, 'NE', 2)
    dict['fanmen2'] = fanmen2

    # 憎恶(ND)
    zenge0 = get_emotion_words_from_ddr(emotion_library, 'ND', 0)
    dict['zenge0'] = zenge0
    zenge1 = get_emotion_words_from_ddr(emotion_library, 'ND', 1)
    dict['zenge1'] = zenge1
    zenge2 = get_emotion_words_from_ddr(emotion_library, 'ND', 2)
    dict['zenge2'] = zenge2

    # 贬责(NN)
    bianze0 = get_emotion_words_from_ddr(emotion_library, 'NN', 0)
    dict['bianze0'] = bianze0
    bianze1 = get_emotion_words_from_ddr(emotion_library, 'NN', 1)
    dict['bianze1'] = bianze1
    bianze2 = get_emotion_words_from_ddr(emotion_library, 'NN', 2)
    dict['bianze2'] = bianze2

    # 妒忌(NK)
    jidu0 = get_emotion_words_from_ddr(emotion_library, 'NK', 0)
    dict['jidu0'] = jidu0
    jidu1 = get_emotion_words_from_ddr(emotion_library, 'NK', 1)
    dict['jidu1'] = jidu1
    jidu2 = get_emotion_words_from_ddr(emotion_library, 'NK', 2)
    dict['jidu2'] = jidu2

    # 怀疑(NL)
    huaiyi0 = get_emotion_words_from_ddr(emotion_library, 'NL', 0)
    dict['huaiyi0'] = huaiyi0
    huaiyi1 = get_emotion_words_from_ddr(emotion_library, 'NL', 1)
    dict['huaiyi1'] = huaiyi1
    huaiyi2 = get_emotion_words_from_ddr(emotion_library, 'NL', 2)
    dict['huaiyi2'] = huaiyi2


    keys = dict.keys()

    del_file(os.path.abspath(os.path.join(os.getcwd(), "..")) + "/" + "Emotions_library" + "/")

    dict_to_remove = []

    for i in keys:
        dir = os.path.abspath(os.path.join(os.getcwd(), "..")) + "/" + "Emotions_library" + "/"
        if not os.path.exists(dir):
            os.makedirs(dir)
        if len(dict.get(i)) != 0:
            dict.get(i).to_csv(dir + i + ".csv",  sep='\n', index=False, header=False)
        else:
            dict_to_remove.append(i)
            logger.info("Can't find dictionary for:" + i)
    for i in dict_to_remove:
        dict.pop(i)
    logger.info("CSV saved for %s", dict.keys())
    return dict

def calculation_sim(dict, words):

    dics_list = dict.keys()
    combo_sim_directory = os.path.join(BASE_PATH, 'SIM')
    check_if_path_exist_or_mkdir(combo_sim_directory)
    results = pd.DataFrame()
    results['words'] = words
    words_without_value = []
    for dic in dics_list:
        logger.info("Start calculating sim with %s", dic)
        results[dic] = calculate_sim_with_dic_and_word(dic, words_without_value, words)
    results.set_index('words')
    results.to_csv(COMBO_FPR_SIM)
    pd.DataFrame(words_without_value).to_csv(WORD_WITHOUT_VALUE, sep='\n', index=False, header=False)

def calculate_sim_with_dic_and_word(dic_name, words_without_value, words):

    model, num_features, model_word_set = ddr.load_model(PATH_TO_MODEL)
    dictionary_directory = os.path.join(BASE_PATH, 'Emotions_library', dic_name + '.csv')
    vector_file_path = os.path.join(BASE_PATH, 'Vector')

    check_if_path_exist_or_mkdir(vector_file_path)
    dictionary_vector_path = os.path.join(vector_file_path, dic_name + '_dictionary_vector')
    document_vector_path = os.path.join(vector_file_path, dic_name + '_document_vector.tsv')
    document_loadings_out_path = os.path.join(vector_file_path, dic_name + '_document_loading.tsv')

    dic_terms = ddr.terms_from_csv(input_path=dictionary_directory, delimiter='\n')

    agg_dic_vecs = ddr.dic_vecs(dic_terms=dic_terms, model=model, num_features=num_features,
                                model_word_set=model_word_set)
    ddr.write_dic_vecs(dic_vecs=agg_dic_vecs, output_path=dictionary_vector_path)
    ddr.doc_vecs_from_csv(input_path=PATH_TP_CONTENT_AFTER_JIEBA, output_path=document_vector_path, model=model,
                          num_features=num_features, model_word_set=model_word_set, text_col=0, delimiter='\t',
                          header=False)
    return ddr.get_loadings(dic_name, agg_doc_vecs_path=document_vector_path, agg_dic_vecs_path=dictionary_vector_path,
                     out_path=document_loadings_out_path, words_without_value=words_without_value, words=words, num_features=num_features)

def get_all_ddr_words(data):
        return data['词语']


def get_words_not_in_ddr():

    df = pd.read_csv(PATH_TP_CONTENT_AFTER_JIEBA, header=0, index_col=False, low_memory=False)

    logger.info("Start getting emotion words from library...")
    emotion_library = load_ddr_from_csv(EMOTION_CSV_TO_LOAD)
    words_from_ddr = get_all_ddr_words(emotion_library)
    wors_not_in_ddr = []

    logger.info("Start checking words if in ddr library...")
    count = 0
    for word in df['jieba']:
        count = count + 1
        if word not in words_from_ddr.values:
            wors_not_in_ddr.append(word)
        from cleaner import update_progress
        update_progress(count / len(df['jieba']))
    logger.info("The total number of words after jieba is %s, %s words not in ddr", len(df['jieba']), len(wors_not_in_ddr))
    pd.DataFrame(wors_not_in_ddr).to_csv(WORD_NOT_IN_DDR, encoding='utf-8-sig', index=False)
    return wors_not_in_ddr

def sort_values(data, emotion_words_dict):

    all = []
    dict_keys = list(emotion_words_dict.keys())
    logger.info("Start sorting values...")

    for row_number in range(len(data)):
        sub_list = []
        sub_list.append(data.iloc[row_number, 1])
        values = []
        count = 0
        for number in data.iloc[row_number, 2:]:

            number = number.replace(']', '').replace('[', '')

            values.append((float(number), dict_keys[count]))

            count = count + 1
        values.sort(key= operator.itemgetter(0), reverse=True)
        sub_list.append(values)
        all.append(sub_list)
        update_progress(row_number / (len(data)-1))
    save_sorted_list_to_csv(all, 'sim', 'sorted_sim')

def save_sorted_list_to_csv(list, sub_dir, name):
    dir = os.path.abspath(os.path.join(os.getcwd(), "..", sub_dir))
    if not os.path.exists(dir):
        logger.info("Creating dir %s", dir)
        os.makedirs(dir)
    logger.info("Start saving dict to csv, the size is %s", len(list[1]))
    path = dir + name + ".csv"
    f = codecs.open(path, 'w', 'utf_8_sig')
    writer = csv.writer(f)
    count = 0
    for item in list:
        writer.writerow(item)
        count = count + 1
        update_progress(count / (len(list) - 1))
    logger.info("Finish saving files to csv, the size is %s", count)
    f.close()



def load_binary_to_model():

    model = Word2Vec.load(PATH_TO_BINARY)
    model.save(PATH_TO_MODEL_WEIBO)

