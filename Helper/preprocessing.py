import pandas as pd
import logging
import jieba
from config import path3, path4, path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def tweets_cleaning(csv_to_process):

    df = pd.read_csv(csv_to_process, header=0, index_col=False, low_memory=False)

    # 拆分年月日
    df['year'] =df['created_at'].map(lambda x: x.split(' ')[0].split('-')[0])
    df['month'] = df['created_at'].map(lambda x: x.split(' ')[0].split('-')[1])
    df['date'] = df['created_at'].map(lambda x: x.split(' ')[0].split('-')[2])

    def exist2(s1, s2, x):
        if s1 in x and s2 in x:
            return 1
        else:
            return 0

    def exist(s, x):
        if s in x:
            return 1
        else:
            return 0

    """
    Step 1: removing irrelevant tweets
    """

    ## 筛选出只有一个二级关键词的tweets

    # 一词关键词列表
    keyword_txt = open(path3, 'r', encoding='utf-8')
    keywords_list = list(keyword_txt)

    keywords = []
    for i in keywords_list:
        keywords.append(i.replace('\n', ''))

    # 把一次关键词的分布统计放入新表
    df_kw = pd.DataFrame()
    for i in keywords:
        df_kw[str(i)] = df['content'].map(lambda x: exist(i, x))

    df_kw['sum'] = df_kw.apply(lambda x: x.sum(), axis=1)
    df_kw['content'] = df['content']
    df_kw['month'] = df['month']
    df_kw['date'] = df['date']
    df_kw['_id'] = df['_id']
    df_kw['user_id'] = df['user_id']
    df_kw['like_num'] = df['like_num']
    df_kw['repost_num'] = df['repost_num']
    df_kw['repost_num'] = df['repost_num']
    df_kw['comment_num'] = df['comment_num']
    df_kw['city'] = df['city']
    df_kw['created_at'] = df['created_at']


    df_kw['emotion'] = df['emotion']
    df_kw['##'] = df['##']
    df_kw['at'] = df['at']
    df_kw['kuang'] = df['kuang']


    # 筛选出只有0或1个关键词的tweets，再去除掉含有一级关键词的tweet，剩下只含有0或1个二级关键词的tweet
    df_kw2 = df_kw[df_kw['sum'] <= 1]
    df_kw_2more = df_kw[df_kw['sum'] > 1]
    first_kw = ['冠状', 'Cov-19', '新冠', 'Coronavirus', '#nCoV', 'sars-cov-2', 'COVID-19', '2019-nCoV', '#2019nCoV',
                '武汉肺炎', 'pandemic', '疫情', '不明原因肺炎', '野味肺炎', '新型肺炎', '封城', '方舱医院', '新肺炎', '隔离', '战疫', '抗疫', '病毒战']
    df_firstkw = pd.DataFrame()
    for i in first_kw:
        df_firstkw = df_firstkw.append(df_kw2[df_kw2['content'].str.contains(str(i))])
        df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains(str(i))].index), inplace=True)

    """
    Step 2: excluding the tweets referring to the foreign epidemic situation
    """
    df_PopOut = pd.DataFrame()  # 用于储存弹出来的样本，以后加回来
    columns_2 = []

    with open(path4, 'r', encoding='utf-8') as f:
        reader = f.readlines()
        reader[0] = '双黄连,抢购'
        reader.pop(-1)
        reader.pop(-1)

        for i in reader:
            l = [i.split(",")[0], i.split(",")[1]]
            columns_2.append(l)

    # 去掉非包含’外国‘的二词关键词
    for i in columns_2:
        df_PopOut = df_PopOut.append(
            df_kw2[(df_kw2['content'].str.contains(i[0])) & (df_kw2['content'].str.contains(i[1]))])
        df_kw2.drop(
            index=(df_kw2.loc[(df_kw2['content'].str.contains(i[0])) & (df_kw2['content'].str.contains(i[1]))].index),
            inplace=True)

    for country in ['美国', '西班牙', '加拿大', '新加坡', '英国', '印度', '日本', '韩国', '德国', '法国', '意大利', '香港', '澳门', '台湾', '海外', '国外',
                    '外国']:
        df_kw2.drop(
            index=(df_kw2.loc[(df_kw2['content'].str.contains(str(country))) & (df_kw2['content'].str.contains('例')) & (
                df_kw2['city'].notnull()) & (df_kw2['sum'] == 0) & (~df_kw2['content'].str.contains('输入'))].index),
            inplace=True)

    df_kw2 = df_kw2.append(df_PopOut)

    """
    继续Step 1
    """
    ## 只含有0或1个二级关键词的tweets中可能包含有二词关键词 eg.'双黄连 抢购'

    # 二词关键词列表
    columns = []

    with open(path, 'r', encoding='utf-8') as f:
        reader = f.readlines()
        reader[0] = '双黄连,抢购'
        reader.pop(-1)
        reader.pop(-1)

        for i in reader:
            l = [i.split(",")[0], i.split(",")[1]]
            columns.append(l)

    ## 将含有二词关键词的已处理tweets单独分离出来

    df_PopOut2 = pd.DataFrame()
    for i in columns:
        df_PopOut2 = df_PopOut2.append(
            df_kw2[(df_kw2['content'].str.contains(i[0])) & (df_kw2['content'].str.contains(i[1]))])
        df_kw2.drop(
            index=(df_kw2.loc[(df_kw2['content'].str.contains(i[0])) & (df_kw2['content'].str.contains(i[1]))].index),
            inplace=True)

    ## 2020年1月20号以前的删除，仅适用于1月份
    """
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 20) & (df_kw2['content'].str.contains('钟南山'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 20) & (df_kw2['content'].str.contains('确诊'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 24) & (df_kw2['content'].str.contains('复工'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 20) & (df_kw2['content'].str.contains('防护服'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 18) & (df_kw2['content'].str.contains('协和医院'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 19) & (df_kw2['content'].str.contains('护目镜'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 19) & (df_kw2['content'].str.contains('潜伏期'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 19) & (df_kw2['content'].str.contains('高福'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 18) & (df_kw2['content'].str.contains('疾控中心'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 19) & (df_kw2['content'].str.contains('CDC'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 19) & (df_kw2['content'].str.contains('无症状'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(int(df_kw2['date']) <= 17) & (df_kw2['content'].str.contains('N95'))].index), inplace=True)
    """

    ## 删除不相关tweets，各月份通用

    df_kw2.drop(index=(
        df_kw2.loc[(df_kw2['content'].str.contains('宾馆' or '酒店')) & (df_kw2['content'].str.contains('高福'))].index),
                inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(df_kw2['content'].str.contains('双肺纹理走形' or '肺内未见实质性病灶')) & (
        df_kw2['content'].str.contains('核酸检测'))].index), inplace=True)
    df_kw2.drop(index=(
        df_kw2.loc[(df_kw2['content'].str.contains('武汉一家人在网上全额预定')) & (df_kw2['content'].str.contains('疾控中心'))].index),
                inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('NCDC' or 'ACDC' or 'CCDC')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('舒兰')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('绥芬河')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('ECOM')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('复学')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[(df_kw2['content'].str.contains('打' or '针' or '扎' or '接种')) & (
        df_kw2['content'].str.contains('疫苗'))].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('找家长 CFA双血统银虎斑')].index), inplace=True)

    df_kw2 = df_kw2.append(df_PopOut2)
    df_kw2 = df_kw2.append(df_kw_2more)
    df_kw2 = df_kw2.append(df_firstkw)

    """
    Step 3 and 4: excluding forwarded and non-domestic-released tweets
    """
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('我分享了')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['content'].str.contains('我发表了')].index), inplace=True)
    df_kw2.drop(index=(df_kw2.loc[df_kw2['city'].isnull()].index), inplace=True)
    df_kw_final = df_kw2[['content', '_id', 'user_id', 'like_num', 'repost_num', 'comment_num', 'city', 'created_at', 'emotion', "##", "at", 'kuang']]

    df_kw_final.to_csv('/Users/xingwenpeng/PycharmProjects/nlp/ProcessedTweets/processed.csv', encoding='utf-8-sig', index=False)

def jieba_cut(content):
    return None