# coding=utf-8
import logging
import re
import sys

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def clean(dict):

    logger.info("Start cleaning..")

    update_dict = []

    max = len(dict)

    count =0

    while count <= max:
        line = dict.get(str(count))
        if line == None:
            break

        updated_line = clean_each_line(line)

        update_dict.append(updated_line)

        count = count +1


    logger.info("Finish cleaning the total is %s, the cleaned is %s", max, count)
    return update_dict



def clean_each_line(line):

    content = line.get("content")

    # hashtag替代
    hashtag = re.findall('#.*?#', content)
    content = re.sub('#.*?#', '', content)

    #【 替代
    kuang = re.findall('【.*?】', content)
    content = re.sub('【.*?】', '', content)

    # 表情[]替代、提取
    emotion = re.findall('\[.*?\]', content)
    content = re.sub('\[.*?\]', '', content)

    #@替代
    at = re.findall('\@.*? ', content)
    content = re.sub('\@.*? ', '', content)

    #去除�❤
    content = re.sub('�', '', content)
    content = re.sub('❤', '', content)

    #去除'http.*?显示地图$'
    content = re.sub('http.*?显示地图$', '', content)

    #去除地图
    reverted = ''.join(reversed(content))
    reverted = re.sub('图地示显 .*? ', '', reverted)
    content = ''.join(reversed(reverted))

    #移除无法编码的表情
    content = str(bytes(content, encoding='utf-8').decode('utf-8').encode('gbk', 'ignore').decode('gbk'))

    line['content'] = content
    line['emotion'] = emotion
    line['##'] = hashtag
    line['at'] = at
    line['kuang'] = kuang


    return line

def clean_again_each_line(line):
    chinese_words = re.findall(r'[\u4e00-\u9fff]+', line)
    return ' '.join(i for i in chinese_words)


def clean_again(path_for_csv):

    logger.info("Start cleaning again...")
    df = pd.read_csv(path_for_csv, header=0, index_col=False, low_memory=False)
    count = 0
    cleaned_content = []
    for row in df['content']:
        cleaned_content.append(clean_again_each_line(str(row)))
        count = count + 1
        update_progress(count/len(df['content']))
    df['content'] = cleaned_content
    logger.info("Saving cleaned files to csv...")
    df.to_csv('/Users/xingwenpeng/PycharmProjects/nlp/Files/cleaned_again.csv', encoding='utf-8-sig', index=False)



def update_progress(progress):
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    barLength = 10  # Modify this to change the length of the progress bar
    block = int(round(barLength * progress))
    text = "\rPercent: [{0}] {1}% {2}".format("#" * block + "-" * (barLength - block), progress * 100, status)
    sys.stdout.write(text)
    sys.stdout.flush()
