import logging
import re

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def clean(dict):

    logger.info("Start cleaning..")

    update_dict = []

    max = len(dict)

    count =0

    while count <= max:

        logger.info("Count: %s", count)

        line = dict.get(str(count))

        if line == None:
            break

        updated_line = clean_each_line(line)

        update_dict.append(updated_line)

        count = count +1


    logger.info("Finish cleaning the total is %s, the processed is %s", max, count)
    return update_dict



def clean_each_line(line):

    content = line.get("content")

    # hashtag替代
    hashtag = re.findall('#.*?#', content)
    content = re.sub('#.*?#', '', content)

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
    line['emation'] = emotion
    line['##'] = hashtag
    line['at'] = at


    return line

