# -*- coding: utf-8 -*-

import cStringIO
import traceback
import datetime
import json
import re
import sys
reload(sys)
sys.setdefaultencoding("utf8")
import sys
import hashlib
import socket
import os
import time
import csv


from PIL import Image
from kafka_util import Kafka_producer
from obs_util import OBS_Api
import requests
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import redis
from settings import get_settings_environment

from get_tld import parse_domain

_env = get_settings_environment("pro")
mylogger_news = _env.mylogger_news


class News2Kafuka(object):
    '''
    网站新闻mongodb数据入kafuka
    '''

    def __init__(self):
        self._kafuka = Kafka_producer(_env.KAFUKA_HOST, _env.KAFUKA_PORT)
        self.obs = OBS_Api()
        self._redis_14 = redis.Redis(host=_env.REDIS_CONFIG["host"], db=_env.REDIS_CONFIG["db_distinct"], port=_env.REDIS_CONFIG["port"], password=_env.REDIS_CONFIG["password"])

    def hash_util(self, content, hash_type="md5"):
        '''
        字符串哈希
        :param content: 要哈希的字符串
        :param hash_type: 哈希的类型 默认是md5
        :return: 哈希以后的结果
        '''
        if hash_type.lower() == "sha1":
            sha1 = hashlib.sha1()
            sha1.update(content)
            return sha1.hexdigest()
        elif hash_type.lower() == "md5":
            md5 = hashlib.md5()
            md5.update(content if isinstance(content, bytes) else content.encode('utf-8'))
            return md5.hexdigest()

    def send_kafuka(self, custom_topic_name, item):
        '''
        将数据发送到kafuka
        :param custom_topic_name: topic_name
        :param item: 数据 (json形式)
        :return: 发送成功 True 失败  False
        '''
        status = True
        try:
            self._kafuka.sendjsondata(custom_topic_name, item)
        except Exception as e:
            mylogger_news.exception("error is %s", e)
            status = False
        finally:
            return status

    def get_item(self, file_name):
        maxInt = sys.maxsize
        csv.field_size_limit(maxInt)
        retv_list = []
        try:
            with open(_env.READ_FILE_DIR + file_name, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) != 11:
                        continue
                    retv_dict = {}
                    title = ""
                    author = ""
                    editor = ""
                    source_report = ""
                    release_datetime = ""
                    no_tag_content = ""
                    content = ""
                    webpage_url = ""
                    title = row[0]
                    author = row[1]
                    editor = row[2]
                    source_report = row[3]
                    release_datetime = row[4]
                    no_tag_content = row[5]
                    content = row[5]
                    #content = row[6]
                    webpage_url = row[7]
                    retv_dict["crawl_datetime"] = row[9]
                    retv_dict["title"] = re.sub("\ufeff", "", title.strip())
                    retv_dict["author"] = re.sub("\ufeff", "", author.strip())
                    retv_dict["editor"] = re.sub("\ufeff", "", editor.strip())
                    retv_dict["source_report"] = re.sub("\ufeff", "", source_report.strip())
                    retv_dict["release_datetime"] = re.sub("\ufeff", "", release_datetime.strip())
                    if not retv_dict["release_datetime"] or retv_dict["release_datetime"] == 'null':
                        retv_dict["release_datetime"] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                    retv_dict["no_tag_content"] = re.sub("\ufeff", "", no_tag_content.strip())
                    retv_dict["content"] = re.sub("\ufeff", "", content.strip())
                    retv_dict["content"] = re.sub("\n", "</br>", retv_dict["content"])
                    retv_dict["webpage_url"] = re.sub("\ufeff", "", webpage_url.strip())
                    retv_dict["meta_info_key"] = parse_domain(retv_dict["webpage_url"], 2)
                    retv_dict["wechat_name"] = row[10]
                    r_w = self._redis_14.hsetnx(_env.REDIS_DISTINCT_KEY, retv_dict["webpage_url"], time.strftime("%Y-%m-%d %H:%M:%S"))
                    if r_w == 0:
                        continue
                    retv_dict["image_status"] = 0
                    retv_dict["webpage_code"] = self.hash_util(retv_dict["webpage_url"])
                    retv_dict["reposts_num"] = 0
                    retv_dict["comments_num"] = 0
                    retv_dict["clicking_num"] = 0
                    retv_dict["participate_num"] = 0
                    retv_dict["vote"] = 0
                    retv_dict["against"] = 0
                    retv_dict["browse_num"] = 0
                    retv_dict["is_deleted"] = 0
                    retv_dict["video_status"] = 0
                    retv_dict["news_crawl_type"] = 0
                    for k, v in retv_dict.items():
                        if type(v) == str and v.strip() == "null":
                            retv_dict[k] = ""
                    retv_list.append(retv_dict)
        except Exception as e:
            mylogger_news.exception("error is %s", e)
        finally:
            return retv_list


if __name__ == '__main__':
    w2k = News2Kafuka()
    while True:
        for i in os.listdir(_env.READ_FILE_DIR):
            res = w2k.get_item(i)
            for item in res:
                if not item:
                    mylogger_news.info("not new news")
                    break
                else:
                    try:
                        temp_status = True
                        for key in _env.post_detail_must_key_list:
                            if not item[key]:
                                temp_status = False
                                mylogger_news.info("news detail must key %s error file name is %s webpage_code is %s", key, i, item["webpage_code"])
                                break
                        if not temp_status:
                            continue
                        try:
                            if not w2k.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_WEBPAGE"], json.dumps(item)):
                                mylogger_news.info("send kafuka fail -----> file name is %s", i)
                        except Exception as e:
                            mylogger_news.exception("error is %s", e)
                        mylogger_news.info("send kafuka sucess webpage_code is %s", item["webpage_code"])
                    except Exception as e:
                        mylogger_news.exception("error is %s", e)
            os.system("mv %s %s " % (_env.READ_FILE_DIR + i, _env.BACKUP_FILE_DIR))
        time.sleep(20)
