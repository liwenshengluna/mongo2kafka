# -*- coding: utf-8 -*-

'''
Created on 2018年3月26日

@author: liws
@desciption: 微博数据入kafuka脚本  crontab定时执行即可
'''

import datetime
import json
import sys
import hashlib
import socket
import time

import pymongo
from kafka_util import Kafka_producer
from obs_util import OBS_Api
import redis
from settings import get_settings_environment

reload(sys)
sys.setdefaultencoding("utf8")
_env = get_settings_environment("pro")
mylogger_weibo = _env.mylogger_weibo


class Weixin2Kafuka(object):
    '''
    微信mongodb数据入kafuka
    '''

    def __init__(self):
        self._kafuka = Kafka_producer(_env.KAFUKA_HOST, _env.KAFUKA_PORT)
        self.mongo_config = _env.MONGO_CONFIG
        self.mongo = pymongo.MongoClient(host=self.mongo_config.get('host'), port=self.mongo_config.get('port'))[self.mongo_config.get('db')]
        self.mongo.authenticate(name=self.mongo_config.get('user'), password=self.mongo_config.get('passwd'))
        self.obs = OBS_Api()
        self._redis_1 = redis.Redis(host=_env.REDIS_CONFIG["host"], db=_env.REDIS_CONFIG["db"], port=_env.REDIS_CONFIG["port"], password=_env.REDIS_CONFIG["password"])
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

    def get_item(self, collection):
        '''
        从mongodb中获取微信数据
        :param collection: mongo集合名称
        :return: 生成器
        '''
        try:
            _collection = self.mongo.get_collection(name=collection)
            res_cursor = _collection.find({"flag": 0}, no_cursor_timeout=True)
            for i in res_cursor:
                yield i
        except Exception as e:
            mylogger_weibo.exception("error is %s", e)

    def update(self, _id, collection):
        '''
        更新已经进入kafuka的微信数据状态
        :param _id: 微信数据mongo id值
        :param collection: mongo集合名称
        :return: 更新成功 True  失败 False
        '''
        status = True
        _collection = self.mongo.get_collection(name=collection)
        try:
            _collection.update_one({"_id": _id}, {"$set": {"flag": 1}})
        except Exception as e:
            mylogger_weibo.exception("error is %s", e)
            status = False
        finally:
            return status

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
            mylogger_weibo.exception("error is %s", e)
            status = False
        finally:
            return status

    def save_image(self, img_list):
        retv_list = []
        for idx, url in enumerate(img_list):
            try:
                if not url:
                    continue
                temp_dict = {}
                temp_dict["old_src"] = url
                temp_dict["new_src"] = url
                temp_dict["status"] = 2
                temp_dict["ranking_num"] = idx + 1
                retv_list.append(temp_dict)
            except Exception as e:
                mylogger_weibo.exception("error is %s", e)
        return retv_list

    def save_video(self, video_list):
        retv_list = []
        for idx, item in enumerate(video_list):
            try:
                if not item:
                    continue
                temp_dict = {}
                temp_dict["ranking_num"] = idx + 1
                temp_dict["div_id"] = "uec_embedded_video_%s" % idx
                temp_dict["old_video_src"] = item.get("media_info", {}).get("stream_url")
                temp_dict["cover_pic"] = item.get("page_pic", {}).get("url")
                retv_list.append(temp_dict)
            except Exception as e:
                mylogger_weibo.exception("error is %s", e)
        return retv_list

    def process_image(self, image_list, item, webpage_code, status):
        retv_dict = {}
        if not image_list:
            return
        retv_dict["_id"] = str(item["_id"])
        retv_dict["parent_id"] = ""
        retv_dict["webpage_code"] = webpage_code
        retv_dict["cache_server"] = socket.gethostname()
        retv_dict["crawl_datetime"] = item["crawl_datetime"]
        retv_dict["status"] = status
        retv_dict["media_type"] = 0
        retv_dict["div_id"] = "uec_img_smsg"
        temp_images_list = self.save_image(image_list)
        if temp_images_list:
            retv_dict["images"] = temp_images_list
        temp_status = True
        for key in _env.post_image_must_key_list:
            if not retv_dict[key]:
                temp_status = False
                mylogger_weibo.info("news  image detail must key error %s id is %s", key, str(item["_id"]))
                break
        if temp_status:
            if not self.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_IMAGE"], json.dumps(retv_dict)):
                mylogger_weibo.info("send kafuka fail -----> id is %s", str(item["_id"]))

    def process_video(self, video_list, item):
        retv_dict = {}
        if not video_list:
            return
        retv_dict["_id"] = str(item["_id"])
        retv_dict["parent_id"] = ""
        retv_dict["webpage_code"] = item["webpage_code"]
        retv_dict["webpage_url"] = item["webpage_url"]
        retv_dict["cache_server"] = socket.gethostname()
        retv_dict["crawl_datetime"] = item["crawl_datetime"]
        retv_dict["status"] = 2
        retv_dict["media_type"] = 1
        temp_video_list = self.save_video(video_list)
        if temp_video_list:
            retv_dict["videos"] = temp_video_list
        if not self.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_VIDEO"], json.dumps(retv_dict)):
            mylogger_weibo.info("send kafuka fail -----> id is %s", str(item["_id"]))
        pass

    def change(self, item):
        retv_item = {}
        retv_item["content"] = item.get("content", "")
        img_group_list = item.get("image_lists", [])
        if len(img_group_list) > 0:
            retv_item["content"] = retv_item["content"] + "<div id=uec_img_smsg></div>"
        retv_item["meta_info_key"] = item.get("meta_info_key", "")
        retv_item["meta_text"] = self._redis_1.get(retv_item["meta_info_key"]) if self._redis_1.get(retv_item["meta_info_key"]) else ""
        retv_item["webpage_url"] = item.get("webpage_url", "")
        #r_w = self._redis_14.hsetnx(_env.REDIS_DISTINCT_KEY, retv_item["webpage_url"], time.strftime("%Y-%m-%d %H:%M:%S"))
        #if r_w == 0:
        #    return None
        if len(img_group_list) > 2:
            retv_item["image_status"] = 2
        else:
            retv_item["image_status"] = 0
        retv_item["no_tag_content"] = item.get("no_tag_content", "")
        if len(item.get("no_tag_content", "")) > 40:
            retv_item["title"] = item.get("no_tag_content", "")[0:40]
        else:
            retv_item["title"] = item.get("no_tag_content", "")
        retv_item["webpage_code"] = item["webpage_code"]
        new_image_list = []
        for one_item in img_group_list:
            img_url = one_item.get("large", {}).get("url", "")
            if img_url:
                new_image_list.append(img_url)
        self.process_image(new_image_list, item, retv_item["webpage_code"], retv_item["image_status"])
        video_list = []
        if item.get("page_info", {}).get("type", "") == "video":
            video_list.append(item["page_info"])
        self.process_video(video_list, item)
        if video_list:
            retv_item["content"] = retv_item["content"] + "<div id=uec_embedded_video_0></div>"
        retv_item["release_datetime"] = item.get("release_datetime", "")
        retv_item["wechat_name"] = item.get("wechat_name", "")
        retv_item["original_id"] = item.get("original_id", "")
        retv_item["original_parent_id"] = item.get("original_parent_id", "")
        retv_item["original_relation_id"] = item.get("original_relation_id", "")
        # retv_item["crawl_datetime"] = item.get("crawl_datetime", "")
        retv_item["crawl_datetime"] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        retv_item["source_report"] = item.get("source_report", "")
        retv_item["region"] = item.get("region", "")
        retv_item["reposts_num"] = int(item["reposts_num"]) if item["reposts_num"] else 0
        retv_item["comments_num"] = int(item["comments_num"]) if item["comments_num"] else 0
        retv_item["clicking_num"] = int(item["clicking_num"]) if item["clicking_num"] else 0
        retv_item["participate_num"] = int(item["participate_num"]) if item["participate_num"] else 0
        retv_item["vote"] = int(item["vote"]) if item["vote"] else 0
        retv_item["against"] = int(item["against"]) if item["against"] else 0
        retv_item["browse_num"] = int(item["browse_num"]) if item["browse_num"] else 0
        retv_item["is_deleted"] = 0
        if video_list:
            retv_item["video_status"] = 2
        else:
            retv_item["video_status"] = 0
        retv_item["news_crawl_type"] = item.get("news_crawl_type", 4)
        return retv_item

if __name__ == '__main__':
    while True:
        w2k = Weixin2Kafuka()
        collection_name = "weibo_detail"
        res = w2k.get_item(collection_name)
        for i in res:
            if not i:
                mylogger_weibo.info("not new weibo")
                break
            else:
                try:
                    temp_status = True
                    item = w2k.change(i)
                    if not item:
                        mylogger_weibo.info("weibo info in redis id is %s", str(i["_id"]))
                        if not w2k.update(i["_id"], collection_name):
                            mylogger_weibo.info("update fail id is %s", str(i["_id"]))
                        continue
                    for key in _env.post_detail_must_key_list:
                        if not item[key]:
                            temp_status = False
                            mylogger_weibo.info("news detail must key error %s id is %s", key, str(i["_id"]))
                            break
                    if not temp_status:
                        if not w2k.update(i["_id"], collection_name):
                            mylogger_weibo.info("update fail id is %s", str(i["_id"]))
                        continue
                    try:
                        if not w2k.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_WEBPAGE"], json.dumps(item)):
                            mylogger_weibo.info("send kafuka fail -----> id is %s", str(i["_id"]))
                    except Exception as e:
                        mylogger_weibo.exception("error is %s id is %s", e, str(i["_id"]))
                    mylogger_weibo.info("send kafuka sucess webpage_code is %s", item["webpage_code"])
                    if not w2k.update(i["_id"], collection_name):
                        mylogger_weibo.info("update fail id is %s", str(i["_id"]))
                    time.sleep(0.05)
                except Exception as e:
                    mylogger_weibo.exception("error is %s", e)
        time.sleep(20)
