# -*- coding: utf-8 -*-

'''
Created on 2018年3月26日

@author: liws
@desciption: 微信数据入kafuka脚本  crontab定时执行即可
'''

import datetime
import json
import re
import sys
import hashlib
import socket
import os
import time


from PIL import Image
import pymongo
from kafka_util import Kafka_producer
from obs_util import OBS_Api
import requests
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import redis
from settings import get_settings_environment

_env = get_settings_environment("pro")
mylogger_weixin = _env.mylogger_weixin


class Weixin2Kafuka(object):
    '''
    微信mongodb数据入kafuka
    '''

    def __init__(self):
        self._kafuka = Kafka_producer(_env.KAFUKA_HOST, _env.KAFUKA_PORT)
        self.mongo_config = _env.MONGO_CONFIG
        self.mongo = pymongo.MongoClient(host=self.mongo_config.get('host_weixin'), port=self.mongo_config.get('port'))[self.mongo_config.get('db_weixin')]
        self.mongo.authenticate(name=self.mongo_config.get('user_weixin'), password=self.mongo_config.get('passwd_weixin'))
        self.obs = OBS_Api()
        self._redis_1 = redis.Redis(host=_env.REDIS_CONFIG["host"], db=_env.REDIS_CONFIG["db"], port=_env.REDIS_CONFIG["port"], password=_env.REDIS_CONFIG["password"])
        self._redis_14 = redis.Redis(host=_env.REDIS_CONFIG["host"], db=_env.REDIS_CONFIG["db_distinct"], port=_env.REDIS_CONFIG["port"], password=_env.REDIS_CONFIG["password"])

    def do_insert(self, collection, item):
        status = True
        try:
            collection = self.mongo.get_collection(name=collection)
            collection.save(item)
            if isinstance(item, dict):
                collection.save(item)
        except Exception as e:
            status = False
        finally:
            return status

    def is_have(self, collection, url_md5):
        status = False
        try:
            collection = self.mongo.get_collection(name=collection)
            res = collection.find_one({"_id": url_md5})
            if res:
                status = True
        except Exception as e:
            mylogger_weixin.exception("error is %s", e)
        finally:
            return status

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
        res_cursor = ""
        try:
            _collection = self.mongo.get_collection(name=collection)
            res_cursor = _collection.find({"flag": 0}, no_cursor_timeout=True)
            for i in res_cursor:
                yield i 
        except Exception as e:
            mylogger_weixin.exception("error is %s", e)
        # return res_cursor

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
            mylogger_weixin.exception("error is %s", e)
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
            mylogger_weixin.exception("error is %s", e)
            status = False
        finally:
            return status

    def get_img_info(self, img_path):
        '''
        获取图片基本信息
        :param img_path: 图片的绝对路径
        :return: size, width, height, image_form
        '''
        size = os.path.getsize(img_path) / 1024
        width, height, image_form, = "", "", "JPG"
        try:
            im = Image.open(img_path)
            width, height = im.size
            image_form = im.format
            im.close()
        except Exception as e:
            mylogger_weixin.exception("error is %s", e)
        finally:
            return size, width, height, image_form

    def save_image(self, img_list, user_id, article_id):
        '''
        保存微信信息中的图片  并且上传到华为云服务器上
        :param img_list: 要下载的图片列表
        :param user_id:  微信公众号唯一 id (biz)
        :return:
        '''
        if not user_id:
            user_id = "other_temp"
        retv_list = []
        if not os.path.exists(_env.SAVEDIRPATH_WEIXIN):
            os.makedirs(_env.SAVEDIRPATH_WEIXIN)
        for idx, url in enumerate(img_list):
            try:
                if not url:
                    continue
                temp_dict = {}
                image_forth_dir = user_id
                img_fmt = "." + re.search('wx_fmt=(\w+)', url).group(1) if re.search('wx_fmt=(\w+)', url) else ".jpg"
                if img_fmt.lower() not in [".jepg", ".jpg", ".gif", ".png"]:
                    img_fmt = ".jpg"
                if len(img_fmt) > 5:
                    if "jpeg" in img_fmt.lower():
                        img_fmt = ".jepg"
                    elif "gif" in img_fmt.lower():
                        img_fmt = ".igif"
                    elif "png" in img_fmt.lower():
                        img_fmt = ".png"
                    else:
                        img_fmt = ".jpg"
                imagename = self.hash_util(url) + img_fmt
                res = requests.get(url, verify=False)
                up_path = "image/" + str(datetime.datetime.today().date()).replace("-", "/") + "/" + user_id + "/" + imagename
                # self.obs.uploadobject(up_path, res.content)
                full_path = _env.SAVEDIRPATH_WEIXIN + "/" + imagename
                with open(full_path, 'wb') as f:
                    f.write(res.content)
                up_path = "image/" + str(datetime.datetime.today().date()).replace("-", "/") + "/" + user_id + "/" + imagename
                resp = self.obs.uploadobject(up_path, full_path)
                # print("common msg:status:{0},errorCode:{1},errorMessage:{2}".format(resp.status, resp.errorCode,resp.errorMessage))
                size, width, height, image_form = self.get_img_info(full_path)
                if not width or not height:
                    mylogger_weixin.info("img error id is %s url is %s", article_id, url)
                    continue
                # my_img_file = cStringIO.StringIO(res.content)
                # size, width, height, image_form = self.get_img_info(my_img_file)
                param_1 = _env.OBJECT_STORE["default"]["IMAGE_SERVER_PLACEHODER"]
                param_2 = _env.OBJECT_STORE["default"]["IMAGE_SECOND_DIR"]
                param_3 = time.strftime("%Y/%m/%d")
                param_4 = image_forth_dir
                param_5 = imagename
                new_src = "{0}/{1}/{2}/{3}/{4}".format(param_1,
                                                       param_2,
                                                       param_3,
                                                       param_4,
                                                       param_5)
                temp_dict["old_src"] = url
                temp_dict["status"] = 0 if res.status_code == 200 else 1
                temp_dict["new_src"] = new_src
                temp_dict["ranking_num"] = idx + 1
                temp_dict["size"] = size
                temp_dict["width"] = width
                temp_dict["height"] = height
                temp_dict["image_form"] = image_form
                retv_list.append(temp_dict)
            except Exception as e:
                mylogger_weixin.exception("error is %s", e)
        return retv_list

    def process_image(self, image_list, item, webpage_code, status):
        '''
        微信图片处理流程
        :param image_list: 微信图片列表
        :param item: 微信原始数据
        :param webpage_code: webpage_url的md5值
        :param status
        :return:
        '''
        retv_dict = {}
        if not image_list:
            return
        retv_dict["_id"] = str(item["_id"])
        retv_dict["parent_id"] = ""
        retv_dict["webpage_code"] = webpage_code
        retv_dict["cache_server"] = socket.gethostname()
        retv_dict["crawl_datetime"] = item["crawl_datetime"]
        retv_dict["status"] = 2
        retv_dict["media_type"] = 0
        user_id = item.get("biz", "")
        # retv_dict["div_id"] = "uec_img_smsg"
        temp_images_list = self.save_image(image_list, user_id, str(item["_id"]))
        if temp_images_list:
            retv_dict["images"] = temp_images_list
        temp_status = True
        for key in _env.post_image_must_key_list:
            if not retv_dict[key]:
                temp_status = False
                mylogger_weixin.info("news  image detail must key error %s id is %s", key, str(item["_id"]))
                break
        if temp_status:
            if not self.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_IMAGE"], json.dumps(retv_dict)):
                mylogger_weixin.info("send kafuka fail -----> id is %s", str(item["_id"]))


    def change(self, item):
        retv_item = {}
        retv_item["content"] = item.get("content", "").replace("data-src", "src").replace('class="hide"', "")
        retv_item["title"] = item.get("title", "")
        #content_add_tag ="<div id='uec_img_smsg'></div>"
        img_group_list = item.get("image_lists", [])
        #if len(img_group_list) > 0:
        #    retv_item["content"] = item.get("content") + content_add_tag
        retv_item["meta_info_key"] = item.get("biz", "")
        retv_item["meta_text"] = self._redis_1.get(retv_item["meta_info_key"]) if self._redis_1.get(retv_item["meta_info_key"]) else ""
        retv_item["webpage_url"] = item.get("webpage_url", "")
        r_w = self._redis_14.hsetnx(_env.REDIS_DISTINCT_KEY, retv_item["webpage_url"], time.strftime("%Y-%m-%d %H:%M:%S"))
        if r_w == 0:
            return None
        if len(img_group_list) > 0:
            retv_item["image_status"] = 1
        else:
            retv_item["image_status"] = 0
        retv_item["no_tag_content"] = item.get("no_tag_content", "")
        retv_item["webpage_code"] = self.hash_util(retv_item["webpage_url"])
        # self.process_image(img_group_list, i, retv_item["webpage_code"], len(img_group_list))
        retv_item["release_datetime"] = item.get("release_datetime", "")
        retv_item["source_crawl"] = item.get("source_crawl", "微信")
        retv_item["crawl_datetime"] = item.get("crawl_datetime", "")
        retv_item["wechat_name"] = item.get("wechat_name", "")
        retv_item["crawl_datetime"] = item.get("crawl_datetime", "")
        retv_item["source_report"] = item.get("source_report", "")
        retv_item["region"] = item.get("region", "")
        retv_item["reposts_num"] = int(item["reposts_num"]) if item["reposts_num"] else 0
        retv_item["comments_num"] = int(item["comments_num"]) if item["comments_num"] else 0
        retv_item["clicking_num"] = int(item["clicking_num"]) if item["clicking_num"] else 0
        retv_item["participate_num"] = int(item["participate_num"]) if item["participate_num"] else 0
        retv_item["vote"] = int(item["vote"]) if item["vote"] else 0
        retv_item["against"] = int(item["against"]) if item["against"] else 0
        retv_item["browse_num"] = int(item["browse_num"]) if item["browse_num"] else 0
        retv_item["is_deleted"] = int(item["is_deleted"]) if item["is_deleted"] else 0
        retv_item["video_status"] = int(item["video_status"]) if item["video_status"] else 0
        retv_item["news_crawl_type"] = 5
        return retv_item


if __name__ == '__main__':
    while True:
        w2k = Weixin2Kafuka()
        collection_name = "weixin_detail"
        res = w2k.get_item(collection_name)
        for i in res:
            if not i:
                mylogger_weixin.info("not new weixin")
                break
            else:
                try:
                    temp_status = True
                    item = w2k.change(i)
                    if not item:
                        mylogger_weixin.info("weixin info in redis id is %s", str(i["_id"]))
                        if not w2k.update(i["_id"], collection_name):
                            mylogger_weixin.info("update fail id is %s", str(i["_id"]))
                        continue
                    for key in _env.post_detail_must_key_list:
                        if not item[key]:
                            temp_status = False
                            mylogger_weixin.info("news detail must key error %s id is %s", key, str(i["_id"]))
                            break
                    if not temp_status:
                        if not w2k.update(i["_id"], collection_name):
                            mylogger_weixin.info("update fail id is %s", str(i["_id"]))
                        continue
                    try:
                        if not w2k.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_WEBPAGE"], json.dumps(item)):
                            mylogger_weixin.info("send kafuka fail -----> id is %s", str(i["_id"]))
                    except Exception as e:
                        mylogger_weixin.exception("error is %s id is %s", e, str(i["_id"]))
                    mylogger_weixin.info("send kafuka sucess webpage_code is %s", item["webpage_code"])
                    if not w2k.update(i["_id"], collection_name):
                        mylogger_weixin.info("update fail id is %s", str(i["_id"]))
                    time.sleep(0.1)
                except Exception as e:
                    mylogger_weixin.exception("error is %s", e)
        time.sleep(20)
