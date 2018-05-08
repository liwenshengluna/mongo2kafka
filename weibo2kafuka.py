# -*- coding: utf-8 -*-

'''
Created on 2018年3月26日

@author: liws
@desciption: 微博数据入kafuka脚本  crontab定时执行即可
'''

import traceback
import datetime
import json
import re
import sys
import hashlib
import socket
import os
import time
# from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool

from PIL import Image
import pymongo
from kafka_util import Kafka_producer
from obs_util import OBS_Api
import requests
import redis
from settings import get_settings_environment

_env = get_settings_environment("pro")
mylogger_weibo = _env.mylogger_weibo

def downloader_and_up_img(item):

    w = Weibo2Kafuka()
    retv_dict = {}
    split_list = item.split("||||")
    url = split_list[0]
    article_id = split_list[3]
    user_id = split_list[2]
    ranking_num = int(split_list[1])
    if not url:
        return retv_dict
    if not user_id:
        user_id = "other_temp"
    image_forth_dir = user_id
    img_fmt = ".jpg"
    if ".jpg" in url.lower():
        img_fmt = ".jpg"
    elif ".gif" in url.lower():
        img_fmt = ".gif"
    elif ".png" in url.lower():
        img_fmt = ".png"
    imagename = w.hash_util(url) + img_fmt
    res = requests.get(url, verify=False)
    up_path = "image/" + str(datetime.datetime.today().date()).replace("-", "/") + "/" + user_id + "/" + imagename
    full_path = _env.SAVEDIRPATH + "/" + imagename
    with open(full_path, 'wb') as f:
        f.write(res.content)
    w.obs.uploadobject(up_path, full_path)
    size, width, height, image_form = w.get_img_info(full_path)
    if not width or not height:
        mylogger_weibo.info("img error id is %s url is %s", article_id, url)
        return retv_dict
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
    retv_dict["old_src"] = url
    retv_dict["status"] = 0 if res.status_code == 200 else 1
    retv_dict["new_src"] = new_src
    retv_dict["ranking_num"] = ranking_num
    retv_dict["size"] = size
    retv_dict["width"] = width
    retv_dict["height"] = height
    retv_dict["image_form"] = image_form
    return retv_dict

class Weibo2Kafuka(object):
    '''
    微博mongodb数据入kafuka
    '''

    def __init__(self, mongo_config=None):
        self._kafuka = Kafka_producer(_env.KAFUKA_HOST, _env.KAFUKA_PORT)
        self.mongo_config = _env.MONGO_CONFIG
        self.mongo = pymongo.MongoClient(host=self.mongo_config.get('host'), port=self.mongo_config.get('port'))[self.mongo_config.get('db')]
        self.mongo.authenticate(name=self.mongo_config.get('user'), password=self.mongo_config.get('passwd'))
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
            mylogger_weibo.exception("error is %s", e)
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
        从mongodb中获取微博数据
        :param collection: mongo集合名称
        :return: 生成器
        '''
        try:
            _collection = self.mongo.get_collection(name=collection, )
            res_cursor = _collection.find({"flag": {"$ne": 1}}, no_cursor_timeout=True)
            for i in res_cursor:
                yield i
        except Exception as e:
            mylogger_weibo.exception("error is %s", e)

    def update(self, _id, collection):
        '''
        更新已经进入kafuka的微博数据状态
        :param _id: 微博数据mongo id值
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
            mylogger_weibo.exception("error is %s", e)
        finally:
            return size, width, height, image_form

    def save_image(self, img_list, user_id, article_id):
        '''
        保存微博信息中的图片  并且上传到华为云服务器上
        :param img_list: 要下载的图片列表
        :param user_id:  微博的用户id
        :return:
        '''
        retv_list = []
        task_list = []
        if not user_id:
            user_id = "other_temp"
        if not os.path.exists(_env.SAVEDIRPATH):
            os.makedirs(_env.SAVEDIRPATH)
        for idx, url in enumerate(img_list):
            temp_number = idx + 1
            temp_str = url + "||||" + str(temp_number) + "||||" + user_id + "||||" + article_id
            task_list.append(temp_str.strip())
        # pool = Pool(len(task_list))
        pool = Pool(6)
        for i in range(len(task_list)):
            res=pool.apply_async(downloader_and_up_img, args=(task_list[i],))
            retv_list.append(res.get())
        pool.close()
        pool.join()
        return retv_list

    def downloader_and_up_img(self, item):
        retv_dict = {}
        split_list = item.split("||||")
        url = split_list[0]
        article_id = split_list[3]
        user_id = split_list[2]
        ranking_num = int(split_list[1])
        if not url:
            return retv_dict
        if not user_id:
            user_id = "other_temp"
        image_forth_dir = user_id
        img_fmt = ".jpg"
        if ".jpg" in url.lower():
            img_fmt = ".jpg"
        elif ".gif" in url.lower():
            img_fmt = ".gif"
        elif ".png" in url.lower():
            img_fmt = ".png"
        imagename = self.hash_util(url) + img_fmt
        res = requests.get(url, verify=False)
        up_path = "image/" + str(datetime.datetime.today().date()).replace("-", "/") + "/" + user_id + "/" + imagename
        full_path = _env.SAVEDIRPATH + "/" + imagename
        with open(full_path, 'wb') as f:
            f.write(res.content)
        self.obs.uploadobject(up_path, full_path)
        size, width, height, image_form = self.get_img_info(full_path)
        if not width or not height:
            mylogger_weibo.info("img error id is %s url is %s", article_id, url)
            return retv_dict
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
        retv_dict["old_src"] = url
        retv_dict["status"] = 0 if res.status_code == 200 else 1
        retv_dict["new_src"] = new_src
        retv_dict["ranking_num"] = ranking_num
        retv_dict["size"] = size
        retv_dict["width"] = width
        retv_dict["height"] = height
        retv_dict["image_form"] = image_form
        return retv_dict

    def fomat_time_partial(self, content, **kwargs):
        '''
        微博数据发布时间处理
        :param content: 微博发布时间
        :param kwargs:
        :return: 格式为 %Y-%m-%d %H:%M:%S 的时间字符串
        '''
        pub_time = ''
        try:
            if content.find("小时前") >= 0:
                t_time = re.findall("(?isu)([0-9]{1,})", content)
                if len(t_time) != 0:
                    tt_time = int(t_time[0])
                    now_t = datetime.datetime.now()
                    real_time = now_t + datetime.timedelta(hours=-tt_time)
                    pub_time = real_time.strftime("%Y-%m-%d %H:%M:%S")
            elif content.find("分钟前") >= 0:
                t_time = re.findall("(?isu)([0-9]{1,})", content)
                if len(t_time) != 0:
                    tt_time = int(t_time[0])
                    now_t = datetime.datetime.now()
                    real_time = now_t + datetime.timedelta(minutes=-tt_time)
                    pub_time = real_time.strftime("%Y-%m-%d %H:%M:%S")
            elif content.find("今天") >= 0:
                pub_time = "{0} {1}{2}".format(time.strftime("%Y-%m-%d"), content.replace("今天", "").replace(" ", ""),
                                               ":00")
            elif content.find("昨天") >= 0:
                now_t = datetime.datetime.now()
                real_time = now_t + datetime.timedelta(days=-1)
                pub_time = "{0} {1}{2}".format(real_time.strftime("%Y-%m-%d"),
                                               content.replace("昨天", "").replace(" ", ""), ":00")
            elif content.find("月") >= 0 and content.find("日") >= 0:
                pub_time = "{0}-{1}{2}".format(time.strftime("%Y"), content.replace("月", "-").replace("日", ""), ":00")
            else:
                if ":" not in content:
                    pub_time = content + "00:00:00"
                else:
                    if  content.count(":") > 1:
                        pub_time = content
                    else:
                        pub_time = content + ":00"
                # pub_time = "{0}-{1} {2}".format(time.strftime("%Y"), content, "00:00:00")
        except Exception as e:
            pass
        if pub_time == '':
            pub_time = time.strftime("%Y-%m-%d %H:%M:%S")
        return pub_time

    def process_image(self, image_list, item, webpage_code, status):
        '''
        微博图片处理流程
        :param image_list: 微博图片列表
        :param item: 微博原始数据
        :param webpage_code: webpage_url的md5值
        :param status: 微博信息图片状态 组图2(图片数量大于等于2)    单图1   其他0
        :return:
        '''
        retv_dict = {}
        if not image_list:
            return
        retv_dict["_id"] = str(item["_id"])
        retv_dict["parent_id"] = ""
        retv_dict["webpage_code"] = webpage_code
        retv_dict["cache_server"] = socket.gethostname()
        retv_dict["crawl_datetime"] = item["spiderTime"]
        retv_dict["status"] = status
        retv_dict["media_type"] = 0
        user_id = item["def17"]
        retv_dict["div_id"] = "uec_img_smsg"
        temp_images_list = self.save_image(image_list, user_id, str(item["_id"]))
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

    def get_title(self, content):
        '''
        获取微博标题
        原则： 1  如果内容中存在一组[]包含的内容  则就选[]中的内容为标题
              2  如果不满足1的条件 则去掉微博内容中 '转发' 字符串 截取内容中的前40个字符做标题
        :param content: 微博内容
        :return: 微博标题
        '''
        retv_title = ""
        try:
            find_title = re.findall("\[.*?\]", content)
            if len(find_title) > 0:
                temp_title = find_title[0]
                return temp_title
            content = content.replace("转发", "")
            if content:
                retv_title = content[:40]
        except Exception as e:
            mylogger_weibo.exception("error is %s", e)
        finally:
            return retv_title

    def change(self, item):
        retv_item = {}
        retv_item["content"] = item.get("def0")
        temp_title = self.get_title(retv_item["content"])
        retv_item["title"] = temp_title if temp_title else item["def4"]
        img_group = item.get("def7")
        if img_group:
            img_group_list = img_group.split(";")
        else:
            img_group_list = []
        if len(img_group_list) >= 2:
            #retv_item["content"] = retv_item["content"] + "<div id='uec_img_smsg'></div>"
            pass
        user_id = item.get("def17")
        retv_item["meta_info_key"] = user_id
        retv_item["meta_text"] = self._redis_1.get(user_id) if self._redis_1.get(user_id) else ""
        retv_item["webpage_url"] = "http://m.weibo.cn/" + user_id + "/" + item.get("def8")
        r_w = self._redis_14.hsetnx(_env.REDIS_DISTINCT_KEY, retv_item["webpage_url"], time.strftime("%Y-%m-%d %H:%M:%S"))
        if r_w == 0:
            return None
        if len(img_group_list) > 2:
            retv_item["image_status"] = 2
        else:
            retv_item["image_status"] = 0
        retv_item["no_tag_content"] = re.sub("<.*?>", "", retv_item["content"])
        retv_item["webpage_code"] = self.hash_util(retv_item["webpage_url"])
        # self.process_image(img_group_list, i, retv_item["webpage_code"], retv_item["image_status"])
        retv_item["release_datetime"] = self.fomat_time_partial(item["def5"].strip())
        retv_item["source_crawl"] = "微博"
        retv_item["source_report"] = item.get("def4", "")
        retv_item["wechat_name"] = item.get("def4", "")
        retv_item["news_crawl_type"] = 4
        retv_item["crawl_datetime"] = item["spiderTime"]
        retv_item["region"] = ""
        retv_item["reposts_num"] = int(item["def1"]) if item["def1"] else 0
        retv_item["comments_num"] = int(item["def2"]) if item["def2"] else 0
        retv_item["clicking_num"] = 0
        retv_item["participate_num"] = 0
        retv_item["vote"] = int(item["def3"]) if item["def3"] else 0
        retv_item["against"] = 0
        retv_item["browse_num"] = 0
        retv_item["is_deleted"] = 0
        retv_item["video_status"] = 0
        return retv_item


if __name__ == '__main__':
    while True:
        w2k = Weibo2Kafuka()
        # print(mongo_db.fomat_time_partial("昨天 11:53"))
        today_str = str(datetime.datetime.today().date()).replace("-", "_")
        collection_name = "weiboCreateByShark_weiboSpider_weibo_shark_%s" % today_str
        collection_name = "weiboCreateByShark_weiboSpider_shark_2018_04_22"
        res = w2k.get_item(collection_name)
        for i in res:
            temp_status = True
            item = w2k.change(i)
            if not item:
                mylogger_weibo.info("weibo info in redis id is %s", str(i["_id"]))
                w2k.update(i["_id"], collection_name)
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
            mylogger_weibo.info("send kafuka sucess webpage_code is %s id is %s", item["webpage_code"], str(i["_id"]))
            if not w2k.update(i["_id"], collection_name):
                mylogger_weibo.info("update fail id is %s", str(i["_id"]))
            time.sleep(0.1)
        time.sleep(20)
