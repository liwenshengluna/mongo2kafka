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

from PIL import Image
import pymongo
from kafka_util import Kafka_producer
from obs_util import OBS_Api
import requests
import redis
from settings import get_settings_environment

_env = get_settings_environment("pro")
mylogger_weibo = _env.mylogger_weibo


class Weibo2Kafuka(object):
    '''
    微博mongodb数据入kafuka
    '''

    def __init__(self, mongo_config=None):
        self._kafuka = Kafka_producer(_env.KAFUKA_HOST, _env.KAFUKA_PORT)
        self.mongo_config = mongo_config
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
            _collection = self.mongo.get_collection(name=collection)
            res_cursor = _collection.find({"flag": {"$ne": 1}})
            for i in res_cursor:
                yield i
                break
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
        im = Image.open(img_path)
        width, height = im.size
        image_form = im.format
        im.close()
        return size, width, height, image_form

    def save_image(self, img_list, user_id):
        '''
        保存微博信息中的图片  并且上传到华为云服务器上
        :param img_list: 要下载的图片列表
        :param user_id:  微博的用户id
        :return:
        '''
        retv_list = []
        if not os.path.exists(_env.SAVEDIRPATH):
            os.makedirs(_env.SAVEDIRPATH)
        for idx, url in enumerate(img_list):
            try:
                if not url:
                    continue
                temp_dict = {}
                image_forth_dir = user_id
                imagename = self.hash_util(url) + ".jpg"
                res = requests.get(url, verify=False)
                full_path = _env.SAVEDIRPATH + "/" + imagename
                with open(full_path, 'wb') as f:
                    f.write(res.content)
                up_path = "image/" + str(datetime.datetime.today().date()).replace("-", "/") + "/" + user_id + "/" + imagename
                self.obs.uploadobject(up_path, full_path)
                print(up_path)
                size, width, height, image_form = self.get_img_info(full_path)
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
                mylogger_weibo.exception("error is %s", e)
        return retv_list

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
                pub_time = "{0}-{1}{2}".format(time.strftime("%Y"), content.replace("月", "-").replace("日", " "), ":00")
            else:
                pub_time = "{0}-{1} {2}".format(time.strftime("%Y"), content, "00:00:00")
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
        retv_dict["_id"] = item["_id"]
        retv_dict["parent_id"] = ""
        retv_dict["webpage_code"] = webpage_code
        retv_dict["cache_server"] = socket.gethostname()
        retv_dict["crawl_datetime"] = item["spiderTime"]
        retv_dict["status"] = status
        retv_dict["media_type"] = 0
        user_id = item["def17"]
        retv_dict["div_id"] = "uec_img_smsg"
        temp_images_list = self.save_image(image_list, user_id)
        if temp_images_list:
            retv_dict["images"] = temp_images_list
        temp_status = True
        for key in _env.post_image_must_key_list:
            if not retv_dict[key]:
                temp_status = False
                mylogger_weibo.info("news  image detail must key error %s id is %s", key, item["_id"])
                break
        if temp_status:
            if not self.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_IMAGE"], json.dumps(retv_dict)):
                mylogger_weibo.info("send kafuka fail -----> id is %s", item["_id"])

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
        '''
        微博mongo数据转化为制定格式的数据
        :param item: 微博mongo数据
        :return: 要推入kafaka的数据格式  如下
        {
    "parent_id" : "59c365a343dea50ee436657e",
    "webpage_url" : "http://k.sina.cn/article_1116846745_4291ba990400043xm.html",
    "title" : "课间跑操  跑出亮丽风景线",
    "keywords" : "风景线,学生,亮丽,学校,运动,媒体,资讯,看点,新浪",
    "content" : "<article class=\"art_box\">\n<!--标题_s-->\n<!--标题_e-->\n<!--关注_s-->\n<section>\n<figure class=\"weibo_info look_info\" data-key=\"ff371a363be931a4efe1db18fe74e511\" data-time=\"1511361881\" data-uid=\"1116846745\">\n<a href=\"http://k.sina.cn/media_1116846745.html\">\n</a>\n<figcaption class=\"weibo_detail\">\n<h2 class=\"weibo_user\">泗县发布</h2>\n<em class=\"look_logo\"></em>\n</figcaption>\n</figure>\n</section>\n<!--背景浮层-->\n<section class=\"fl_bg j_float_bg hide\"></section>\n<!--确认弹出框-->\n<!--关注_e-->\n<div id=\"wx_pic\" style=\"margin:0 auto;display:none;\">\n<img src=\"http://n.sinaimg.cn/default/2fb77759/20151125/320X320.png\"/>\n</div>\n<section class=\"art_pic_card art_content\" data-sudaclick=\"kandian_a\" data-sudatagname=\"a\">\n<p><font>#泗县大杨乡中心学校#为培养学生热爱体育、崇尚运动的健康观念和良好习惯，逐步形成强身健体、阳光运动的校园氛围，泗县大杨乡中心学校举行课间跑阳光运动，跑出精彩，跑出亮丽的风景线。\n      每天上午大课间，大杨乡中心学校的校园内就会响起节奏鲜明、动听欢快的音乐。此时，全校800多名师生以班级为单位组成多个方阵，踏着明快的节拍，排着整齐的队伍，喊着嘹亮的口号在操场上匀速跑动着。\n      据了解，跑操不仅可以提高血液的含氧量，促进血液循环，增强学生体质、锤炼学生意志，更有利于培养学生的组织纪律观念和团结协作精神。\n      大杨乡中心学校举行课间跑操运动，为全校师生提供一个锻炼体魄的平台，创造了积极健康、蓬勃向上的校园文化氛围。（特邀主持：  吕楠）</font></p><p><img alt=\"课间跑操  跑出亮丽风景线\" h=\"720\" src=\"http://n.sinaimg.cn/sinacn/20171122/6efa-fynwxum9638137.jpg\" w=\"960\" wh=\"1.33\"/></p><p><img alt=\"课间跑操  跑出亮丽风景线\" h=\"540\" src=\"http://n.sinaimg.cn/sinacn/20171122/070d-fynwxum9638186.jpg\" w=\"960\" wh=\"1.78\"/></p><p><img alt=\"课间跑操  跑出亮丽风景线\" h=\"540\" src=\"http://n.sinaimg.cn/sinacn/20171122/e818-fynwxum9638204.jpg\" w=\"960\" wh=\"1.78\"/></p> </section>\n<!-- 分享模块_s -->\n</article>",
    "no_tag_content" : "泗县发布\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n#泗县大杨乡中心学校#为培养学生热爱体育、崇尚运动的健康观念和良好习惯，逐步形成强身健体、阳光运动的校园氛围，泗县大杨乡中心学校举行课间跑阳光运动，跑出精彩，跑出亮丽的风景线。\n      每天上午大课间，大杨乡中心学校的校园内就会响起节奏鲜明、动听欢快的音乐。此时，全校800多名师生以班级为单位组成多个方阵，踏着明快的节拍，排着整齐的队伍，喊着嘹亮的口号在操场上匀速跑动着。\n      据了解，跑操不仅可以提高血液的含氧量，促进血液循环，增强学生体质、锤炼学生意志，更有利于培养学生的组织纪律观念和团结协作精神。\n      大杨乡中心学校举行课间跑操运动，为全校师生提供一个锻炼体魄的平台，创造了积极健康、蓬勃向上的校园文化氛围。（特邀主持：  吕楠）",
    "webpage_code" : "003d1ca03ba3248bed5fe1fa305405aa",
    "classification" : "新浪看点",
    "source_report" : "泗县发布",
    "release_datetime" : "2017-11-22 22:42:29",
    "source_crawl" : "新浪新闻",
    "crawl_datetime" : "2017-11-22 22:42:29",
    "region" : "北京",
    "reposts_num" : NumberInt(0),
    "comments_num" : NumberInt(0),
    "clicking_num" : NumberInt(0),
    "participate_num" : NumberInt(0),
    "vote" : NumberInt(0),
    "against" : NumberInt(0),
    "browse_num" : NumberInt(0),
    "is_deleted" : NumberInt(0),
    "image_status" : NumberInt(2),
    "video_status" : NumberInt(0)
}
        '''
        retv_item = {}
        retv_item["content"] = item.get("def0")
        temp_title = self.get_title(retv_item["content"])
        retv_item["title"] = temp_title if temp_title else item["def4"]
        content_add_tag ="<div id='uec_img_smsg'></div>"
        img_group = item.get("def7")
        if img_group:
            img_group_list = img_group.split(";")
        else:
            img_group_list = []
        if len(img_group_list) > 0:
            retv_item["content"] = item.get("def0") + content_add_tag
        user_id = item.get("def17")
        retv_item["meta_info_key"] = user_id
        retv_item["meta_text"] = self._redis_1.get(user_id) if self._redis_1.get(user_id) else ""
        retv_item["webpage_url"] = "http://m.weibo.com/" + user_id + "/" + item.get("def8")
        # r_w = self._redis_14.hsetnx(redis_distinct_key, retv_item["webpage_url"], time.strftime("%Y-%m-%d %H:%M:%S"))
        # if r_w == 0:
        #     return None
        if len(img_group_list) == 1:
            retv_item["image_status"] = 1
        if len(img_group_list) == 0:
            retv_item["image_status"] = 0
        if len(img_group_list) > 1:
            retv_item["image_status"] = 2
        retv_item["no_tag_content"] = re.sub("<.*?>", "", retv_item["content"])
        retv_item["webpage_code"] = self.hash_util(retv_item["webpage_url"])
        self.process_image(img_group_list, i, retv_item["webpage_code"], retv_item["image_status"])
        retv_item["release_datetime"] = self.fomat_time_partial(item["def5"])
        retv_item["source_crawl"] = "微博"
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
    w2k = Weibo2Kafuka()
    # print(mongo_db.fomat_time_partial("昨天 11:53"))
    today_str = str(datetime.datetime.today().date()).replace("-", "_")
    collection_name = "weiboCreateByShark_weiboSpider_weibo_shark_%s" % today_str
    collection_name = "weiboCreateByShark_weiboSpider_weibo_shark_2018_04_02"
    res = w2k.get_item(collection_name)
    if not res:
        mylogger_weibo.info("not new weibo")
        sys.exit()
    else:
        for i in res:
            temp_status = True
            item = w2k.change(i)
            if not item:
                mylogger_weibo.info("weibo info in redis id is %s", i["_id"])
                continue
            for key in _env.post_detail_must_key_list:
                if not item[key]:
                    temp_status = False
                    mylogger_weibo.info("news detail must key error %s id is %s", key, i["_id"])
                    break
            if not temp_status:
                continue
            if not w2k.send_kafuka(_env.KAFKA_TOPICS["default"]["KAFUKA_TOPIC_WEBPAGE"], json.dumps(item)):
                mylogger_weibo.info("send kafuka fail -----> id is %s", i["_id"])
            if not w2k.update(i["_id"], collection_name):
                mylogger_weibo.info("update fail id is %s", i["_id"])
