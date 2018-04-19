# -*- coding: utf-8 -*-

import datetime
import logging
from logging.handlers import TimedRotatingFileHandler


class BaseConfig(object):
    '''
    基础环境配置
    '''

    log_format = "%(asctime)s %(levelname)-5s %(message)s"
    log_level = logging.INFO
    mylogger_weibo = logging.getLogger("m2k_weibo")
    mylogger_weixin = logging.getLogger("m2k_weixin")
    mylogger_news = logging.getLogger("m2k_news")
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(log_format))
    ch.setLevel(logging.INFO)
    mylogger_weibo.addHandler(ch)
    mylogger_weixin.addHandler(ch)
    mylogger_news.addHandler(ch)

    fh = TimedRotatingFileHandler("../log/m2k_weibo.log", 'midnight')
    fh.setFormatter(logging.Formatter(log_format))
    fh.setLevel(log_level)
    mylogger_weibo.addHandler(fh)

    fh = TimedRotatingFileHandler("../log/m2k_weixin.log", 'midnight')
    fh.setFormatter(logging.Formatter(log_format))
    fh.setLevel(log_level)
    mylogger_weixin.addHandler(fh)

    fh = TimedRotatingFileHandler("../log/m2k_news.log", 'midnight')
    fh.setFormatter(logging.Formatter(log_format))
    fh.setLevel(log_level)
    mylogger_news.addHandler(fh)

    OBS_ON = True #默认打开
    OBS_AK = "FNWTWJJRI5BWKVUPJ1C7"
    OBS_SK = "OZvynXG2kyqCHCx6Kb9mpmtYJI0VEHiqweCMOpZW"
    OBS_ISSECURE = False
    OBS_SERVER = "obs.cn-north-1.myhwclouds.com" #域名
    OBS_SIGNATURE = "v4" #认证版本
    OBS_REGION = "beijing" #区域
    OBS_MD5 = None #加密方式
    OBS_ACL = "public-read" #对象读取权限
    OBS_LOCATION = "http://inewsengine.com"
    OBS_BUCKETNAME = "obs-inews-01" #缺省桶名称

    MONGO_CONFIG = {
        "host": "114.115.148.216",
        "port": 27025,
        "db": "irobot_new",
        "user": "crawler",
        "passwd": "test",

        "host_weixin": "114.115.148.216",
        "user_weixin": "irobot",
        "passwd_weixin": "irobot",
        "db_weixin": "irobot"

    }

    REDIS_CONFIG = {
        "host": "10.50.1.200",
        "port": "6379",
        "db": "1",
        "db_distinct": "14",
        "password": None,
    }
    REDIS_DISTINCT_KEY = "webpage_urls"
    READ_FILE_DIR = "/data/news_file_data/ready_file/"
    BACKUP_FILE_DIR = "/data/news_file_data/backup_file/"

    NEW_DETAIL_MUST_FIELD = []

    KAFUKA_HOST = "10.50.1.197:19092,10.50.1.202:19092, 10.50.1.198:19092"
    KAFUKA_PORT = 9092

    SAVEDIRPATH = "/inews/crawler_lws/mongo2kafuka/save_image_%s" % str(datetime.datetime.today().date()).replace("-",
                                                                                                                  "_")
    SAVEDIRPATH_WEIXIN = "/inews/crawler_lws/mongo2kafuka/save_image_weixin_%s" % str(
        datetime.datetime.today().date()).replace("-", "_")
    OBS_ON = True  # 测试环境对象存储关闭
    OBJECT_STORE = {
        "default": {
            "IMAGE_SAVE_ROOT_DIR": "E:/imgs",
            "IMAGE_SERVER_PLACEHODER": "${inewsImageServer}",
            "IMAGE_SECOND_DIR": "image",
            "IMAGE_LIB_LIMIT_COUNT": 2,  # 图库图片数量限制，小于2张的不算入图库
            "IMAGE_WIDTH_PIXEL_LIMIT": 500,  # 图库图片宽度限制
            "IMAGE_HEIGHT_PIXEL_LIMIT": 500  # 图库图片高度限制
        },
        "oss": {
            "IMAGE_SAVE_ROOT_DIR": "E:/imgs",
            "IMAGE_SERVER_PLACEHODER": "https://imedia-peoplesdaily.oss-cn-beijing.aliyuncs.com",
            "IMAGE_SECOND_DIR": "image",
            "IMAGE_LIB_LIMIT_COUNT": 2,  # 图库图片数量限制，小于2张的不算入图库
            "IMAGE_WIDTH_PIXEL_LIMIT": 500,  # 图库图片宽度限制
            "IMAGE_HEIGHT_PIXEL_LIMIT": 500  # 图库图片高度限制
        },
    }

    KAFKA_TOPICS = {
        "default": {
            "KAFUKA_TOPIC_WEBPAGE": "rbt_webpage",
            "KAFUKA_TOPIC_WEBPAGE_UPDATE": "rbt_webpage_update",
            "KAFUKA_TOPIC_HOTLIST": "rbt_hotlist",
            "KAFUKA_TOPIC_TOPLIST": "rbt_toplist",
            "KAFUKA_TOPIC_IMAGE": "rbt_image",
            "KAFUKA_TOPIC_VIDEO": "rbt_video",
            "KAFUKA_TOPIC_COMMENT": "rbt_comment"
        },
        "kafka_en": {
            "KAFUKA_TOPIC_WEBPAGE": "rbt_webpage_en",
            "KAFUKA_TOPIC_WEBPAGE_UPDATE": "rbt_webpage_update_en",
            "KAFUKA_TOPIC_HOTLIST": "rbt_hotlist_en",
            "KAFUKA_TOPIC_TOPLIST": "rbt_toplist_en",
            "KAFUKA_TOPIC_IMAGE": "rbt_image_en",
            "KAFUKA_TOPIC_VIDEO": "rbt_video_en",
            "KAFUKA_TOPIC_COMMENT": "rbt_comment_en"
        },
        "kafka_people": {
            "KAFUKA_TOPIC_WEBPAGE": "rbt_webpage_people",
            "KAFUKA_TOPIC_WEBPAGE_UPDATE": "rbt_webpage_update_people",
            "KAFUKA_TOPIC_HOTLIST": "rbt_hotlist_people",
            "KAFUKA_TOPIC_TOPLIST": "rbt_toplist_people",
            "KAFUKA_TOPIC_IMAGE": "rbt_image",
            "KAFUKA_TOPIC_VIDEO": "rbt_video_people",
            "KAFUKA_TOPIC_COMMENT": "rbt_comment_people"
        }
    }

    post_detail_must_key_list = [
        "webpage_url",
        "title",
        "webpage_code",
        # "source_crawl",
        "content",
    ]

    post_image_must_key_list = [
        "webpage_code",
    ]


class ProductionConfig(BaseConfig):
    '''
    线上正式环境配置
    '''
    MONGO_CONFIG = {
        "host": "114.115.148.216",
        "port": 27025,
        "db": "irobot_new",
        "user": "crawler",
        "passwd": "test",

        "host_weixin": "114.115.148.216",
        "user_weixin": "irobot",
        "passwd_weixin": "irobot",
        "db_weixin": "irobot"
    }

    REDIS_CONFIG = {
        "host": "192.168.1.177",
        "port": "6379",
        "db": "1",
        "db_distinct": "13",
        "password": "iNews2018!",
    }
    KAFUKA_HOST = " 192.168.1.15:19092,192.168.1.114:19092,192.168.1.223:19092"


class DevelopmentConfig(BaseConfig):
    '''
    开发环境配置
    '''
    MONGO_CONFIG = {
        "host": "10.50.1.202",
        "port": 27017,
        "db": "irobot_lws",
        "user": "irobot_lws",
        "passwd": "irobot_lws",

        "host_weixin": "10.50.1.202",
        "user_weixin": "irobot_lws",
        "passwd_weixin": "irobot_lws",
        "db_weixin": "irobot_lws"

    }

    REDIS_CONFIG = {
        "host": "10.50.1.200",
        "port": "6379",
        "db": "1",
        "db_distinct": "14",
        "password": None,
    }

    KAFUKA_HOST = "10.50.1.197:19092,10.50.1.202:19092, 10.50.1.198:19092"


def get_settings_environment(envtype):
    '''
    根据环境类型获取对应环境配置
    :param envtype: 环境类型 pro->线上环境  dev->开发环境 其他均返回默认环境
    :return:
    '''
    if envtype == "pro":
        return ProductionConfig()
    elif envtype == "dev":
        return DevelopmentConfig()
    else:
        return BaseConfig()



if __name__ == '__main__':
    _evn = get_settings_environment("pro")
    print _evn.post_detail_must_key_list
