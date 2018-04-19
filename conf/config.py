# -*- coding: utf-8 -*-
'''
Created on 2017年6月12日

@author: huang
@desciption:环境配置信息
'''
import os
import logging
import socket



class Env_base(object):
    
    '''
    project config/项目参数配置
    '''
    PROJ_NAME = "irobot"
    PROJ_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) #项目目录，绝对路径
    PROJ_IS_PATH_APPEND = True #是否添加项目路径到python path
    PROJ_PATH_CONF_FILE_NAME = "MyModule.pth"
    PROJ_DEFAULT_ENV = "local_host" #默认环境
#     PROJ_DEFAULT_ENV = "new_test" #默认环境
#     PROJ_DEFAULT_ENV = "hw" #默认环境

    PROJ_HOSTNAME = socket.gethostname()
    PROJ_RUN_ASYNC = True #是否异步执行
    
    '''
    log/日志
    '''
    LOG_NAME = PROJ_NAME
    LOG_LEVEL = logging.INFO
    LOG_FILE_NAME = PROJ_NAME
    LOG_FILE_SAVE_PATH = os.path.join(PROJ_ROOT_DIR,"log")
    LOG_IS_OUTPUT_FILE = True
    LOG_IS_OUTPUT_CONSOLE = True
    
    
    '''
    obs info：华为云对象存储配置信息
    '''
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
    
    '''
    celery
    '''
    CELERY_NAME_C = PROJ_NAME #名称
    CELERY_TASKS_PATH_LIST_C = ["tasks"] #celery tasks所在目录
#     CELERY_IMPORTS=("tasks.tasks","tasks.task_time","tasks.task_worker",)
    CELERYD_FORCE_EXECV = True # 非常重要,有些情况下可以防止死锁
    CELERY_BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 3600}  # 任务发出后，经过一段时间还未收到acknowledge , 就将任务重新交给其他worker执行
    CELERYD_TASK_TIME_LIMIT = 180    # 单个任务的运行时间不超过此值，否则会被SIGKILL 信号杀死
    CELERYD_TASK_SOFT_TIME_LIMIT = 180
    CELERY_ENABLE_UTC = True #是否启用时区
    CELERY_DISABLE_RATE_LIMITS   = True
    CELERY_TIME_ZONE = "Asia/Shanghai" #时区
    CELERY_TASK_RESULT_EXPIRES = 18000 #执行结果过期时间 ，rabbitmq才起作用
    CELERY_ACCEPT_CONTENT = ['json'] #
    CELERY_TASK_SERIALIZER = 'json' #序列化类型
    CELERY_RESULT_SERIALIZER = 'json' #执行结果类型
    CELERYD_CONCURRENCY = 25 #worker启动数量
    CELERY_REDIS_MAX_CONNECTIONS = 100 #redis 链接最大数量
    CELERYD_PREFETCH_MULTIPLIER = 500 # celery worker 每次去rabbitmq取任务的数量，我这里预取了4个慢慢执行,因为任务有长有短没有预取太多
    CELERYD_MAX_TASKS_PER_CHILD = 1000 # 每个worker执行了多少任务就会死掉，我建议数量可以大一些，比如200
    CELERY_DEFAULT_QUEUE = "irobot_default_queue" # 默认的队列，如果一个消息不符合其他的队列就会放在默认队列里面
    
    '''
    image
    '''
    IMAGE_SAVE_ROOT_DIR = "/inews"
    IMAGE_SERVER_PLACEHODER = "${inewsImageServer}"
    IMAGE_SECOND_DIR = "image"
    IMAGE_LIB_LIMIT_COUNT = 2 #图库图片数量限制，小于2张的不算入图库
    IMAGE_WIDTH_PIXEL_LIMIT = 500 #图库图片宽度限制
    IMAGE_HEIGHT_PIXEL_LIMIT = 500 #图库图片高度限制
    
    '''
    video
    '''
    VIDEO_TAGS_LIST = ["object","video","embed"] 
    
    '''
    task process
    '''
    TASK_PROCESS_LIST = "list"
    TASK_PROCESS_INTERM = "interm"
    TASK_PROCESS_DETAIL = "detail"
    TASK_PROCESS_GROUP_IMAGE = "group_image"
    TASK_PROCESS_INNER_GROUP_IMAGE = "inner_group_image"
    TASK_PROCESS_COMMENT = "comment"                      #added by hq 2017/10/11
    TASK_PROCESS_COMMENT_URL = "comment_url"             #added by hq 2017/10/11
    TASK_PROCESS_IMAGE = "image"
    TASK_PROCESS_VIDEO = "video"
    
    BROWSER_TYPE = "phantomjs"
    
    GROUP_IMAGE_OUTSIDE = "<div id = 'uec_inews_picture_group_{0}'><script>var uec_inews_picture_group_json = {1}</script></div>"
    
    '''
    kafka
    '''    
    KAFUKA_ON = True
    KAFKA_TOPICS = {
            "default":{
                "KAFUKA_TOPIC_WEBPAGE" : "rbt_webpage",
                "KAFUKA_TOPIC_WEBPAGE_UPDATE" : "rbt_webpage_update",
                "KAFUKA_TOPIC_HOTLIST" : "rbt_hotlist",
                "KAFUKA_TOPIC_TOPLIST" : "rbt_toplist",
                "KAFUKA_TOPIC_IMAGE" : "rbt_image",
                "KAFUKA_TOPIC_VIDEO" : "rbt_video",
                "KAFUKA_TOPIC_COMMENT" : "rbt_comment"
                },
            "kafka_en" : {
                "KAFUKA_TOPIC_WEBPAGE" : "rbt_webpage_en",
                "KAFUKA_TOPIC_WEBPAGE_UPDATE" : "rbt_webpage_update_en",
                "KAFUKA_TOPIC_HOTLIST" : "rbt_hotlist_en",
                "KAFUKA_TOPIC_TOPLIST" : "rbt_toplist_en",
                "KAFUKA_TOPIC_IMAGE" : "rbt_image_en",
                "KAFUKA_TOPIC_VIDEO" : "rbt_video_en",
                "KAFUKA_TOPIC_COMMENT" : "rbt_comment_en"                    
                },
            "kafka_people" : {
                "KAFUKA_TOPIC_WEBPAGE" : "rbt_webpage_people",
                "KAFUKA_TOPIC_WEBPAGE_UPDATE" : "rbt_webpage_update_people",
                "KAFUKA_TOPIC_HOTLIST" : "rbt_hotlist_people",
                "KAFUKA_TOPIC_TOPLIST" : "rbt_toplist_people",
                "KAFUKA_TOPIC_IMAGE" : "rbt_image",
                "KAFUKA_TOPIC_VIDEO" : "rbt_video_people",
                "KAFUKA_TOPIC_COMMENT" : "rbt_comment_people"                    
                }
        }
#     KAFUKA_TOPIC_WEBPAGE = "rbt_webpage"
#     KAFUKA_TOPIC_WEBPAGE_UPDATE = "rbt_webpage_update"
#     KAFUKA_TOPIC_HOTLIST = "rbt_hotlist"
#     KAFUKA_TOPIC_IMAGE = "rbt_image"
#     KAFUKA_TOPIC_VIDEO = "rbt_video"
#     KAFUKA_TOPIC_COMMENT = "rbt_comment"
    '''
    OBJECT_STORE
    '''
    OBJECT_STORE = {
        "default" : {
            "IMAGE_SAVE_ROOT_DIR" : "/inews",
            "IMAGE_SERVER_PLACEHODER" : "${inewsImageServer}",
            "IMAGE_SECOND_DIR"  : "default_image",
            "IMAGE_LIB_LIMIT_COUNT"  : 2,  # 图库图片数量限制，小于2张的不算入图库
            "IMAGE_WIDTH_PIXEL_LIMIT"  : 500,  # 图库图片宽度限制
            "IMAGE_HEIGHT_PIXEL_LIMIT"  : 500  # 图库图片高度限制
        },
        "oss": {
            "IMAGE_SAVE_ROOT_DIR": "/inews",
            "IMAGE_SERVER_PLACEHODER": "https://imedia-peoplesdaily.oss-cn-beijing.aliyuncs.com",
            "IMAGE_SECOND_DIR": "oss_image",
            "IMAGE_LIB_LIMIT_COUNT": 2,  # 图库图片数量限制，小于2张的不算入图库
            "IMAGE_WIDTH_PIXEL_LIMIT": 500,  # 图库图片宽度限制
            "IMAGE_HEIGHT_PIXEL_LIMIT": 500  # 图库图片高度限制
        },
    }


class local_host(Env_base):
    PROJ_IS_PATH_APPEND = False  # windows下不需要添加项目路径到python path
#     PROJ_RUN_ASYNC = False  # 是否异步执行
    PROJ_RUN_ASYNC = False  # 是否异步执行

    '''
	log/日志
	'''
    LOG_LEVEL = logging.DEBUG
    LOG_FILE_SAVE_PATH = "E:\\logs\\irobot"
    LOG_IS_OUTPUT_FILE = True

    '''
	obs info：华为云对象存储配置信息
	'''
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
    '''
	redis/redis配置
	'''
    REDIS_HOST = "10.50.1.200" #192.168.6.8
    REDIS_PORT = "6379"#"6379"
    REDIS_DB0 = "0"
    REDIS_DB1 = "1"
    REDIS_DB14 = "14"

    REDIS_PW = None
    REDIS_KEY_EX = 1296000  # redis key过期时间，15天
    
    REDIS_WEBPAGE_URLS = "webpage_urls"
    REDIS_TASK_WEBPAGE_URLS = "task_webpage_urls"
    REDIS_TASK_CACHE = "task_cache"
    REDIS_COMMENT_ID = "task_commont_id"
    REDIS_COMMENT_URL_TASK = "task_comment_url_task"
    REDIS_WXB_URL = "task_wxb_url"
    REDIS_WEIXIN_ID = 'task_weixin_id'
    
    '''
	celery
	'''
    CELERY_BROKER = "redis://%s:%s/%s" % (REDIS_HOST, REDIS_PORT, REDIS_DB14)  # broker url
    CELERY_BACKAND = "redis://%s:%s/%s" % (REDIS_HOST, REDIS_PORT, REDIS_DB14)  # result backand

    COMMENT_CALLBACK_URL = "http://10.6.7.198:8080"
    '''
	mongodb
	'''
    MONGODB_HOST = ["10.50.1.197","10.50.1.202","10.50.1.198"]
    MONGODB_PORT = 27017
    MONGODB_DB = "irobot_local"
    MONGODB_USER = "irobot_local"
    MONGODB_PASSWORD = "irobot"
    
    IMAGE_SAVE_ROOT_DIR = "E:/imgs"
    
    KAFUKA_ON = True
    # KAFUKA_HOST = "10.43.4.57:9092"
    KAFUKA_HOST = "10.50.1.197:19092,10.50.1.202:19092,10.50.1.198:19092" #'192.168.1.58:19092,192.168.1.70:19092,192.168.1.193:19092'
    KAFUKA_PORT = 9092
    
    ERROR_RETRY_NUM = 3
    
    BROWSER_TYPE = "firefox"
    BROWSER_TYPE = "phantomjs"
    BROWSER_PHANTOMJS_INIT_PATH = "E:\\MYDATA\\work2\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe"
    BROWSER_PHANTOMJS_INIT_PATH = "C:\\Users\\admin\\Anaconda3\\phantomjs.exe"
    BROWSER_TIMEOUT = 60 #单位：秒


class Env_new_test(Env_base):
    PROJ_IS_PATH_APPEND = True  # windows下不需要添加项目路径到python path

    '''
    log/日志
    '''
    LOG_LEVEL = logging.DEBUG
    LOG_FILE_SAVE_PATH = "/data/userhome/irobot/logs/irobot"
    LOG_IS_OUTPUT_FILE = False

    '''
    obs info：华为云对象存储配置信息
    '''
    OBS_ON = True  # 测试环境对象存储关闭

    '''
    redis/redis配置
    '''
    REDIS_HOST = "192.168.6.8" #10.50.1.200
    # REDIS_HOST = "10.50.1.200"
    REDIS_PORT = "6379"
    REDIS_DB0 = "0"
    REDIS_DB1 = "1"
    REDIS_DB14 = "13"
    REDIS_PW = None
    REDIS_KEY_EX = 1296000  # redis key过期时间，15天
    
    REDIS_WEBPAGE_URLS = "webpage_urls"
    REDIS_TASK_WEBPAGE_URLS = "task_webpage_urls"
    REDIS_TASK_CACHE = "task_cache"
    REDIS_COMMENT_ID = "task_commont_id"
    REDIS_WXB_URL = "task_wxb_url"
    REDIS_WEIXIN_ID = 'task_weixin_id'

    '''
    celery
    '''
    CELERY_BROKER = "redis://%s:%s/%s" % (REDIS_HOST, REDIS_PORT, REDIS_DB14)  # broker url
    CELERY_BACKAND = "redis://%s:%s/%s" % (REDIS_HOST, REDIS_PORT, REDIS_DB14)  # result backand

    COMMENT_CALLBACK_URL = "http://10.50.1.191"
    '''
    mongodb
    '''
    MONGODB_HOST = ["192.168.6.9","192.168.6.11","192.168.6.13"]
    # MONGODB_HOST = ["10.50.1.197","10.50.1.202","10.50.1.198"]
    MONGODB_PORT = 27017
    MONGODB_DB = "irobot_test"
    MONGODB_USER = "irobot_test"
    MONGODB_PASSWORD = "irobot"
    
    IMAGE_SAVE_ROOT_DIR = "/data/userhome/irobot/inews"
    
    KAFUKA_ON = True
#     KAFUKA_HOST = "10.43.4.57:9092"
    KAFUKA_HOST = "10.50.1.197:19092,10.50.1.202:19092,10.50.1.198:19092" #'192.168.1.58:19092,192.168.1.70:19092,192.168.1.193:19092'
    KAFUKA_PORT = 9092
    
    ERROR_RETRY_NUM = 3
    
    BROWSER_TYPE = "phantomjs"
    BROWSER_PHANTOMJS_INIT_PATH = "/data/userhome/irobot/software/phantomjs-2.1.1-linux-x86_64/bin/phantomjs"
    BROWSER_TIMEOUT = 60 #单位：秒


class Env_hw(Env_base):
    PROJ_IS_PATH_APPEND = True  # windows下不需要添加项目路径到python path

    '''
    log/日志
    '''
    LOG_LEVEL = logging.DEBUG
    LOG_FILE_SAVE_PATH = "/inews/crawlManager/logs/irobot"
    LOG_IS_OUTPUT_FILE = False

    '''
    obs info：华为云对象存储配置信息
    '''
    OBS_ON = True  # 测试环境对象存储关闭

    '''
    redis/redis配置
    '''
    REDIS_HOST = "192.168.1.177"
    REDIS_PORT = "6379"
    REDIS_DB0 = "0"
    REDIS_DB1 = "1"
    REDIS_DB14 = "13"
    REDIS_PW = "iNews2018!"
    REDIS_KEY_EX = 1296000  # redis key过期时间，15天
    
    REDIS_WEBPAGE_URLS = "webpage_urls"
    REDIS_TASK_WEBPAGE_URLS = "task_webpage_urls"
    REDIS_TASK_CACHE = "task_cache"
    REDIS_COMMENT_ID = "task_commont_id"
    
    '''
    celery
    '''
#     CELERY_BROKER = "redis://%s:%s/%s" % (REDIS_HOST, REDIS_PORT, REDIS_DB14)  # broker url
#     CELERY_BACKAND = "redis://%s:%s/%s" % (REDIS_HOST, REDIS_PORT, REDIS_DB14)  # result backand
    CELERY_BROKER = "redis://:%s@%s:%s/%s" % (REDIS_PW,REDIS_HOST,REDIS_PORT,REDIS_DB14) #redis broker url
    CELERY_BACKAND = "redis://:%s@%s:%s/%s" % (REDIS_PW,REDIS_HOST,REDIS_PORT,REDIS_DB14)


    COMMENT_CALLBACK_URL = "http://inewsengine.com"

    '''
    mongodb
    '''
#     MONGODB_HOST_ = ["114.115.148.216","114.115.148.20","114.115.148.52"]
    MONGODB_HOST = ["192.168.1.58","192.168.1.70","192.168.1.193"]
    MONGODB_PORT = 27025
    MONGODB_DB = "irobot"
    MONGODB_USER = "irobot"
    MONGODB_PASSWORD = "irobot"
    
    KAFUKA_ON = True
#     KAFUKA_HOST = '114.115.148.216:19092,114.115.148.20:19092,114.115.148.52:19092'
    KAFUKA_HOST = '192.168.1.58:19092,192.168.1.70:19092,192.168.1.193:19092'
    KAFUKA_PORT = 9092
    
    ERROR_RETRY_NUM = 3
    
    BROWSER_TYPE = "phantomjs"
    BROWSER_PHANTOMJS_INIT_PATH = "/inews/core/phantomjs/bin/phantomjs"
    BROWSER_TIMEOUT = 60 #单位：秒
 
    
def get_env(envtype=None):
    
    '''获取配置信息工厂方法'''
    
    if envtype is not None:
        if envtype.lower() == "local_host":
            return local_host()
        elif envtype.lower() == "hw":
            return Env_hw()
        elif envtype.lower() == "new_test":
            return Env_new_test()
    else:
        return get_env(envtype=Env_base.PROJ_DEFAULT_ENV)


def get_obs(name="default"):
    '''
    desc:获取对象存储实例
    '''
    if name is None:
        return None
    elif name.lower() == "default":
        from utils.obs_util import OBS_Api
        return  OBS_Api()
    elif name.lower() == "oss":
        from utils.oss_util import Oss2Util
        return Oss2Util()

