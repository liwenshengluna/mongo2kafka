# -*- coding: utf-8 -*-

'''
Created on 2018年3月26日

@author: liws
@desciption: 微信数据入kafuka脚本  crontab定时执行即可
'''
import cStringIO
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
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import redis

from settings import mongo_config, KAFUKA_HOST, KAFUKA_PORT, savedirpath_weixin, redis_config
from settings import OBJECT_STORE, KAFKA_TOPICS
from log import mylogger_weixin
from settings import post_detail_must_key_list, post_image_must_key_list


class Weixin2Kafuka(object):
    '''
    微信mongodb数据入kafuka
    '''

    def __init__(self, mongo_config=None):
        self._redis_14 = redis.Redis(host=redis_config["host"], db=redis_config["db_distinct"], port=redis_config["port"], password=redis_config["password"])


if __name__ == '__main__':
    w2k = Weixin2Kafuka(mongo_config)
    s = w2k._redis_14.hget("webpage_urls", "http://mp.weixin.qq.com/s?timestamp=1523359445&src=3&ver=1&signature=jgvtizUy7hvJ2fxN04WNvoNIc68cpRS543UV1ffr3nFHU5uvNF*7T-*Smo5S5TSPyZ20MGrBctiRTWKIyeaEhyvTprrsPdeRQCxH9xjA17RjyksQ8VnuOoTAegzCwg4WlfZoprlUpxJmkQ2h0flovWN8wa6oTeRETurrPxPni3c=")
    print s
