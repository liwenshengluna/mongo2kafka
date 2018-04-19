# coding=utf-8
'''
Created on 2017年6月13日

@author: huang
@desciption:redis操作工具
'''
import redis

class Redis_Conn(object):
    
    def __init__(self, host=None, db=None, port=None, password=None):
        #连接池
#         self.conn_pool = redis.ConnectionPool(host = host,db = db,port = port,password = password)
        #链接   redis.Redis(host="192.168.10.5",db=10, port=6379,password = "iNews2018!")
        self.conn = redis.Redis(host=host, db=db, port=port, password=password)
    
    def set(self,name,value,ex=None, px=None, nx=False, xx=False):
        '''
        ex，过期时间（秒）
        px，过期时间（毫秒）
        nx，如果设置为True，则只有name不存在时，当前set操作才执行,同setnx(name, value)
        xx，如果设置为True，则只有name存在时，当前set操作才执行
        '''
        self.conn.set(name, value, ex=ex, px=px, nx=nx, xx=xx)
        
    def get(self,name):
        '''
        name:键值
        '''
        return self.conn.get(name)
        