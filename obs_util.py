# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf8")

if sys.version_info.major == 2 or not sys.version > '3': 
    import httplib 
else: 
    import http.client as httplib 

from com.obs.client.obs_client import ObsClient 
from com.obs.models.put_object_header import PutObjectHeader

from settings import get_settings_environment

_AK = "FNWTWJJRI5BWKVUPJ1C7"
_SK = "OZvynXG2kyqCHCx6Kb9mpmtYJI0VEHiqweCMOpZW"
_ISSECURE = False
_SERVER = "obs.cn-north-1.myhwclouds.com" #域名
_SIGNATURE = "v4" #认证版本
_REGION = "beijing" #区域
_MD5 = None #加密方式
_ACL = "public-read" #对象读取权限
_LOCATION = "http://inewsengine.com"
_BUCKETNAME = "obs-inews-01" #缺省桶名称


class OBS_Api(object):
    '''
    desc:华为云对象存储
    '''
     
    def __init__(self, conf=None):
        if not conf:
            self.__conf__ = get_settings_environment("pro")
        
        #缺省配置
        self.__ak__ = self.__conf__.OBS_AK if conf is not None else _AK
        self.__sk__ = self.__conf__.OBS_SK if conf is not None else _SK
        self.__issecure__ = self.__conf__.OBS_ISSECURE if conf is not None else _ISSECURE
        self.__server__ = self.__conf__.OBS_SERVER if conf is not None else _SERVER
        self.__signature__ = self.__conf__.OBS_SIGNATURE if conf is not None else _SIGNATURE
        self.__region__ = self.__conf__.OBS_REGION if conf is not None else _REGION
        self.__client__ = ObsClient(access_key_id=self.__ak__,secret_access_key=self.__sk__,is_secure=self.__issecure__,server=self.__server__,signature=self.__signature__,region=self.__region__)
         
        #缺省桶名称
        self.__bucketname__ = self.__conf__.OBS_BUCKETNAME
         
        #消息头
        self.__header__ = PutObjectHeader(md5=self.__conf__.OBS_MD5,acl=self.__conf__.OBS_ACL,location=self.__conf__.OBS_LOCATION)
      
    def set_bucket_name(self,bucketname):
        '''
        #设置桶名称
        '''
        self.__bucketname__ = bucketname
         
    def set_putobjectheader(self,md5=None,acl=None,location=None):
        '''
        #设置消息头
        '''
        self.__header__ = PutObjectHeader(md5=md5,acl=acl,location=location)
     
    def uploadobject(self, up_path, localpath):
        '''
        #uploadPath:对象存储的objectkey
        #localPath:本地路径
        '''
        # metadata = kwargs.get("meta") if "meta" in kwargs else None
        f = open(localpath, "rb")
        content = f.read()
        f.close()
        resp = self.__client__.putContent(self.__bucketname__, up_path, content=content, headers=self.__header__)
        if resp.status > 300: 
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
        # return self.__client__.postObject(self.__bucketname__, uploadpath, localpath,metadata=metadata,headers=self.__header__)
     
    def __getattr__(self, attr):
        '''
        #OBS原生方法调用，按方法名称调用，例如：实例名obsapi调用对象列表方法，obsapi.listObjects(bucketName='obs-inews-01',max_keys=10)
        '''
        def wrapper(*args,**kwargs):
            return getattr(self.__client__,attr)(*args,**kwargs)
        return wrapper


if __name__ == "__main__":
    obs = OBS_Api()
    obs.uploadobject("image/1133333test/22222.jpg", "./1.jpg")
    # print ("common msg:status:{0},errorCode:{1},errorMessage:{2}".format(resp.status,resp.errorCode,resp.errorMessage))
