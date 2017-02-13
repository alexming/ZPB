#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: zpb/conf.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-08 18:01
#########################################################################


# stdlib
import logging
# config
from config import settings, BaseConf
# tornado
from tornado import httpclient


class Conf(BaseConf):
    """
    Configuration class
    """
    try:
        conf = settings.ZPB
    except AttributeError:
        conf = {}
    # master db configuration.
    DATABASE = conf.get('database', {})
    # Redis server nodes
    REDIS_NODES = conf.get('redis-nodes', {})
    # User-Agent
    USERAGENT = conf.get('user-agent', '')
    # Proxy
    PROXY = conf.get('proxy', {})
    # HTTP Server Port
    HTTPPORT = conf.get('httpserver', {}).get('port', 8700)
    # ygys
    YGYS = conf.get('ygys', {})
    # Dama2
    DAMA = conf.get('dama2', {})
    # 简历导入计数hash
    RESUME_IMPORT_HKEY = 'resume_import:%s'
    EMAIL_RESUME_IMPORT_HKEY = 'email_import:%s'
    RESUME_IMPORT_QUEUE_KEY = 'resume_import_queue'
    RESUME_GRABED_QUEUE_KEY = 'resume_grabed_queue'
    # 缓存器
    REDIS_AUTH_SOURCE = 'login_cache_0'
    REDIS_TASK_UNI_SOURCE = 'task_uni_queue'
    REDIS_FORWARD_SOURCE = 'dict_cache_2'
    REDIS_BACKWARD_SOURCE = 'dict_cache_1'
    REDIS_RESUME_SOURCE = 'task_resume'
    REDIS_MAIL_SOURCE = 'task_mail'

# logger
logger = logging.getLogger(Conf.APPNAME)
# logger of sudo
sudslog = logging.getLogger('suds.client')
if not Conf.YGYS.get('debug', False):
    sudslog.disabled = True

# tornado
httpdefaults = {'user_agent': Conf.USERAGENT, 'request_timeout': 60, 'use_gzip': True}
if 'host' in Conf.PROXY.keys():
    httpdefaults['proxy_host'] = Conf.PROXY['host']
    httpdefaults['proxy_port'] = Conf.PROXY['port']
httpclient.AsyncHTTPClient.configure('tornado.curl_httpclient.CurlAsyncHTTPClient', defaults=httpdefaults)