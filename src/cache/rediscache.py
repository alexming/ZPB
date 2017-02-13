#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: cache/rediscache.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:02
#########################################################################

# redis
import redis
# zpb
from zpb.conf import Conf, logger


class TQRedis:
    #私有类变量
    __RedisPool = {}
    #redis 选择db0
    #私有方法
    @classmethod
    def __GetPool(Self, redisName):
        if Self.__RedisPool.has_key(redisName):
            return Self.__RedisPool[redisName]
        else:
            redis_node = Conf.REDIS_NODES[redisName]
            pool = redis.ConnectionPool(**redis_node)
            Self.__RedisPool[redisName] = pool
            return pool

    @classmethod
    def GetRedis(Self, redisName):
        pool = Self.__GetPool(redisName)
        redisCli = redis.StrictRedis(connection_pool=pool)
        try:
            if redisCli.ping():
                return redisCli
            else:
                error_msg = u'redis服务器({})ping失败'.format(redisName)
                logger.error(error_msg)
                raise BaseException(error_msg)
        except redis.exceptions.ConnectionError as e:
            error_msg = u' redis服务器({})连接失败,原因:{}'.format(redisName, str(e))
            raise BaseException(error_msg)


# 全局账号认证缓存
AuthRedisCli = TQRedis.GetRedis(Conf.REDIS_AUTH_SOURCE)
# 全局字典(正向->下载)
ForwardDictCli = TQRedis.GetRedis(Conf.REDIS_FORWARD_SOURCE)
# 全局字典(反向->上传)
BackwardDictCli = TQRedis.GetRedis(Conf.REDIS_BACKWARD_SOURCE)
# 全局简历缓存
ResumeRedisCli = TQRedis.GetRedis(Conf.REDIS_RESUME_SOURCE)
# 全局邮件缓存
MailRedisCli = TQRedis.GetRedis(Conf.REDIS_MAIL_SOURCE)
