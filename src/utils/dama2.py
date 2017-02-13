#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: utils/dama2.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-16 15:50
#########################################################################


#stdlib
import json
import binascii
from urllib import urlencode, unquote
from tornado.httpclient import HTTPRequest, HTTPClient, HTTPError
# zpb
from zpb.exception import *
from zpb.utils.tools import md5
from zpb.core.HttpHandler import HttpHandler
from zpb.conf import Conf, logger


class Dama2(object):
    """
    打码兔,限制每日最多打码题分1000
    提供网址打码和图片打码,后者打码速度更快
    """

    appid = Conf.DAMA['appid']
    appkey = Conf.DAMA['appkey']
    baseuri = Conf.DAMA['damauri']
    username = Conf.DAMA['username']
    passwd = Conf.DAMA['passwd']
    # 计算密码块
    md5pwd = md5(appkey + md5(md5(username) + md5(passwd)))

    def __init__(self):
        self.httpCli = HttpHandler(self.__class__.__name__)

    # 请求签名
    def _getSign(self, params):
        return md5(self.__class__.appkey + self.__class__.username + params)[0: 8]

    # 通过URL打码
    def d2Url(self, dtype, url):
        data = {
            'appID': self.__class__.appid,
            'user': self.__class__.username,
            'pwd': self.__class__.md5pwd,
            'type': dtype,
            'url': url,
            'sign': self._getSign(url)
        }
        rep = self.httpCli.call('{}/d2Url'.format(self.__class__.baseuri), data)
        js = json.loads(rep.data)
        if js['ret'] == 0:
            cookie = js.get('cookie', '')
            cookie = unquote(cookie).replace('+', ' ')
            return {'verify': js['result'], 'cookie': cookie, 'retid': js['id']}
        else:
            raise DamaError(js['ret'])

    # 通过验证码图片打码
    def d2File(self, dtype, filedata):
        data = {
            'appID': self.__class__.appid,
            'user': self.__class__.username,
            'pwd': self.__class__.md5pwd,
            'type': dtype,
            'fileData': binascii.b2a_hex(filedata),
            'sign': self._getSign(filedata)
        }
        rep = self.httpCli.call('{}/d2File'.format(self.__class__.baseuri), data)
        js = json.loads(rep.data)
        if js['ret'] == 0:
            cookie = js.get('cookie', '')
            cookie = unquote(cookie).replace('+', ' ')
            return {'verify': js['result'], 'cookie': cookie, 'retid': js['id']}
        else:
            raise DamaError(js['ret'])


if __name__ == '__main__':
    Dama2().d2Url(42, 'http://www.xxxx.com/ValidateCodePicture.aspx?Key=')
