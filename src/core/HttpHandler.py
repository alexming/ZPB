#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/HttpHandler.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 16:59
#########################################################################


# stdlib
import ssl
import socket
import urllib2
from urllib import urlencode
from urllib2 import build_opener, HTTPCookieProcessor, ProxyHandler
from SerialCookieJar import RedisSerialCookieJar
# zpb
from zpb.core.MetaRequest import XRequest, XResponse, HTTP_STATUS
from zpb.cache.rediscache import AuthRedisCli
from zpb.exception import *
from zpb.conf import Conf, logger


class HttpHandler(object):

    MAX_RETRY_TIMES = 5

    def __init__(self, sessionKey):
        # key of session
        self.__sessionKey = sessionKey
        # 认证回调
        self.__authCallBack = None
        #
        self.__manager = None
        # 获取Cookiejar对象(存在本机的cookie消息)
        self.__cookieJar = RedisSerialCookieJar(AuthRedisCli)
        # 从session缓存服务器加载session
        self.__cookieJar.load(self.__sessionKey)
        # 自定义opener,并将opener跟CookieJar对象绑定
        self.__cookieOpener = build_opener(HTTPCookieProcessor(self.__cookieJar))
        # 代理
        if Conf.PROXY.has_key('host'):
            self.addProxy()
        #
        self.retry = 0

    @property
    def authCallBack(self):
        return self.__authCallBack

    @property
    def cookies(self):
        # 从session缓存服务器加载session
        self.__cookieJar.load(self.__sessionKey, True, True)
        return ';'.join('{}={}'.format(cookie.name, cookie.value) for cookie in self.__cookieJar)

    # 权限认证
    @authCallBack.setter
    def authCallBack(self, value):
        self.__authCallBack = value

    @property
    def manager(self):
        return self.__manager

    @manager.setter
    def manager(self, value):
        self.__manager = value

    def addProxy(self):
        proxy_support = ProxyHandler({'http': 'http://{host}:{port}'.format(**Conf.PROXY)})
        self.__cookieOpener.add_handler(proxy_support)
        proxy_https_support = ProxyHandler({'https': 'http://{host}:{port}'.format(**Conf.PROXY)})
        self.__cookieOpener.add_handler(proxy_https_support)

    def _addUserAgent(self, request):
        #request.add_header('Accept-Encoding', 'gzip, deflate, sdch')
        request.add_header('User-Agent', Conf.USERAGENT)

    # 阻塞式http请求
    def call(self, request, data=None, headers=None, authentication=True):
        if isinstance(request, basestring):
            request = XRequest(request)
        elif not isinstance(request, XRequest):
            raise ValueError('request must be a XRequest')
        self._addUserAgent(request)
        if headers:
            for (key, value) in headers.items():
                request.add_header(key, value)
        if data is not None:
            if isinstance(data, dict):
                request.add_data(urlencode(data))
            elif isinstance(data, basestring):
                request.add_data(data)
        try:
            self.__cookieJar.load(self.__sessionKey, True, True)
            ret = self.__cookieOpener.open(request)
            response = XResponse(ret, request)
            # 认证验证
            if self.__authCallBack and authentication:
                if self.__authCallBack(response):
                    response.status = HTTP_STATUS.SUCCESS
                    response.message = u'请求成功,已通过认证验证'
                else:
                    response.status = HTTP_STATUS.AUTHFAILURE
                    response.message = u'请求失败,无法通过cookie缓存访问第三方系统'
                    if authentication and self.manager:
                        from zpb.core.BaseHandlerManager import HANDLE_STATUS
                        self.manager.login()
                        if HANDLE_STATUS.SUCCESS == self.manager.status:
                            logger.info(u'<{}>登录成功,将再次执行原请求...'.format(self.manager.name))
                            self.manager.status = HANDLE_STATUS.AGAIN
                            # 增加登录成功后的cookie
                            request.add_unredirected_header('Cookie', self.cookies)
                            response = self.call(request, data, headers, authentication)
                        else:
                            response.message = u'<{}>-<{}>登录失败,{}'.format(self.manager.name, self.manager.bind.login_name, self.manager.message)
            else:
                response.status = HTTP_STATUS.SUCCESS
            return response
        except (urllib2.HTTPError, urllib2.URLError) as e:
            if isinstance(e, urllib2.HTTPError):
                raise NetworkError(str(e.code) + ', ' + request.url)
            else:
                raise NetworkError(str(e.reason) + ', ' + request.url)
        except (socket.timeout, ssl.SSLError) as e:
            if self.retry <= self.MAX_RETRY_TIMES:
                self.retry += 1
                return self.call(request, data, headers, authentication)
            else:
                raise NetworkError(u'超时, ' + request.url)
        except BaseException as e:
            raise UnHandleRuntimeError(e)

    def addCookieStr(self, cookiestr):
        self.__cookieJar.add(cookiestr)
        self.saveCookie()

    def addCookie(self, name, value, domain='.', path='/'):
        cookiestr = u'{}={}; domain={}; path={}; HttpOnly'.format(name, value, domain, path)
        self.addCookieStr(cookiestr)

    def saveCookie(self):
        self.__cookieJar.save(self.__sessionKey)

    def getCookie(self, cookiename, defaultVal=''):
        ret = self.__cookieJar.get(cookiename)
        if not ret:
            ret = defaultVal
        return ret

    def clearCookie(self):
        self.__cookieJar.clear()
        self.saveCookie()
