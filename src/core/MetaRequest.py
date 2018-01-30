#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/MetaRequest.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:01
#########################################################################


# stdlib
import urllib2
import logging
from urllib import urlencode
from traceback import format_exc
# zpb
from zpb.utils.tools import enum
from zpb.conf import logger


urllib2.socket.setdefaulttimeout(30)

HTTP_STATUS = enum(OK = 200, SUCCESS = 1, FAILURE = 0, AUTHFAILURE = -1)

#封装原生urllib2.Request,带有meta功能
class XRequest(urllib2.Request, object):


    def __init__(self, url, data=None, headers={}, callback=None, errback=None, meta=None, origin_req_host=None, unverifiable=False):

        super(XRequest, self).__init__(url, data, headers, origin_req_host, unverifiable)

        self.url = self.get_full_url()
        #assert callback or not errback, "Cannot use errback without a callback"
        self.callback = callback
        self.errback = errback

        self._meta = meta


    @property
    def meta(self):
        return self._meta or {}


    def call(self, data=None):
        try:
            if data is not None:
                if isinstance(data, dict):
                    self.add_data(urlencode(data))
                elif isinstance(data, basestring):
                    self.add_data(data)
            ret = urllib2.urlopen(self)
            response = XResponse(ret, self)
            response.status = HTTP_STATUS.SUCCESS
            return response
        except Exception, e:
            msg = 'Http Request call error：{}\n{}\n'.format(self.url, format_exc(e))
            logger.error(msg)
            response = XResponse(None, self)
            response.status = HTTP_STATUS.FAILURE
            response.msg = msg
            return response


class XResponse(object):

    def __init__(self, response, request):
        if response:
            self.code = response.code
            self.msg = response.msg
            self.url = response.url
            self.headers = response.headers
        else:
            self.code = 0
            self.msg = ''
            self.url = ''
            self.headers = None
        if isinstance(request, XRequest):
            self.request = request
            self.meta = {}
            self.meta.update(self.request.meta)
            self.data = None
            if self.code == HTTP_STATUS.OK:
                self.data = response.read()
        self.status = HTTP_STATUS.SUCCESS
