#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/SerialCookieJar.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:02
#########################################################################


# stdlib
import time
from cookielib import (_warn_unhandled_exception, CookieJar, Cookie, LoadError, split_header_words)


#可序列化CookieJar
class SerialCookieJar(CookieJar):

    def __init__(self, policy=None):
        CookieJar.__init__(self, policy)

    def save(self, key, ignore_discard=False, ignore_expires=False):
        raise NotImplementedError()

    def load(self, key, ignore_discard=False, ignore_expires=False):
        raise NotImplementedError()

    def add(self, cookiestr):
        raise NotImplementedError()


class RedisSerialCookieJar(SerialCookieJar):

    def __init__(self, redis, policy=None):
        SerialCookieJar.__init__(self, policy)
        self.redis = redis

    def save(self, key, ignore_discard=False, ignore_expires=False):
        rvalue = ''
        now = time.time()
        for cookie in self:
            if ignore_discard and cookie.discard:
                continue
            if ignore_expires and cookie.is_expired(now):
                continue
            if cookie.secure: secure = "TRUE"
            else: secure = "FALSE"
            if cookie.domain.startswith("."): initial_dot = "TRUE"
            else: initial_dot = "FALSE"
            if cookie.expires is not None:
                expires = str(cookie.expires)
            else:
                expires = ""
            if cookie.value is None:
                name = ""
                value = cookie.name
            else:
                name = cookie.name
                value = cookie.value
            rvalue += "\t".join([cookie.domain, initial_dot, cookie.path,
                               secure, expires, name, value]) +  "\n"
        self.redis.set(key, rvalue)

    def load(self, key, ignore_discard=False, ignore_expires=False):
        now = time.time()
        line = ''
        try:
            rvalue = self.redis.get(key)
            if rvalue is None: return
            lines = rvalue.split("\n")
            for line in lines:
                if line == '': break
                if line.endswith("\n"): line = line[:-1]
                #注释
                if (line.strip().startswith(("#", "$")) or line.strip() == ""):
                    continue
                domain, domain_specified, path, secure, expires, name, value = \
                    line.split("\t")
                secure = (secure == "TRUE")
                domain_specified = (domain_specified == "TRUE")
                if name == '':
                    name = value
                    value = None
                #
                initial_dot = domain.startswith(".")
                assert domain_specified == initial_dot

                discard = False
                if expires == "":
                    expires = None
                    discard = True
                # assume path_specified is false
                c = Cookie(0, name, value,
                           None, False,
                           domain, domain_specified, initial_dot,
                           path, False,
                           secure,
                           expires,
                           discard,
                           None,
                           None,
                           {})
                if ignore_discard and c.discard:
                    continue
                if ignore_expires and c.is_expired(now):
                    continue
                self.set_cookie(c)
        except Exception:
            #_warn_unhandled_exception()
            raise LoadError("invalid format cookies redis %s: %r" % (key, line))

    def add(self, cookiestr):
        try:
            cookies = self._cookies_from_attrs_set(split_header_words([cookiestr]), None)
        except BaseException:
            cookies = []
        for cookie in cookies:
            self.set_cookie(cookie)

    def get(self, name):
        for cookie in self:
            if cookie.name == name:
                return cookie.value
                

if __name__ == '__main__':
    from cache.rediscache import AuthRedisCli
    cookiejar = RedisSerialCookieJar(AuthRedisCli)
    #cookiejar.load('rd2:59769472xppd')
    #cookiejar.add('MonitorGUID=c56cbd07961442b6880528a33b984389; domain=.zhaopin.com; path=/')
    #cookiejar.add('pcc=r=250905397&t=0; domain=.zhaopin.com; path=/; HttpOnly')
    #cookiejar.save('rd2:59769472xppd')
    cookiejar.load('cjol:999')
    cookiejar.add('ASP.NET_SessionId=ejz4v5kyahrh15bpghyiz1p4; path=/; HttpOnly;rms_masterct=rms05_ct; path=/; domain=.cjol.com')
    cookiejar.save('cjol:999')
