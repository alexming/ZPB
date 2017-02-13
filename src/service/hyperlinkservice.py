#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/hyperlinkservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-30 16:08
#########################################################################


# stdlib
import urllib
import urllib2
from urllib import quote
# zpb
from zpb.business.model.hyperlink import HyperLink
from zpb.conf import Conf, logger


baikurl = u'http://baike.baidu.com/search/word?word={}'

class NoRedirectHandler(urllib2.HTTPRedirectHandler):
    """docstring for RedirectHandler"""
    def http_error_302(self, req, fp, code, msg, headers):
        infourl = urllib.addinfourl(fp, headers, req.get_full_url())
        infourl.status = code
        infourl.code = code
        return infourl

    http_error_300 = http_error_302
    http_error_301 = http_error_302
    http_error_303 = http_error_302
    http_error_307 = http_error_302

http_handler = urllib2.HTTPHandler(debuglevel = 0)
http_opener = urllib2.build_opener(http_handler, NoRedirectHandler)


def _getHyperLink(itemname):
    response = None
    try:
        response = http_opener.open(baikurl.format(quote(itemname)))
        if response.code == 302:
            return response.headers.get('Location')
    except BaseException as e:
        logger.error(u'<{}>百科超链接查询失败,原因:{}'.format(itemname, str(e)))
    finally:
        if response:
            response.close()


def GetBaikLink(linkid, linkname):
    logger.info(u'正在处理关键词<{}>'.format(linkname))
    linkval = _getHyperLink(linkname)
    if linkval:
        try:
            if linkval.find('none') == -1:
                HyperLink.appendLink(linkid, linkval, 'T')
            else:
                HyperLink.appendLink(linkid, linkval, 'F')
        except:
            pass
