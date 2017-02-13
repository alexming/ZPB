#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/siteconfig.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 16:01
#########################################################################


# stdlib
import importlib
# zpb
from zpb.conf import logger
from zpb.utils.tools import str2int
from zpb.service.authservice import AuthService


class SiteConfig(object):

    _SITE = {
        
    }

    @staticmethod
    def getSiteNameWithId(siteid):
        return SiteConfig.getSiteNameById(siteid)


    @staticmethod
    def getSiteNameById(siteid):
        if isinstance(siteid, basestring):
            siteid = str2int(siteid)
        if SiteConfig._SITE.has_key(siteid):
            return SiteConfig._SITE[siteid]['name']
        else:
            return siteid


    @staticmethod
    def _getSiteById(siteid):
        if isinstance(siteid, basestring):
            siteid = str2int(siteid)
        if SiteConfig._SITE.has_key(siteid):
            return SiteConfig._SITE[siteid]


    @staticmethod
    def GetTaskHandler(companyid, siteid, taskid):
        from zpb.exception import *
        site = SiteConfig._getSiteById(siteid)
        if site:
            bind = None
            if companyid > 0:
                bind = AuthService().getBindByCompanyIdAndSiteId(companyid, siteid)
            if not bind and companyid > 0:
                raise CompanyAccountNotBindError(companyid, siteid, u'账号未绑定')
            try:
                module = site['module']
                clazz  = site['class']
                m = importlib.import_module(module)
                c = getattr(m, clazz)
                handler = c(taskid, bind)
                handler.name = SiteConfig.getSiteNameById(siteid)
                return handler
            except (ValueError, ImportError, AttributeError) as e:
                raise BaseError(companyid, siteid, u'任务对象生成失败,' + e)
        else:
            raise CompanyAccountNotSupportError(companyid, siteid, u'未支持的招聘平台')
