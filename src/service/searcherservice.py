#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/searcherservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-06 18:28
#########################################################################


import zpb
import logging
# zpb
from zpb.business.model.resumesearcher import ResumeSearcher
from zpb.business.siteconfig import SiteConfig
from zpb.exception import *
from zpb.conf import logger


def DoResumeSearcher(companyid, siteid, taskid, searcherid, **kwargs):
    try:
    	handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        ResumeSearcher.updateImportTimeBySearcherId(searcherid)
        if handler.bind.check_status in [0, 50]:
            handler.resume_search(searcherid)
        else:
            handler.message = u'绑定账号登录失败'
        logger.info(handler.message)
    except BaseError:
    	pass
