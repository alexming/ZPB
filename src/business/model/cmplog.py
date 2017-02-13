#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/business/model/cmplog.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-16 00:29
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.sql import func
# zpb
from zpb.business.model.basemodel import BaseModel
from zpb.conf import logger


class CMPLog(BaseModel):
    # 表名
    __tablename__ = 'cmp_log'
    # 表结构
    log_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    log_type = Column(Integer)
    site_id = Column(Integer)
    job_id = Column(Integer)
    resume_code = Column(String(32))
    is_auto_run = Column(String(1))
    other_info = Column(String(2000))
    log_status = Column(Integer)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    @staticmethod
    def new(companyid, siteid, logtype):
        log = CMPLog()
        log.company_id = companyid
        log.site_id = siteid
        log.log_type = logtype
        log.job_id = 0
        log.log_status = 0
        log.is_auto_run = 'F'
        log.is_valid = 'T'
        log.create_time = datetime.today()
        log.create_user = 'zpb'
        return log

    @staticmethod
    def appendResumeImport(companyid, siteid, imphistoryid, number):
        log = CMPLog.new(companyid, siteid, 201)
        log.resume_code = imphistoryid
        log.other_info = u'收到 {} 封新的简历,请及时查收!'.format(number)
        return log.save()

    @staticmethod
    def appendJobImport(companyid, siteid, number):
        log = CMPLog.new(companyid, siteid, 103)
        log.other_info = u'同步 {} 个新的职位,请及时查收!'.format(number)
        return log.save()
