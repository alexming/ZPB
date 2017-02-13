#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/business/model/cmpactivity.py
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


class CMPActivity(BaseModel):
    # 表名
    __tablename__ = 'cmp_out_activity'
    # 表结构
    activity_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    resume_code = Column(String(32))
    job_id = Column(Integer)
    activity_type = Column(Integer)
    activity_content = Column(String(200))
    import_id = Column(Integer)
    create_time = Column(TIMESTAMP, server_default=func.now())

    def __init__(self, companyid, acttype):
        self.company_id = companyid
        self.activity_type = acttype
        self.import_id = 0
        self.create_time = datetime.today()

