#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/jobmemo.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-06 11:57
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.sql import func
# zpb
from zpb.business.model.basemodel import BaseModel
from zpb.conf import logger


class JobMemo(BaseModel):
    # 表名
    __tablename__ = 'job_memo'
    # 结构
    memo_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    memo_content = Column(Text)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))

    def __init__(self, jobid):
        self.job_id = jobid
        self.is_valid = 'T'
        self.create_user = 'zpb'
        self.create_time = datetime.today()
