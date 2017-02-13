#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/imphistoryresume.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-30 10:54
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import TIMESTAMP
# zpb
from zpb.business.model.basemodel import BaseModel
from zpb.conf import logger


class ImpHistoryResume(BaseModel):
    # 表名
    __tablename__ = 'imp_history_resume'
    # 表结构
    detail_id = Column(Integer, primary_key=True)
    # imp_history表主ID
    history_id = Column(Integer)
    company_id = Column(Integer)
    # 本系统简历ID(uuid)
    resume_code = Column(String(50))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    @staticmethod
    def newAndSave(historyid, companyid, resumecode):
        imp = ImpHistoryResume()
        imp.history_id = historyid
        imp.company_id = companyid
        imp.resume_code = resumecode
        imp.is_valid = 'T'
        imp.create_user = 'zpb'
        imp.create_time = datetime.today()
        return imp.save()
