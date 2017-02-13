#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/ressyncdistribute.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-20 16:50
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.sql import func, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.siteconfig import SiteConfig
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger


class ResSyncDistribute(BaseModel):
    # 表名
    __tablename__ = 'res_sync_distribute'
    # 表结构
    sync_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    resume_code = Column(String(32))
    site_id = Column(Integer)
    from_site_code = Column(String(50))
    sync_status = Column(Integer)
    request_time = Column(DATETIME)
    process_time = Column(DATETIME)
    error_message = Column(Text)
    own_pay = Column(String(1))
    is_valid = Column(String(1))
    create_time = Column(DATETIME, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(DATETIME, server_onupdate=func.now())
    modify_user = Column(String(50))

    def _uuid(self):
        uid = md5('{}-{}-{}-{}'.format(self.company_id, self.sync_id, self.resume_code, self.__class__.__name__))
        uname = u'<{}>简历付费下载'.format(SiteConfig.getSiteNameById(self.site_id))
        task = {
            'id': uid,
            'name': uname,
            'func': 'zpb.service.handleservice.DoResumePayDown',
            'args': (self.company_id, self.site_id, self.sync_id, self.from_site_code, self.own_pay, uid),
            'kwargs': {'priority': 1}
        }
        return task

    @classmethod
    def queryPending(cls):
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(ResSyncDistribute).filter(and_(ResSyncDistribute.sync_status==0,ResSyncDistribute.is_valid=='T')).limit(20)
            if rows:
                for row in rows:
                    ret.append(row._uuid())
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def queryWithId(cls, syncid):
        session = DBInstance.session
        try:
            row = session.query(ResSyncDistribute).filter(ResSyncDistribute.sync_id==syncid).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()
