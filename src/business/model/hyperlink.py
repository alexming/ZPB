#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/hyperlink.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-29 17:43
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.sql import func, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger


class HyperLink(BaseModel):
    # 表名
    __tablename__ = 'res_hyper_link'
    # 表结构
    link_id = Column(Integer, primary_key=True)
    link_type = Column(String(10))
    link_name = Column(String(100))
    link_val = Column(String(200))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    def _uuid(self):
        uid = md5('{}-{}'.format(self.link_id, self.__class__.__name__))
        uname = u'关键词百科超链接'
        task = {
            'id': uid,
            'name': uname,
            'func': 'zpb.service.hyperlinkservice.GetBaikLink',
            'args': (self.link_id, self.link_name),
            'kwargs': {'priority': 50}
        }
        return task

    @staticmethod
    def _appendLink(linkname, linktype):
        session = DBInstance.session
        try:
            link = session.query(HyperLink).filter(
                and_(HyperLink.link_type==linktype, HyperLink.link_name==linkname)).first()
        finally:
            session.close()
        if not link:
            link = HyperLink()
            link.link_type = linktype
            link.link_name = linkname
            link.link_val = ''
            link.is_valid = 'F'
            link.create_time = datetime.today()
            link.create_user = 'zpb'
            return link.save()

    @staticmethod
    def appendLink(linkid, linkval, isvalid):
        session = DBInstance.session
        try:
            link = session.query(HyperLink).filter(HyperLink.link_id==linkid).first()
        finally:
            session.close()
        if link:
            link.link_val = linkval
            link.is_valid = isvalid
            link.modify_time = datetime.today()
            link.modify_user = 'zpb'
            return link.save()


    @classmethod
    def queryPending(cls):
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(HyperLink).filter(HyperLink.link_val=='').limit(20)
            if rows:
                for row in rows:
                    ret.append(row._uuid())
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

# dtc async
def AppendSchoolLink(name):
    return HyperLink._appendLink(name, 'school')

# dtc async
def AppendCompanyLink(name):
    return HyperLink._appendLink(name, 'company')
