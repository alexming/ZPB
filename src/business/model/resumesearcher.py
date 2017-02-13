#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/resumesearcher.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-06 11:57
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME
from sqlalchemy.sql import func, text
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.siteconfig import SiteConfig
from zpb.service.authservice import AuthService
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger


class ResumeSearcher(BaseModel):
    # 表名
    __tablename__ = 'res_searcher'
    # 表结构
    searcher_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    searcher_name = Column(String(100))
    is_auto_import = Column(String(1))
    hour_frequency = Column(Integer)
    last_import_time = Column(DATETIME)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    def _uuid(self, siteid):
        uid = md5('{}-{}-{}-{}'.format(self.company_id, siteid, self.searcher_id, self.__class__.__name__))
        uname = u'<{}>简历搜索器:{}'.format(SiteConfig.getSiteNameById(siteid), self.searcher_name),
        task = {
            'id': uid,
            'name': uname,
            'func': 'service.searcherservice.DoResumeSearcher',
            'args': (self.company_id, siteid, uid, self.searcher_id)
        }
        return task

    @classmethod
    def queryPending(cls):
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(ResumeSearcher).filter(
                text("is_valid='T' and is_auto_import='T'"\
                    "and (last_import_time is null or "\
                    "date_add(last_import_time,interval hour_frequency day_hour)<now())")).limit(20)
            if rows:
                for row in rows:
                    sites = AuthService().getBindSiteByCompanyId(row.company_id)
                    for siteid in sites:
                        ret.append(row._uuid(siteid))
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def updateImportTimeBySearcherId(cls, searcherid):
        session = DBInstance.session
        try:
            stmt = update(ResumeSearcher).where(ResumeSearcher.searcher_id == searcherid).values(last_import_time = datetime.now())
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()


class ResumeSearcherCond(BaseModel):
    # 表名
    __tablename__ = 'res_searcher_cond'
    # 表结构
    condition_id = Column(Integer, primary_key=True)
    searcher_id = Column(Integer)
    condition_type = Column(String(20))
    condition_val = Column(String(100))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    @classmethod
    def queryBySearcherId(cls, searcherid):
        session = DBInstance.session
        try:
            rows = session.query(ResumeSearcherCond).filter(ResumeSearcherCond.searcher_id == searcherid).all()
            return rows
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()
