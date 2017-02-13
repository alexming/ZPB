#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/imphistory.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 16:01
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME, TEXT
from sqlalchemy.sql import func
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.model.cmpactivity import CMPActivity
from zpb.business.siteconfig import SiteConfig
from zpb.exception import *
from zpb.conf import logger


# 简历导入历史日志
class ImpHistory(BaseModel):
    # 表名
    __tablename__ = 'imp_history'
    # 表结构
    history_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    import_type = Column(Integer)
    src_memo = Column(String(500))
    succ_num = Column(Integer)
    fail_num = Column(Integer)
    start_time = Column(DATETIME)
    end_time = Column(DATETIME)
    import_id = Column(Integer)
    proc_status = Column(Integer)
    fail_reason = Column(TEXT)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    __IMPTYPE__ = {
        1: u'文件',
        2: u'文本文件',
        3: u'邮箱',
        4: u'招聘平台'
    }

    @staticmethod
    def new(companyid, siteid, importid, importtype=4):
        imp = ImpHistory()
        imp.company_id = companyid
        # 此处默认(4),resumeservice手工导入修改该数据
        imp.import_type = importtype  # 招聘平台导入
        if imp.import_type == 4:
            imp.src_memo = SiteConfig.getSiteNameWithId(siteid)
        else:
            imp.src_memo = ''
        imp.import_id = importid
        imp.succ_num = 0
        imp.fail_num = 0
        imp.start_time = datetime.today()
        # 初始状态处理中
        imp.proc_status = 0
        imp.is_valid = 'T'
        imp.create_user = 'zpb'
        imp.create_time = datetime.today()
        return imp

    @classmethod
    def queryByHistoryId(cls, historyid):
        session = DBInstance.session
        try:
            row = session.query(ImpHistory).filter(ImpHistory.history_id == historyid).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def incSuccessByHistoryId(cls, historyid):
        session = DBInstance.session
        try:
            stmt = update(ImpHistory).where(ImpHistory.history_id == historyid).values(succ_num = ImpHistory.succ_num + 1)
            session.execute(stmt)
            session.commit()
            return True
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def removeByHistoryId(cls, historyid):
        session = DBInstance.session
        try:
            stmt = update(ImpHistory).where(ImpHistory.history_id == historyid).\
                    values(is_valid='F', proc_status=1, end_time=datetime.today())
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    def save(self):
        # 增加公告
        if self.proc_status in [1, 2]:
            if self.succ_num > 0:
                act = CMPActivity(self.company_id, 40 + self.import_type)
                act.activity_content = u'从{}{}同步 {} 份简历'.format(
                    ImpHistory.__IMPTYPE__[self.import_type],
                    u'[{}]'.format(self.src_memo) if self.src_memo else '',
                    self.succ_num)
                act.import_id = self.history_id
                act.save()
        return super(ImpHistory, self).saveAndRefresh()


if __name__ == '__main__':
    imp = ImpHistory.new(24, 1, 0)
    imp.saveAndRefresh()
    print imp.history_id
    #imp.modify_user = 'zpb'
    #imp.save()
    #print imp.history_id
    #imp.save()
    #print imp.history_id
