#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/dbtask.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:28
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME
from sqlalchemy.sql import text, func, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.model.jobsyncdistribute import JobSyncDistribute
from zpb.business.siteconfig import SiteConfig
from zpb.service.authservice import AuthService
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger


class DBTask(BaseModel):
    # 表名
    __tablename__ = 'job_sync_command'
    # 表结构
    sync_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    company_id = Column(Integer)
    site_id = Column(Integer)
    command_type = Column(Integer)
    other_id = Column(Integer)
    sync_status = Column(Integer)
    request_time = Column(DATETIME)
    proc_priority = Column(Integer)
    is_once = Column(String(1))
    last_proc_time = Column(DATETIME)
    succ_num = Column(Integer)
    fail_num = Column(Integer)
    log_info = Column(Text)
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    def _uuid(self, siteid):
        uid = md5('{}-{}-{}-{}'.format(self.company_id, siteid, self.sync_id, self.__class__.__name__))
        uname = u'<{}>任务指令中心:{}'.format(SiteConfig.getSiteNameById(siteid), self.command_type),
        task = {
            'id': uid,
            'name': uname,
            'func': 'zpb.service.handleservice.Do{}'.format(self.command_type),
            # args: 必要条件
            'args': (self.company_id, siteid, self.sync_id, uid),
            # kwargs: 非必要条件
            'kwargs': {'jobid': self.job_id, 'otherid': self.other_id, 'priority': self.proc_priority}
        }
        return task

    # 查询所有待处理任务
    @classmethod
    def queryPending(cls):
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(DBTask).filter(
                text('sync_status in (0, 11) and (expire_time is null or expire_time>now()) and '\
                    '(is_once=\'T\' or '\
                    '(is_once=\'F\' and (last_proc_time is null or date_add(last_proc_time,'\
                    'interval 1 day_hour)<now())))')).order_by(DBTask.proc_priority).limit(50)
            if rows:
                for row in rows:
                    # 针对所有网站进行处理
                    if row.site_id == 0 and row.command_type != 301:
                        sites = set(AuthService().getBindSiteByCompanyId(row.company_id))
                        # 职位操作
                        if row.command_type in [101, 102, 103, 104]:
                            sync_sites = set(JobSyncDistribute.queryValidSiteByJobIdAndCompanyId(row.job_id, row.company_id))
                            sites = sync_sites or sites
                        for siteid in sites:
                            ret.append(row._uuid(siteid))
                        # 没有任何有效绑定账号
                        if len(sites) == 0:
                            row.sync_status = 10
                            row.fail_num = 1
                            row.log_info = u'未绑定任何有效账号！'
                            row.save()
                    else:
                        ret.append(row._uuid(row.site_id))
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    # 依据主键查询
    @classmethod
    def queryWithId(cls, syncid):
        session = DBInstance.session
        try:
            row = session.query(DBTask).filter(DBTask.sync_id == syncid).first()
            if row:
                row.succ_num = 0
                row.fail_num = 0
                # 防止日志记录过长
                if row.log_info and row.log_info.count(u'\n') > 100:
                    row.log_new = True
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    # 新增任务(仅用於招聘助手绑定招聘平台账号后新增定期任务)
    @classmethod
    def newSchedule(cls, companyid, siteid, enable=True):
        session = DBInstance.session
        task = None
        try:
            task = session.query(DBTask).filter(and_(DBTask.company_id==companyid, DBTask.site_id==siteid, DBTask.command_type==201, DBTask.is_once=='F')).first()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()
        if task:
            if enable:
                task.sync_status = 0
                task.is_once = 'F'
                task.last_proc_time = datetime.today()
                task.modify_time = datetime.today()
                task.modify_user = 'zpb'
            else:
                task.sync_status = 20
                task.is_once = 'T'
                task.modify_time = datetime.today()
                task.modify_user = 'zpb'
        else:
            if enable:
                task = DBTask()
                task.job_id = 0
                task.company_id = companyid
                task.site_id = siteid
                task.command_type = 201
                task.sync_status = 0
                task.request_time = datetime.today()
                task.last_proc_time = datetime.today()
                task.proc_priority = 20
                task.succ_num = 0
                task.fail_num = 0
                task.other_id = 0
                task.is_once = 'F'
                task.log_info = ''
                task.create_time = datetime.today()
                task.create_user = 'zpb'
            else:
                return True
        return super(DBTask, task).save()

    def save(self):
        session = DBInstance.session
        try:
            kwargs = {
                'sync_status': self.sync_status,
                'last_proc_time': datetime.now()
            }
            # 周期性任务状态重置
            if self.is_once == 'F':
                kwargs['sync_status'] = 0
            if self.succ_num:
                kwargs['succ_num'] = DBTask.succ_num + self.succ_num
            if self.fail_num:
                kwargs['fail_num'] = DBTask.fail_num + self.fail_num
            if self.log_info:
                # 丢弃历史日志
                if getattr(self, 'log_new', None):
                    kwargs['log_info'] = self.log_info
                else:
                    kwargs['log_info'] = func.ifnull(DBTask.log_info, '') + self.log_info
                kwargs['log_info'] += u'\n'
            stmt = update(DBTask).where(DBTask.sync_id==self.sync_id).values(**kwargs)
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(self), e)
        finally:
            session.close()
