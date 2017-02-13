#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/jobsyncdistribute.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 15:36
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME
from sqlalchemy.sql import func, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.exception import *
from zpb.conf import logger


# 职位同步(新增/修改/暂停/重发布/删除)操作记录表
class JobSyncDistribute(BaseModel):
    # 表名
    __tablename__ = 'job_sync_distribute'
    # 表结构
    sync_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    company_id = Column(Integer)
    site_id = Column(Integer)
    sync_status = Column(Integer)
    first_sync_time = Column(DATETIME)
    last_sync_time = Column(DATETIME)
    last_refresh_time = Column(DATETIME)
    sync_direction = Column(Integer)
    third_job_code = Column(String(100))
    third_other_info = Column(String(500))
    sync_succ_num = Column(Integer)
    sync_fail_num = Column(Integer)
    error_message = Column(Text)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    @classmethod
    def new(cls, jobid, siteid, companyid):
        row = JobSyncDistribute()
        row.job_id = jobid
        row.site_id = siteid
        row.company_id = companyid
        row.sync_succ_num = 0
        row.sync_fail_num = 0
        row.sync_status = 0
        row.is_valid = 'T'
        row.create_user = 'zpb'
        row.create_time = datetime.today()
        row.first_sync_time = datetime.today()
        return row

    @classmethod
    def queryByJobIdAndCompanyIdWithSiteId(cls, jobid, companyid, siteid):
        session = DBInstance.session
        try:
            row = session.query(JobSyncDistribute).filter(and_(JobSyncDistribute.job_id == jobid,
                JobSyncDistribute.company_id == companyid, JobSyncDistribute.site_id == siteid)).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def queryValidSiteByJobIdAndCompanyId(cls, jobid, companyid):
        """
        依据职位编码与企业编码查询职位同步表中有效状态记录的所有绑定招聘平台代码
        在给所有招聘平台(site_id=0)做职位操作时,判定可操作的招聘平台
        """
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(JobSyncDistribute.site_id).filter(and_(JobSyncDistribute.job_id == jobid,
                JobSyncDistribute.company_id == companyid, JobSyncDistribute.is_valid == 'T')).all()
            for r in rows:
                ret.append(r[0])
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def queryJobIdByThirdJobCode(cls, companyid, siteid, thirdjobcode):
        session = DBInstance.session
        try:
            row = session.query(JobSyncDistribute.job_id).filter(and_(JobSyncDistribute.company_id == companyid,
                JobSyncDistribute.site_id == siteid, JobSyncDistribute.third_job_code == thirdjobcode,
                JobSyncDistribute.is_valid == 'T')).first()
            if row:
                return row.job_id
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()


def run_main():
    sites = JobSyncDistribute.queryValidSiteByJobIdAndCompanyId(2578, 66)
    print sites
    
