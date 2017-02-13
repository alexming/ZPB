#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: basejob.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-17 10:52
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATE, DATETIME
from sqlalchemy.sql import func, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.model.jobsyncdistribute import JobSyncDistribute
from zpb.business.model.jobmemo import JobMemo
from zpb.business.siteconfig import SiteConfig
from zpb.exception import *
from zpb.conf import logger


class BaseJob(BaseModel):
    # 表名
    __tablename__ = 'job_base_info'
    # 表结构
    job_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    from_site_id = Column(Integer)
    from_site_code = Column(String(50))
    job_name = Column(String(200))
    job_type_level_1 = Column(Integer)
    job_type_level_2 = Column(Integer)
    job_type_level_3 = Column(Integer)
    job_mode = Column(Integer)
    location_province_id = Column(Integer)
    location_city_id = Column(Integer)
    location_area_id = Column(Integer)
    location_detail = Column(String(500))
    salary_id = Column(Integer)
    recruit_num = Column(Integer)
    lmt_edu_id = Column(Integer)
    lmt_age_min = Column(Integer)
    lmt_age_max = Column(Integer)
    lmt_work = Column(Integer)
    lmt_major_ids = Column(String(50))
    lmt_sex_ids = Column(String(50))
    lmt_lang_ids = Column(String(50))
    receiver_emails = Column(String(200))
    work_responsibility = Column(String(2000))
    work_qualification = Column(String(2000))
    search_keywords = Column(String(200))
    job_status = Column(Integer)
    start_date = Column(DATE)
    end_date = Column(DATE)
    publish_date = Column(DATE)
    is_auto_refresh = Column(String(1))
    last_refresh_time = Column(DATETIME)
    last_sync_time = Column(DATETIME)
    is_valid = Column(String(1))
    lmt_salary_min = Column(Integer)
    lmt_salary_max = Column(Integer)
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    @classmethod
    def queryOne(cls, jobid):
        """
        依据职位编码查找职位信息
        :type jobid: int
        :param jobid: 职位编码
        """
        session = DBInstance.session
        try:
            row = session.query(BaseJob).filter(BaseJob.job_id == jobid).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def queryTagsByJobId(cls, jobid):
        """
        依据职位编码查找职位标签
        :type jobid: int
        :param jobid: 职位编码
        """
        session = DBInstance.session
        try:
            rows = session.query(SysTag.tag_id, SysTag.tag_name).join(JobProperty,
                JobProperty.tag_id == SysTag.tag_id).filter(
                and_(JobProperty.job_id == jobid,
                    SysTag.is_valid == 'T')).all()
            return rows
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def updateSyncTimeByJobId(cls, jobid):
        """
        职位同步到招聘平台后更新最后同步时间
        :type jobid: int
        :param jobid: 职位编码
        """
        session = DBInstance.session
        try:
            stmt = update(BaseJob).where(BaseJob.job_id==jobid).values(last_sync_time=datetime.today())
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()                

    @classmethod
    def updateRefreshTimeByJobId(cls, jobid):
        """
        职位刷新到招聘平台后更新最后刷新时间
        :type jobid: int
        :param jobid: 职位编码
        """
        session = DBInstance.session
        try:
            stmt = update(BaseJob).where(BaseJob.job_id==jobid).values(last_refresh_time=datetime.today())
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()                

    def save(self):
        """
        职位保存或更新,同时更新职位属性与职位标签
        """
        session = DBInstance.session
        try:
            session.add(self)
            if self.job_id:
                session.query(JobProperty).filter(JobProperty.job_id == self.job_id).delete()
            else:
                session.flush()
                # 增加同步分发记录
                dist = JobSyncDistribute.new(self.job_id, self.from_site_id, self.company_id)
                dist.sync_status = 20
                dist.third_job_code = self.from_site_code
                dist.sync_succ_num = 1
                session.add(dist)
                # 增加职位新增日志
                jmm = JobMemo(self.job_id)
                jmm.memo_content = u'来源于[{}]'.format(SiteConfig.getSiteNameById(self.from_site_id))
                session.add(jmm)
            # 增加标签
            for item in self.tags:
                tag = SysTag.queryByTagNameWithCompanyid(item, self.company_id)
                if not tag:
                    tag = SysTag.new(item, self.company_id)
                    session.add(tag)
                    session.flush()
                prop = JobProperty.new(self.job_id, tag.tag_id, 9999)
                session.add(prop)
            for tagid in self.props:
                prop = JobProperty.new(self.job_id, tagid)
                session.add(prop)
            session.commit()
            return True
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(self), e)
        finally:
            session.close()


class JobProperty(BaseModel):
    # 表名
    __tablename__ = 'job_base_property'
    # 表结构
    property_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    tag_type = Column(Integer)
    tag_id = Column(Integer)
    sort_value = Column(Integer)
    is_valid = Column(String(1))

    @classmethod
    def new(cls, jobid, tagid, sort=0):
        prop = JobProperty()
        prop.job_id = jobid
        prop.tag_type = 1
        prop.tag_id = tagid
        prop.sort_value = sort
        prop.is_valid = 'T'
        return prop


# 职位标签
class SysTag(BaseModel):
    # 表名
    __tablename__ = 'sys_tag'
    # 表结构
    tag_id = Column(Integer, primary_key=True)
    tag_type = Column(Integer)
    tag_name = Column(String(100))
    is_valid = Column(String(1))
    sort_value = Column(Integer)
    parent_id = Column(Integer)
    link_type_id = Column(Integer)

    @classmethod
    def new(cls, tagname, companyid):
        tag = SysTag()
        tag.tag_type = 1
        tag.tag_name = tagname
        tag.is_valid = 'T'
        tag.sort_value = 9999
        tag.parent_id = 0
        tag.link_type_id = companyid
        return tag

    @classmethod
    def queryByTagNameWithCompanyid(cls, tagname, companyid):
        """
        查找企业的职位标签
        :type tagname: str
        :param tagname: 标签名
        :type companyid: int
        :param companyid: 企业编码
        """
        session = DBInstance.session
        try:
            row = session.query(SysTag).filter(and_(SysTag.tag_name == tagname,
                SysTag.link_type_id == companyid)).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()


if __name__ == '__main__':
    job = BaseJob.queryOne(91)
    print job.job_id
