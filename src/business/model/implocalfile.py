#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: implocalfile.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-24 17:26
#########################################################################


# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.sql import func, and_
from sqlalchemy.dialects.mysql import TIMESTAMP, BLOB
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger


class ImpLocalFile(BaseModel):
    # 表名
    __tablename__ = 'imp_local_file'
    # 表结构
    import_id = Column(Integer, primary_key=True)
    input_type = Column(Integer)
    user_file_name = Column(String(200))
    local_file_name = Column(String(200))
    file_content = Column(BLOB)
    input_content = Column(Text)
    # 本系统职位ID
    resume_code = Column(String(32))
    company_id = Column(Integer)
    from_site_id = Column(Integer)
    apply_job_id = Column(Integer)
    proc_status = Column(Integer)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    def _uuid(self):
        uid = md5('{}-{}-{}'.format(self.company_id, self.import_id, self.__class__.__name__))
        uname = u'手动简历解析'
        task = {
            'id': uid,
            'name': uname,
            'func': 'zpb.service.resumeservice.ParseLocalResume',
            'args': (self.company_id, uid, self.import_id)
        }
        return task

    # query待处理任务
    @classmethod
    def queryPending(cls):
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(ImpLocalFile).filter(and_(ImpLocalFile.proc_status==1, ImpLocalFile.is_valid=='T')).limit(20)
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
    def queryByImportId(cls, importid):
        session = DBInstance.session
        try:
            row = session.query(ImpLocalFile).filter(and_(ImpLocalFile.import_id==importid, ImpLocalFile.is_valid=='T')).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()
