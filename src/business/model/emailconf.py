#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/emailconf.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-26 15:08
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME
from sqlalchemy.sql import func, text, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger


class EmailConf(BaseModel):
    # 表名
    __tablename__ = 'imp_email_pop3'
    # 表结构
    import_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    pop3_host = Column(String(50))
    pop3_port = Column(Integer)
    is_ssl = Column(String(1))
    email_user = Column(String(100))
    email_password = Column(String(50))
    import_memo = Column(String(200))
    is_auto_import = Column(String(1))
    hour_frequency = Column(Integer)
    last_import_time = Column(DATETIME)
    import_num = Column(Integer)
    last_message = Column(String(500))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    def _uuid(self):
        uid = md5('{}-{}-{}-{}'.format(self.company_id, self.email_user, self.import_id, self.__class__.__name__))
        uname = u'<{}>邮件简历搜索器'.format(self.email_user)
        task = {
            'id': uid,
            'name': uname,
            'func': 'zpb.service.mailservice.DoMailSearcher',
            'args': (self.company_id, uid, self.import_id, 0)
        }
        return task

    @classmethod
    def queryPending(cls):
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(EmailConf).filter(text("is_valid='T' and is_auto_import='T'"\
                "and (last_import_time is null or "\
                "date_add(last_import_time,interval hour_frequency day_hour)<now())")).limit(20)
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
            row = session.query(EmailConf).filter(and_(EmailConf.import_id==importid, EmailConf.is_valid=='T')).first()
            return row
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def updateImportTimeAndNumberByImportId(cls, importid, number):
        session = DBInstance.session
        try:
            stmt = update(EmailConf).where(EmailConf.import_id==importid).values(import_num=EmailConf.import_num + number, last_import_time=datetime.today())
            session.execute(stmt)
            session.commit()
            return True
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def newAndSave(cls, companyid, host, port, ssl, usercode, password):
        session = DBInstance.session
        try:
            emailconf = session.query(EmailConf).filter(and_(
                EmailConf.company_id==companyid,
                EmailConf.email_user==usercode)).first()
            if not emailconf:
                emailconf = EmailConf()
                emailconf.company_id = companyid
                emailconf.email_user = usercode
                emailconf.is_auto_import = 'T'
                emailconf.hour_frequency = '2'
                emailconf.is_valid = 'T'
                emailconf.import_num = 0
                emailconf.create_user = 'zpb'
                emailconf.create_time = datetime.today()
            else:
                emailconf.modify_user = 'zpb'
                emailconf.modify_time = datetime.today()
            emailconf.pop3_host = host
            emailconf.pop3_port = port
            emailconf.is_ssl = 'T' if ssl else 'F'
            emailconf.email_password = password
            return super(EmailConf, emailconf).save()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def unBind(cls, companyid, usercode):
        session = DBInstance.session
        try:
            emailconf = session.query(EmailConf).filter(and_(
                EmailConf.company_id==companyid,
                EmailConf.email_user==usercode)).first()
            if emailconf:
                emailconf.is_valid = 'F'
                return super(EmailConf, emailconf).save()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    def save(self):
        if self.is_valid == 'T':
            self.last_import_time = datetime.today()
        return super(EmailConf, self).save()
