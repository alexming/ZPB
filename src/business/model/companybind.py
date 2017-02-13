#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/model/companybind.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 16:46
#########################################################################


# stdlib
from datetime import date, datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME
from sqlalchemy.sql import func, and_
# zpb
from zpb.exception import *
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.utils.tools import decryptBindPasswd
from zpb.conf import logger


class Bind(BaseModel):
    # 表名
    __tablename__ = 'cmp_bind_account'
    # 表结构
    bind_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    site_id = Column(Integer)
    member_name = Column(String(50))
    login_name = Column(String(50))
    login_pswd = Column(String(50))
    other_name = Column(String(50))
    check_status = Column(Integer)
    login_result = Column(String(200))
    last_check_time = Column(DATETIME)
    last_login_time = Column(DATETIME)
    last_oper_time = Column(DATETIME)
    last_succ_time = Column(DATETIME)
    is_valid = Column(String(1))
    max_job_num = Column(Integer)
    leased_job_num = Column(Integer)
    max_down_num = Column(Integer)
    job_down_num = Column(Integer)
    is_auto_import = Column(String(1))
    auto_import_to_job_id = Column(Integer)
    last_import_time = Column(DATETIME)
    is_procing = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    def decryptedPasswd(self):
        return decryptBindPasswd(self.login_name, self.login_pswd)

    # 加载is_valid=T的所有数据
    @classmethod
    def loadValidAll(cls):
        session = DBInstance.session
        try:
            ret = session.query(Bind).filter(Bind.is_valid == 'T').all()
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def loadValidSiteByCompanyId(cls, companyid):
        """
        仅加载待验证与验证通过之绑定账号
        :type companyid: int
        :param companyid: 企业编码
        """
        session = DBInstance.session
        try:
            ret = []
            rows = session.query(Bind.site_id).filter(and_(Bind.company_id==companyid, Bind.is_valid=='T')).all()
            for r in rows:
                ret.append(r[0])
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()        

    # 加载单项数据
    @classmethod
    def loadValidOne(cls, companyid, siteid):
        """
        依据企业编码与平台编码加载有效绑定信息
        :type companyid: int
        :param companyid: 企业编码
        :type siteid: int
        :param siteid: 平台编码
        """
        session = DBInstance.session
        try:
            ret = session.query(Bind).filter(and_(Bind.company_id==companyid, Bind.site_id==siteid, Bind.is_valid=='T')).first()
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    # 依据关键字检索
    @classmethod
    def queryOne(cls, companyid, siteid):
        """
        依据企业编码与平台编码加载绑定信息
        :type companyid: int
        :param companyid: 企业编码
        :type siteid: int
        :param siteid: 平台编码
        """
        session = DBInstance.session
        try:
            ret = session.query(Bind).filter(and_(Bind.company_id==companyid, Bind.site_id==siteid)).first()
            return ret
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def newAndSave(cls, companyid, siteid, membername, loginname, loginpswd):
        """
        招聘平台账号绑定
        """
        session = DBInstance.session
        try:
            bind = session.query(Bind).filter(
                    and_(
                        Bind.company_id==companyid,
                        Bind.site_id==siteid,
                        Bind.member_name==membername,
                        Bind.login_name==loginname)).first()
            if not bind:
                bind = Bind()
                bind.company_id = companyid
                bind.site_id = siteid
                bind.member_name = membername
                bind.login_name = loginname
                bind.is_valid = 'T'
                bind.is_auto_import = 'T'
            elif bind.is_valid == 'F':
                bind.is_valid = 'T'
            else:
                return
            bind.login_pswd = loginpswd
            bind.check_status = 50
            bind.login_result = u'登录成功'
            bind.last_check_time = datetime.today()
            bind.last_login_time = datetime.today()
            bind.last_succ_time = datetime.today()
            bind.create_time = datetime.today()
            bind.create_user = 'zpb'
            return bind.save()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    @classmethod
    def unBind(cls, companyid, siteid, membername, loginname):
        """
        招聘平台账号解除绑定
        """
        session = DBInstance.session
        try:
            bind = session.query(Bind).filter(
                    and_(
                        Bind.company_id==companyid,
                        Bind.site_id==siteid,
                        Bind.member_name==membername,
                        Bind.login_name==loginname)).first()
            if bind:
                bind.is_valid = 'F'
                return bind.save()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()


    # 依据key查找简历最后导入时间
    # 返回日期类型或返回初始日期
    def queryImportTime(self):
        if self.last_import_time:
            return datetime.date(self.last_import_time)
        return date(1900, 1, 1)

    @classmethod
    def updateBindImportTimeByCompanyIdAndSiteId(cls, companyid, siteid):
        session = DBInstance.session
        try:
            kwargs = {
                'last_import_time': datetime.today(),
                'is_procing': 'F',
                'modify_user': 'zpb',
                'modify_time': datetime.today()
            }
            stmt = update(Bind).where(and_(Bind.company_id==companyid, Bind.site_id==siteid)).values(**kwargs)
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    # 登录成功
    def saveSucc(self):
        self.check_status = 50
        self.login_result = u'登录成功'
        self.last_succ_time = datetime.today()
        self.save()

    # 登录失败
    def saveFail(self, message):
        self.check_status = 10
        self.login_result = message
        self.save()

    # 保存
    def save(self):
        self.last_oper_time = datetime.today()
        session = DBInstance.session
        try:
            kwargs = {
                c.name: getattr(self, c.name) for c in self.__table__.columns \
                if c.name not in ['bind_id','company_id','site_id','member_name',
                    'login_name','login_pswd','other_name','is_auto_import','auto_import_to_job_id','last_import_time','is_procing']
            }
            stmt = update(Bind).where(Bind.bind_id==self.bind_id).values(**kwargs)
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(self), e)
        finally:
            session.close()

if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    #
    bind = Bind.loadValidOne(28, 3)
    if bind:
        bind.updateImportTime()
        print bind.queryImportTime()
