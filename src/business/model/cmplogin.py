#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/zpb/business/model/cmplogin.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-15 14:30
#########################################################################


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.sql import func
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.exception import *
from zpb.conf import logger


class CmpLogin(BaseModel):
    # 表名
    __tablename__ = 'cmp_login_account'
    # 表结构
    user_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    login_email = Column(String(50))
    login_mobile = Column(String(20))
    login_pswd = Column(String(50))
    is_acitived = Column(String(1))
    is_valid = Column(String(1))
    last_login_time = Column(DATETIME)
    last_login_ip = Column(String(20))


    @classmethod
    def valid(cls, login_email, login_pswd):
        """
        账号验证
        :type login_email: str
        :param login_email: 登录用户
        :type login_pswd: str
        :param login_pswd: 登录密码
        """
        session = DBInstance.session
        try:
            try:
                row = session.query(CmpLogin).filter(CmpLogin.login_email==login_email).first()
                if row:
                    if row.login_pswd == login_pswd:
                        return (True, '{}:{}'.format(row.company_id, row.user_id))
                    else:
                        return (False, u'登录账号或密码错误!')
                else:
                    return (False, u'您输入的账号不存在,请检查后重新输入!')
            except BaseException as e:
                raise DBOperateError(currentFuncName(cls), e)
        except DBOperateError:
            return (False, u'账号验证系统异常,请稍后重试!')
        finally:
            session.close()

    def save(self):
        pass
