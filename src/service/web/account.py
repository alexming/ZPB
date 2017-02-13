#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/zpb/service/web/account.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-26 14:09
#########################################################################


# stdlib
import json
from urllib import unquote
# tornado
import tornado.gen
import tornado.web
# zpb
from zpb.business.model.cmplogin import CmpLogin
from zpb.business.model.companybind import Bind
from zpb.business.model.dbtask import DBTask
from zpb.core.BaseHandlerManager import HANDLE_STATUS
from zpb.utils.tools import str2int, encryptBindPasswd, decryptBindPasswd
from zpb.business.siteconfig import SiteConfig
from zpb.exception import *
from zpb.conf import logger


class AccountVerifyHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self):
        ret = {'status': 1, 'message': ''}
        try:
            company_id = str2int(self.get_argument('company_id', ''))
            site_id = str2int(self.get_argument('site_id', ''))
            if not (company_id and site_id):
                raise InvalidParamError(u'提交参数错误!')
            try:
                handler = SiteConfig.GetTaskHandler(company_id, site_id, None)
                try:
                    # 开始登陆
                    handler.login()
                    # 反馈结果
                    if handler.status == HANDLE_STATUS.SUCCESS:
                        ret['status'] = 100
                        # 增加201轮询指令
                        DBTask.newSchedule(company_id, site_id)
                    else:
                        ret['message'] = handler.message
                except BaseException as e:
                    raise UnHandleRuntimeError(e)
                    logger.error(u'Web服务请求异常,原因:{}'.format(e))
            except BaseError as e:
                ret['message'] = e.message
        finally:
            logger.debug(ret)
            self.write(json.dumps(ret))


class LoginHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            usercode = self.get_argument('usercode', '')
            password = self.get_argument('password', '')
            if usercode and password:
                (res, message) = CmpLogin.valid(usercode, password)
                if res:
                    message = encryptBindPasswd('zpb', message)
                    if message:
                        ret['status'] = 0
                        ret['data'] = message
                        ret['message'] = u'登录成功!'
                    else:
                        ret['message'] = u'内部服务错误!'
                else:
                    ret['message'] = message
            else:
                ret['message'] = u'用户名或密码不能为空!'
        finally:
            logger.debug(ret)
            self.write(json.dumps(ret))


class AccountBindHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            site_id = str2int(self.get_argument('site_id', ''))
            member_name = self.get_argument('member_name', '')
            login_name = self.get_argument('login_name', '')
            login_pswd = self.get_argument('login_pswd', '')
            # 中文解码
            member_name = unquote(member_name.encode('gbk'))
            login_name = unquote(login_name.encode('gbk'))
            #
            session = self.get_argument('sessionid', '')
            try:
                if not session:
                    raise InvalidParamError(u'非法的请求!')
                if not (site_id and login_name and login_pswd):
                    raise InvalidParamError(u'非法的请求!')
                messages = decryptBindPasswd('zpb', session).split(':')
                if len(messages) != 2:
                    raise InvalidParamError(u'非法的请求!')
                companyid = messages[0]
                handler = SiteConfig.GetTaskHandler(0, site_id, None)
                # 登录密码解密
                password = decryptBindPasswd(login_name, login_pswd)
                if not password:
                    raise InvalidParamError(u'登录账号或密码错误!')
                try:
                    # 开始登陆
                    (res, message) = handler.innerlogin(member_name, login_name, password)
                    # 反馈结果
                    if res:
                        if Bind.newAndSave(companyid, site_id, member_name, login_name, login_pswd):
                            ret['status'] = 0
                            ret['message'] = u'账号绑定成功!'
                        else:
                            ret['status'] = 0
                            ret['message'] = u'账号已绑定!'
                        DBTask.newSchedule(companyid, site_id)
                    else:
                        ret['message'] = message
                except BaseException as e:
                    raise UnHandleRuntimeError(e)
                    logger.error(u'Web服务请求异常,原因:{}'.format(e))
            except BaseError as e:
                ret['message'] = e.message
        finally:
            logger.debug(ret)
            self.write(json.dumps(ret))


class AccountUnbindHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            site_id = str2int(self.get_argument('site_id', ''))
            member_name = self.get_argument('member_name', '')
            login_name = self.get_argument('login_name', '')
            session = self.get_argument('sessionid', '')
            # 中文解码
            member_name = unquote(member_name.encode('gbk'))
            login_name = unquote(login_name.encode('gbk'))
            #
            if session:
                if site_id and login_name:
                    messages = decryptBindPasswd('zpb', session).split(':')
                    if len(messages) == 2:
                        companyid = messages[0]
                        if Bind.unBind(companyid, site_id, member_name, login_name):
                            ret['status'] = 0
                            ret['message'] = u'账号解绑成功!'
                            DBTask.newSchedule(companyid, site_id, False)
                        else:
                            ret['message'] = u'账号解绑失败,账号尚未绑定!'
                    else:
                        ret['message'] = u'非法的请求!'
                else:
                    ret['message'] = u'非法的请求,提交参数不全!'
            else:
                ret['message'] = u'非法的请求!'
        finally:
            logger.debug(ret)
            self.write(json.dumps(ret))
