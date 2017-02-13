#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/zpb/service/web/email.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-26 11:45
#########################################################################


# stdlib
import json
# tornado
import tornado.gen
import tornado.web
# zpb
from zpb.business.model.emailconf import EmailConf
from zpb.utils.tools import decryptBindPasswd
from zpb.utils.tools import str2int
from zpb.conf import logger


class EmailBindHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            email_host = self.get_argument('host', '')
            email_port = str2int(self.get_argument('port', '110'))
            user_code = self.get_argument('usercode', '')
            user_pswd = self.get_argument('password', '')
            ssl = self.get_argument('ssl', False)
            session = self.get_argument('sessionid', '')
            if email_port == 0:
                email_port = 110
            if session:
                if email_host and email_port and user_code and user_pswd:
                    messages = decryptBindPasswd('zpb', session).split(':')
                    if len(messages) == 2:
                        companyid = messages[0]
                        res = EmailConf.newAndSave(companyid, email_host, email_port, ssl, user_code, user_pswd)
                        # 反馈结果
                        if res:
                            ret['status'] = 0
                            ret['message'] = u'账号绑定成功!'
                        else:
                            ret['message'] = u'内部服务错误!'
                    else:
                        ret['message'] = u'非法的请求!'
                else:
                    ret['message'] = u'非法的请求,提交参数不全!'
            else:
                ret['message'] = u'非法的请求!'
        finally:
            logger.debug(ret)
            self.write(json.dumps(ret))


class EmailUnbindHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            user_code = self.get_argument('usercode', '')
            session = self.get_argument('sessionid', '')
            #
            if session:
                if user_code:
                    messages = decryptBindPasswd('zpb', session).split(':')
                    if len(messages) == 2:
                        companyid = messages[0]
                        if EmailConf.unBind(companyid, user_code):
                            ret['status'] = 0
                            ret['message'] = u'账号解绑成功!'
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
