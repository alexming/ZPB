#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/zpb/service/web/resume.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-26 14:15
#########################################################################


# stdlib
import os
import base64
import json
# tornado
import tornado.gen
import tornado.web
# zpb
from zpb.business.model.resume import ResumeBase, AssembelResumeByJson
from zpb.utils.tools import decryptBindPasswd
from zpb.business.siteconfig import SiteConfig
from zpb.conf import Conf, logger
# suds
from suds.client import Client as SudsClient


class ResumeExportHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            session = self.get_argument('sessionid', '')
            if session:
                messages = decryptBindPasswd('zpb', session).split(':')
                if len(messages) == 2:
                    companyid = messages[0]
                    logger.debug(u'企业编码<{}>请求已下载简历'.format(companyid))
                    data = ResumeBase.queryAndExport(companyid, pagesize=5)
                    ret['status'] = 0
                    ret['count'] = len(data)
                    ret['message'] = u'简历下载成功!'
                    ret['res_resume_base'] = data
                else:
                    ret['message'] = u'非法的请求!'
            else:
                ret['message'] = u'非法的请求!'
        finally:
            self.write(json.dumps(ret))


class ResumeAckExportHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            session = self.get_argument('sessionid', '')
            resumecodes = self.get_argument('resumecodes', '')
            if session:
                if resumecodes:
                    messages = decryptBindPasswd('zpb', session).split(':')
                    if len(messages) == 2:
                        companyid = messages[0]
                        logger.info(u'企业编码<{}>确认已下载简历'.format(companyid))
                        data = ResumeBase.ackExport(companyid, resumecodes.split(','))
                        ret['status'] = 0
                        ret['message'] = u'简历状态确认成功!'
                    else:
                        ret['message'] = u'非法的请求!'
                else:
                    ret['message'] = u'未提交待确认简历编码!'
            else:
                ret['message'] = u'非法的请求!'
        finally:
            self.write(json.dumps(ret))


class ResumeAnalysisHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self):
        ret = {'status': 1, 'message': ''}
        try:
            # 提取post参数中name=file的文件元数据
            if self.request.files.has_key('file'):
                #
                try:
                    soapCli = SudsClient(Conf.YGYS['soapuri'], timeout=Conf.YGYS['timeout'])
                except BaseException as e:
                    logger.error(e)
                    ret['message'] = u'无法连接简历解析WebService服务'
                    return
                #
                try:
                    sitecode = self.get_argument('sitefromcode', '0')
                    maildate = self.get_argument('maildate', '')
                    filedata = self.request.files['file'][0]
                    filename = filedata['filename']
                    logger.info(u'解析简历文件<{}>'.format(filename))
                    # 文件扩展名
                    ext = os.path.splitext(filename)[-1]
                    if not ext: ext = '.text'
                    res = soapCli.service.TransResumeByJsonStringForFileBase64(
                        Conf.YGYS['username'], Conf.YGYS['password'],
                        base64.b64encode(filedata['body']),
                        ext
                    )
                    if res:
                        js = json.loads(res)
                        if js['Type'] == 0:
                            ret['message'] = js['Name']
                        elif not js['Name']:
                            ret['message'] = u'非完整简历'
                        else:
                            js['companyid'] = 0
                            js['jobid'] = 0
                            js['source'] = 0
                            js['apply_job_id'] = 0
                            js['siteid'] = sitecode
                            js['apply_time'] = maildate
                            js['websiteresumeid'] = js['WebSiteResumeID']
                            js['matching'] = 0
                            (res, message, new) = AssembelResumeByJson(js)
                            if res:
                                data = ResumeBase.queryAndExportByResumeCode(message)
                                ret['status'] = 0
                                ret['count'] = 1
                                ret['message'] = u'简历文件解析成功!'
                                ret['res_resume_base'] = data
                            else:
                                ret['message'] = message
                    else:
                        ret['message'] = u'解析结果空白!'
                except BaseException as e:
                    logger.error(e)
                    ret['message'] = u'简历解析内部服务错误!'
            else:
                ret['message'] = u'未上传需要解析的简历文件!'
        finally:
            if ret['status'] == 1:
                logger.error(ret['message'])
            self.write(json.dumps(ret))
