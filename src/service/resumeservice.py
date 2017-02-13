#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/resumeservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:11
#########################################################################


# stdlib
import os
import json
import base64
import time
from datetime import datetime
# suds
from suds.client import Client as SudsClient
# zpb
from zpb.cache.rediscache import ResumeRedisCli
from zpb.core.BaseHandlerManager import HANDLE_STATUS
from zpb.business.model.resume import ResumeBase, AssembelResumeByJson
from zpb.business.model.imphistory import ImpHistory
from zpb.business.model.implocalfile import ImpLocalFile
from zpb.business.model.imphistoryresume import ImpHistoryResume
from zpb.business.rd2.RD2HandlerManager import RD2HandlerManager
from zpb.business.wyjob.WYJobHandlerManager import WYJobHandlerManager
from zpb.business.lag.LAGHandlerManager import LAGHandlerManager
from zpb.business.cjol.CJOLHandlerManager import CJOLHandlerManager
from zpb.business.siteconfig import SiteConfig
from zpb.utils.tools import datetime2str
from zpb.exception import *
from zpb.conf import Conf, logger
# dtc
import dtc


def DownResume(checkstatservice, **kwargs):
    data = kwargs.copy()
    taskid = data['taskid']
    companyid = data['companyid']
    siteid = data['siteid']
    username = data['username']
    resumeid = data['resumeid']
    postdate = data['postdate']
    # 强制刷新简历(用於付费简历下载)
    force = data.get('force', False)
    #
    sitename = SiteConfig.getSiteNameById(siteid)
    importkey = Conf.RESUME_IMPORT_HKEY % taskid
    try:
        try:
            # 简历更新度验证(投递日期)
            if not force:
                if not ResumeBase.isNew(companyid, siteid, resumeid, postdate):
                    # 未更新的简历将会被忽略
                    logger.debug(u'<{}>简历<{}, {}>重复下载!'.format(sitename, username, resumeid))
                    ResumeRedisCli.hincrby(importkey, 'ignore')
                    return
            try:
                handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
                logger.info(u'正在下载<{}>简历<{}, {}>'.format(sitename, username, resumeid))
                handler.resume_down(data)
                if handler.status == HANDLE_STATUS.SUCCESS:
                    logger.info(handler.message)
                    dtc.async('zpb.service.resumeservice.ParseResume', checkstatservice, **data),
                elif handler.status == HANDLE_STATUS.AGAIN:
                    logger.info(u'<{}>简历<{}, {}>需要重新下载'.format(sitename, username, resumeid))
                    data['retry'] = data.get('retry', 0) + 1
                    dtc.async('zpb.service.resumeservice.DownResume', checkstatservice, **data),
                else:
                    ResumeRedisCli.hincrby(importkey, 'failure')
                    logger.error(handler.message)
            except BaseError as e:
                pass
        except BaseException as e:
            dtc.async('zpb.service.resumeservice.DownResume', checkstatservice, **data),
            logger.error(u'<{}>简历<{}, {}>下载异常,原因:{}'.format(sitename, username, resumeid, e))
    finally:
        dtc.async(checkstatservice, taskid)


def ParseResume(checkstatservice, **kwargs):
    data = kwargs.copy()
    taskid = data['taskid']
    companyid = data['companyid']
    siteid = data['siteid']
    jobid = data['jobid']
    source = data.get('source', 0)
    username = data['username']
    resumeid = data['resumeid']
    postdate = data['postdate']
    # 强制刷新简历(用於付费简历下载)
    force = data.get('force', False)
    # 简历与职位匹配度
    matching = data.get('matching', 0)
    #
    sitename = SiteConfig.getSiteNameById(siteid)
    importkey = Conf.RESUME_IMPORT_HKEY % taskid
    try:
        try:
            # 开始解析
            logger.info(u'正在解析<{}>简历<{}>, <{}>'.format(sitename, username, resumeid))
            filepath = data['filepath']
            if os.path.isfile(filepath):
                # 用於文件备份
                # basename = os.path.basename(filepath)
                # dirname = os.path.dirname(filepath)
                ext = os.path.splitext(filepath)[-1]
                ret = _doResumeParseByFile(base64.b64encode(open(filepath, 'rb').read()), ext)
                if ret:
                    js = json.loads(ret)
                    if js['Type'] == 0:
                        ResumeRedisCli.hincrby(importkey, 'failure')
                        logger.error(u'<{}>简历<{}, {}>解析失败,原因:{}'.format(sitename, username, resumeid, js['Name']))
                        return
                    if not js['Name']:
                        ResumeRedisCli.hincrby(importkey, 'failure')
                        logger.error(u'<{}>简历<{}, {}>解析失败,原因:文件不是一份完整的简历!'.format(sitename, username, resumeid))
                        return
                    #
                    js['companyid'] = companyid
                    js['siteid'] = siteid
                    if siteid == 4:
                        if resumeid:
                            js['websiteresumeid'] = resumeid
                        elif js['WebSiteResumeID']:
                            js['websiteresumeid'] = js['WebSiteResumeID'].replace('J', '')
                        else:
                            js['websiteresumeid'] = ''
                    else:
                        js['websiteresumeid'] = resumeid if resumeid else js['WebSiteResumeID']
                    js['jobid'] = jobid
                    js['source'] = source
                    js['force'] = force
                    js['matching'] = matching
                    js['apply_job_id'] = 0
                    js['apply_time'] = postdate
                    (res, message, new) = AssembelResumeByJson(js)
                    if res:
                        logger.info(u'<{}>简历<{}, {}>解析成功!'.format(sitename, username, resumeid))
                        # 简历存储
                        imphistoryid = ResumeRedisCli.hget(importkey, 'imphistoryid')
                        # 简历刷新,不必新增
                        if new:
                            ResumeRedisCli.hincrby(importkey, 'success')
                            # 保存简历历史详细记录
                            if ImpHistoryResume.newAndSave(imphistoryid, companyid, message):
                                ImpHistory.incSuccessByHistoryId(imphistoryid)
                            else:
                                logger.error(u'<{}>简历<{}, {}>历史详情保存异常!'.format(sitename, username, resumeid))
                        else:
                            ResumeRedisCli.hincrby(importkey, 'ignore')
                    else:
                        ResumeRedisCli.hincrby(importkey, 'failure')
                        logger.error(u'<{}>简历<{}, {}>解析失败,原因:{}'.format(sitename, username, resumeid, message))
                else:
                    ResumeRedisCli.hincrby(importkey, 'failure')
                    message = u'简历服务器解析简历返回结果异常,<{}><{}, {}>'.format(sitename, username, resumeid)
                    logger.error(message)
            else:
                ResumeRedisCli.hincrby(importkey, 'failure')
                message = u'简历解析失败,磁盘文件<{}>不存在'.format(sitename, username, resumeid)
                logger.error(message)
        except BaseException as e:
            dtc.async('zpb.service.resumeservice.ParseResume', checkstatservice, **data),
            logger.error(u'简历解析服务异常,message:{}'.format(e))
    finally:
        dtc.async(checkstatservice, taskid)


def _doResumeParseByFile(b64content, ext):
    try:
        soapCli = SudsClient(Conf.YGYS['soapuri'], timeout=Conf.YGYS['timeout'])
    except BaseException as e:
        logger.error(u'无法连接简历解析SOAP服务,uri:<{}>,e:<{}>'.format(Conf.YGYS['soapuri'], e))
        return
    try:
        return soapCli.service.TransResumeByJsonStringForFileBase64(
            Conf.YGYS['username'], Conf.YGYS['password'],
            b64content, ext
        )
    except BaseException as e:
        logger.error(u'简历文件解析失败,原因:{}'.format(e))


def _doResumeParseByString(content):
    try:
        soapCli = SudsClient(Conf.YGYS['soapuri'], timeout=Conf.YGYS['timeout'])
    except BaseException as e:
        logger.error(u'无法连接简历解析SOAP服务,uri:<{}>,e:<{}>'.format(Conf.YGYS['soapuri'], e))
        return
    try:
        return soapCli.service.TransResumeByJsonString(
            Conf.YGYS['username'], Conf.YGYS['password'],
            content
        )
    except BaseException as e:
        logger.error(u'简历文本解析失败,原因:{}'.format(e))


# 手工解析简历任务分发
def ParseLocalResume(companyid, taskid, importid):
    row = ImpLocalFile.queryByImportId(importid)
    if row:
        sitename = SiteConfig.getSiteNameById(row.from_site_id)
        logger.info(u'开始解析<{}>的本地简历'.format(sitename))
        row.proc_status = 10
        imp = ImpHistory.new(row.company_id, row.from_site_id, row.import_id, row.input_type)
        if row.input_type == 1:
            imp.src_memo = row.user_file_name
        if not imp.save():
            return
        # 异常信息提示
        log_msg = u''
        try:
            if row.input_type == 1:
                log_msg = u'简历文件<{}>解析'.format(row.user_file_name)
                # 数据库存储类型为hex编码,此处进行解码
                content = base64.b64encode(row.file_content)
                ext = os.path.splitext(row.user_file_name)[-1]
                ret = _doResumeParseByFile(content, ext)
            else:
                log_msg = u'简历文本解析'
                content = row.input_content
                ret = _doResumeParseByString(content)
            if ret:
                js = json.loads(ret)
                if js['Type'] > 0 and js['Name']:
                    js['companyid'] = row.company_id
                    js['siteid'] = row.from_site_id
                    if js['WebSiteResumeID']:
                        js['websiteresumeid'] = js['WebSiteResumeID']
                    else:
                        js['websiteresumeid'] = 'Local{0}'.format(row.import_id)
                    js['jobid'] = ''
                    js['source'] = 0
                    js['apply_job_id'] = row.apply_job_id
                    js['apply_time'] = datetime2str(datetime.today())
                    (res, message, new) = AssembelResumeByJson(js)
                    if res:
                        row.resume_code = message
                        row.proc_status = 20
                        # 保存简历历史详细记录
                        if ImpHistoryResume.newAndSave(imp.history_id, row.company_id, message):
                            # 保存简历历史记录
                            imp.succ_num = 1
                            imp.proc_status = 1
                            message = u'{}成功'.format(log_msg)
                            logger.info(message)
                        else:
                            message = u'数据存储失败'
                            logger.error('{}失败,{}'.format(log_msg, message))
                            imp.fail_num = 1
                            imp.proc_status = 2
                            imp.fail_reason = message
                    else:
                        logger.error('{}失败,{}'.format(log_msg, message))
                        imp.fail_num = 1
                        imp.proc_status = 2
                        imp.fail_reason = message
                else:
                    message = u'简历内容为空'
                    logger.error('{}失败,{}'.format(log_msg, message))
                    imp.fail_num = 1
                    imp.proc_status = 2
                    imp.fail_reason = message
            else:
                message = u'解析结果为空'
                logger.error('{}失败,{}'.format(log_msg, message))
                imp.fail_num = 1
                imp.proc_status = 2
                imp.fail_reason = message
        except BaseException as e:
            message = u'{}异常,原因:{}'.format(log_msg, e)
            logger.error(message)
            imp.fail_num = 1
            imp.proc_status = 2
            imp.fail_reason = u'内部服务错误!'
        # 历史结果存储
        row.save()
        imp.end_time = datetime.today()
        imp.save()
