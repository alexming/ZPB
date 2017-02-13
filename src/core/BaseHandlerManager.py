#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/BaseHandlerManager.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 16:58
#########################################################################


# stdlib
import os
import abc
import json
from time import sleep
from urlparse import urlparse
from datetime import datetime
# zpb
from zpb.cache.rediscache import ResumeRedisCli
from zpb.core.HttpHandler import HttpHandler
from zpb.utils.tools import enum, md5, input_field_value, substring, makedirs
from zpb.utils.dama2 import Dama2
from zpb.core.MetaRequest import XRequest, HTTP_STATUS
from zpb.business.model.imphistory import ImpHistory
from zpb.exception import *
from zpb.conf import Conf, logger
# dtc
import dtc

HANDLE_STATUS = enum(FAILURE=-10000, AGAIN=-1000, SUCCESS=1)


class HandlerManagerError(TypeError):
    pass


class BaseHandlerManager(object):

    __metaclass__ = abc.ABCMeta

    MAXLOGINTIMES = 5

    # 初始化
    def __init__(self, authRedirect=None):
        self._authRedirect = authRedirect
        self._httpClient = HttpHandler(self.sessionkey)
        self._httpClient.manager = self
        self._httpClient.authCallBack = self.authCallBack
        #
        self.storeDir = ''
        #
        self.status = HANDLE_STATUS.FAILURE
        # 重登陆次数
        self.reloginTimes = 0
        self.message = ''

    # 登录
    def login(self):
        self.status = HANDLE_STATUS.FAILURE
        self.reloginTimes = 0
        self._httpClient.clearCookie()
        self.bind.last_login_time = datetime.today()
        (ret, message) = self.innerlogin(self.bind.member_name, self.bind.login_name, self.bind.decryptedPasswd())
        if ret:
            self.reloginTimes = 0
            self._httpClient.saveCookie()
            self.status = HANDLE_STATUS.SUCCESS
            self.message = u'<{}>-<{}>登录成功'.format(self.name, self.bind.login_name)
            self.bind.saveSucc()
        else:
            self.status = HANDLE_STATUS.FAILURE
            self.message = message
            self.bind.saveFail(message)

    # 登录
    def innerlogin(self, membername, loginname, loginpwd):
        pass

    # 网站基础信息采集
    def info(self):
        pass

    # 职位导入
    def position_import(self):
        pass

    # 新增职位
    def position_add(self, jobid):
        pass

    # 职位修改
    def position_modify(self, jobid):
        pass

    # 职位刷新
    def position_refresh(self, thirdjobcode):
        pass

    # 删除职位
    def position_delete(self, thirdjobcode):
        pass

    # 职位再发布
    def position_issue(self, thirdjobcode):
        pass

    # 职位恢复
    def position_renew(self, thirdjobcode):
        pass

    # 职位暂停
    def position_pause(self, thirdjobcode):
        pass

    # 职位停止
    def position_stop(self, thirdjobcode):
        pass

    # 简历导入
    def resume_import(self, importid):
        # 历史记录初始化
        imp = ImpHistory.new(self.bind.company_id, self.bind.site_id, importid)
        imp.save()
        # 缓存记录初始化
        key = Conf.RESUME_IMPORT_HKEY % self.taskid
        ResumeRedisCli.hmset(
            key,
            {
                'total': 0, 'grab': 0, 'success': 0,
                'ignore': 0, 'failure': 0, 'finish': 0,
                'siteid': self.bind.site_id, # 来源招聘平台
                'importid': importid,  # 来源id,用於追溯
                'companyid': self.bind.company_id,
                'imphistoryid': imp.history_id  # 后续存储imp_history_resume时使用
            }
        )
        # 设置数据过期时间
        ResumeRedisCli.expire(key, 60 * 60 * 24)

    # 简历下载分布式请求
    def async_resume_import(self, resume):
        resume['uid'] = md5('{companyid}-{siteid}-{resumeid}'.format(**resume))
        ResumeRedisCli.hincrby(Conf.RESUME_IMPORT_HKEY % self.taskid, 'total')
        ret, _ = dtc.async('zpb.service.resumeservice.DownResume', 'zpb.service.stateservice.CheckResumeImportStat', **resume)
        if not ret:
            ResumeRedisCli.hincrby(Conf.RESUME_IMPORT_HKEY % self.taskid, 'ignore')

    # 简历搜索器
    def resume_search(self, searcherid):
        pass

    # 付费简历下载
    def resume_searcher_down(self, resumeid, syncid, companyid=None):
        pass

    # 打码公用接口
    def damaByUrl(self, dtype, url):
        parseurl = urlparse(url)
        fmturl = parseurl.scheme + '://' + parseurl.hostname + parseurl.path
        if self.bind:
            damakey = 'dama:{}:{}:{}:{}'.format(self.bind.company_id, self.bind.site_id, self.bind.login_name, fmturl)
        else:
            damakey = 'dama:{}'.format(fmturl)
        ResumeRedisCli.set(damakey, url)
        try:
            logger.info(u'<{}>进行验证码打码'.format(self.name))
            ret = Dama2().d2Url(dtype, url)
            logger.info(u'<{}>验证码打码完成'.format(self.name))
            return ret['verify']
        finally:
            ResumeRedisCli.delete(damakey)

    # 打码公用接口
    def damaByUrlImage(self, dtype, url):
        parseurl = urlparse(url)
        fmturl = parseurl.scheme + '://' + parseurl.hostname + parseurl.path
        if self.bind:
            damakey = 'dama:{}:{}:{}:{}'.format(self.bind.company_id, self.bind.site_id, self.bind.login_name, fmturl)
        else:
            damakey = 'dama:{}'.format(fmturl)
        ResumeRedisCli.set(damakey, url)
        try:
            logger.info(u'<{}>进行验证码打码'.format(self.name))
            response = self._httpClient.call(url, authentication=False)
            ret = Dama2().d2File(dtype, response.data)
            logger.info(u'<{}>验证码打码完成'.format(self.name))
            return ret['verify']
        finally:
            ResumeRedisCli.delete(damakey)

    # 从队列导入简历进行下载
    def resume_down(self, data):
        try:
            # 兼容GET请求与POST请求
            response = self._httpClient.call(data['url'], data.get('postdata', None))
            # 特殊处理,RD2简历下载请求量过大,输入验证码
            if response.url.find('/resume/validateuser?') > -1:
                self.monitorvalidate()
                sleep(5)
                raise NetworkError(u'简历下载请求过快,需要验证')
            ext = '.html'
            if 'Content-Disposition' in response.headers.keys():
                filename = substring(response.headers['Content-Disposition'], 'filename="', '"')
                ext = os.path.splitext(filename)[-1]
            fullfilepath = self._resume_filepath(data['resumeid'], ext)
            filecontent = self.resume_detail_content(response.data)
            with open(fullfilepath, 'w+') as fh:
                fh.write(filecontent)
                fh.close()
            data['filepath'] = fullfilepath
            ResumeRedisCli.hincrby(Conf.RESUME_IMPORT_HKEY % data['taskid'], 'grab')
            self.status = HANDLE_STATUS.SUCCESS
            self.message = u'<{}>简历<{}, {}>下载成功!'.format(self.name, data['username'], data['resumeid'])
        except BaseError as e:
            self.message = u'<{}>简历<{}, {}>下载请求失败,原因:{}'.format(self.name, data['username'], data['resumeid'], e.message)
            if data.get('retry', 0) < 5:
                self.status = HANDLE_STATUS.AGAIN

    def resume_detail_content(self, httpbody):
        return httpbody

    def _resume_filepath(self, resumeid, ext):
        STORE_RESUME_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'resume'))
        filepath = os.path.join(STORE_RESUME_DIR, self.storeDir, datetime.today().strftime('%Y%m%d'))
        makedirs(filepath)
        fullfilepath = os.path.join(filepath, '{}{}'.format(resumeid, ext))
        return fullfilepath

    # 认证回调
    def authCallBack(self, response):
        if self._authRedirect:
            if isinstance(self._authRedirect, basestring):
                if response.url.find(self._authRedirect) > -1:
                    return False
            elif isinstance(self._authRedirect, list):
                for url in self._authRedirect:
                    if response.url.find(url) > -1:
                        return False
        return True

    # 获取页面提交表单Field
    def _GetInputFields(self, data):
        if data is None:
            return {}
        else:
            return input_field_value(data)
