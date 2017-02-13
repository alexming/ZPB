#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/handleservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:09
#########################################################################


# stdlib
from datetime import datetime
# zpb
from zpb.business.model.dbtask import DBTask
from zpb.business.model.jobmemo import JobMemo
from zpb.business.model.basejob import BaseJob
from zpb.business.model.jobsyncdistribute import JobSyncDistribute
from zpb.business.model.ressyncdistribute import ResSyncDistribute
from zpb.core.BaseHandlerManager import HANDLE_STATUS
from zpb.business.siteconfig import SiteConfig
from zpb.utils.tools import md5
from zpb.exception import *
from zpb.conf import logger
# dtc
import dtc


def Do20(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        # 账号已解除绑定
        if handler.bind.is_valid != 'T':
            DBTask.newSchedule(companyid, siteid, False)
            raise CompanyAccountUnBindError(companyid, siteid, u'账号已解除绑定')
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        try:
            handler.position_import()
            if HANDLE_STATUS.SUCCESS == handler.status:
                task.sync_status = 2000
                task.succ_num += 1
                task.log_info = handler.message
                # 账号绑定
                handler.bind.check_status = 50
                handler.bind.login_result = '登录成功'
                handler.bind.last_succ_time = datetime.today()
                handler.bind.save()
                # 下发其它异步任务
                infouid = md5(taskid + 'info')
                dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, infouid), uid=infouid)
                resuid = md5(taskid + '201')
                dtc.async('zpb.service.handleservice.Do201', *(companyid, siteid, syncid, resuid), uid=resuid)
                logger.info(handler.message)
            elif HANDLE_STATUS.AGAIN == handler.status:
                task.sync_status = 11
                task.fail_num += 1
                task.log_info = handler.message
                logger.error(handler.message)
            else:
                task.sync_status = 10
                task.fail_num += 1
                task.log_info = handler.message
                logger.error(handler.message)
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def DoInfo(companyid, siteid, taskid):
    try:
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        try:
            handler.info()
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        pass


def Do101(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        jobid = kwargs.pop('jobid', None)
        if not jobid:
            raise InvalidParamError(companyid, siteid, u'未指定发布职位编号')
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        # 账号已解除绑定
        if handler.bind.is_valid != 'T':
            DBTask.newSchedule(companyid, siteid, False)
            raise CompanyAccountUnBindError(companyid, siteid, u'账号已解除绑定')
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        # step 1
        dist = JobSyncDistribute.queryByJobIdAndCompanyIdWithSiteId(jobid, companyid, siteid)
        if not dist:
            raise JobNotDistributeError(companyid, siteid, u'未找到职位同步记录')
        try:
            if not dist.third_job_code:
                handler.position_add(jobid)
            else:
                handler.position_modify(jobid, dist.third_job_code, dist.last_sync_time)
            # step 2
            if HANDLE_STATUS.SUCCESS == handler.status:
                if hasattr(handler, 'thirdjobcode'):
                    dist.third_job_code = handler.thirdjobcode
                dist.sync_succ_num += 1
                dist.sync_status = 20
                dist.error_message = ''
                dist.last_sync_time = datetime.today()
                #
                BaseJob.updateSyncTimeByJobId(jobid)
                #
                dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, taskid))
            else:
                dist.sync_fail_num += 1
                dist.sync_status = 10
                dist.error_message = handler.message
            dist.save()
            # step 3
            jmm = JobMemo(jobid)
            if HANDLE_STATUS.SUCCESS == handler.status:
                jmm.memo_content = u'[{}]发布成功'.format(handler.name)
            else:
                jmm.memo_content = u'[{}]发布失败,{}'.format(handler.name, handler.message)
            jmm.save()
            # step 4
            if HANDLE_STATUS.SUCCESS == handler.status:
                task.succ_num += 1
                task.log_info = handler.message
                task.sync_status = 20
                logger.info(handler.message)
            else:
                task.fail_num += 1
                task.log_info = handler.message
                if HANDLE_STATUS.AGAIN == handler.status:
                    task.sync_status = 11
                else:
                    task.sync_status = 10
                logger.error(handler.message)
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def Do102(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        jobid = kwargs.pop('jobid', None)
        if not jobid:
            raise InvalidParamError(companyid, siteid, u'未指定发布职位编号')
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        # 账号已解除绑定
        if handler.bind.is_valid != 'T':
            DBTask.newSchedule(companyid, siteid, False)
            raise CompanyAccountUnBindError(companyid, siteid, u'账号已解除绑定')
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        # step 1
        dist = JobSyncDistribute.queryByJobIdAndCompanyIdWithSiteId(jobid, companyid, siteid)
        if not dist:
            raise JobNotDistributeError(companyid, siteid, u'未找到职位同步记录')
        if not dist.third_job_code:
            raise JobNotDistributeError(companyid, siteid, u'职位未同步')
        try:
            handler.position_refresh(dist.third_job_code)
            # step 2
            if HANDLE_STATUS.SUCCESS == handler.status:
                dist.sync_status = 20
                dist.error_message = ''
                dist.last_refresh_time = datetime.today()
                #
                BaseJob.updateRefreshTimeByJobId(jobid)
                #
                dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, taskid))
            else:
                dist.sync_fail_num += 1
                dist.sync_status = 10
                dist.error_message = handler.message
            dist.save()
            # step 3
            jmm = JobMemo(jobid)
            if HANDLE_STATUS.SUCCESS == handler.status:
                jmm.memo_content = u'[{}]刷新成功'.format(handler.name)
            else:
                jmm.memo_content = u'[{}]刷新失败,{}'.format(handler.name, handler.message)
            jmm.save()
            # step 4
            if HANDLE_STATUS.SUCCESS == handler.status:
                task.succ_num += 1
                task.log_info = handler.message
                task.sync_status = 20
                logger.info(handler.message)
            else:
                task.fail_num += 1
                task.log_info = handler.message
                if HANDLE_STATUS.AGAIN == handler.status:
                    task.sync_status = 11
                else:
                    task.sync_status = 10
                logger.error(handler.message)
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def Do103(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        jobid = kwargs.pop('jobid', None)
        if not jobid:
            raise InvalidParamError(companyid, siteid, u'未指定发布职位编号')
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        # 账号已解除绑定
        if handler.bind.is_valid != 'T':
            DBTask.newSchedule(companyid, siteid, False)
            raise CompanyAccountUnBindError(companyid, siteid, u'账号已解除绑定')
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        # step 1
        dist = JobSyncDistribute.queryByJobIdAndCompanyIdWithSiteId(jobid, companyid, siteid)
        if not dist:
            raise JobNotDistributeError(companyid, siteid, u'未找到职位同步记录')
        if not dist.third_job_code:
            raise JobNotDistributeError(companyid, siteid, u'职位未同步')
        try:
            handler.position_delete(dist.third_job_code)
            # step 2
            if HANDLE_STATUS.SUCCESS == handler.status:
                dist.sync_status = 20
                dist.error_message = ''
                dist.last_refresh_time = datetime.today()
                #
                dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, taskid))
            else:
                dist.sync_fail_num += 1
                dist.sync_status = 10
                dist.error_message = handler.message
            dist.save()
            # step 3
            jmm = JobMemo(jobid)
            if HANDLE_STATUS.SUCCESS == handler.status:
                jmm.memo_content = u'[{}]暂停成功'.format(handler.name)
            else:
                jmm.memo_content = u'[{}]暂停失败,{}'.format(handler.name, handler.message)
            jmm.save()
            # step 4
            if HANDLE_STATUS.SUCCESS == handler.status:
                task.succ_num += 1
                task.log_info = handler.message
                task.sync_status = 20
                logger.info(handler.message)
            else:
                task.fail_num += 1
                task.log_info = handler.message
                if HANDLE_STATUS.AGAIN == handler.status:
                    task.sync_status = 11
                else:
                    task.sync_status = 10
                logger.error(handler.message)
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def Do104(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        jobid = kwargs.pop('jobid', None)
        if not jobid:
            raise InvalidParamError(companyid, siteid, u'未指定发布职位编号')
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        # 账号已解除绑定
        if handler.bind.is_valid != 'T':
            DBTask.newSchedule(companyid, siteid, False)
            raise CompanyAccountUnBindError(companyid, siteid, u'账号已解除绑定')
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        # step 1
        dist = JobSyncDistribute.queryByJobIdAndCompanyIdWithSiteId(jobid, companyid, siteid)
        if not dist:
            raise JobNotDistributeError(companyid, siteid, u'未找到职位同步记录')
        if not dist.third_job_code:
            raise JobNotDistributeError(companyid, siteid, u'职位未同步')
        try:
            handler.position_pause(dist.third_job_code)
            # step 2
            if HANDLE_STATUS.SUCCESS == handler.status:
                dist.sync_status = 20
                dist.error_message = ''
                dist.last_refresh_time = datetime.today()
                #
                dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, taskid))
            else:
                dist.sync_fail_num += 1
                dist.sync_status = 10
                dist.error_message = handler.message
            dist.save()
            # step 3
            jmm = JobMemo(jobid)
            if HANDLE_STATUS.SUCCESS == handler.status:
                jmm.memo_content = u'[{}]删除成功'.format(handler.name)
            else:
                jmm.memo_content = u'[{}]删除失败,{}'.format(handler.name, handler.message)
            jmm.save()
            # step 4
            if HANDLE_STATUS.SUCCESS == handler.status:
                task.succ_num += 1
                task.log_info = handler.message
                task.sync_status = 20
                logger.info(handler.message)
            else:
                task.fail_num += 1
                task.log_info = handler.message
                if HANDLE_STATUS.AGAIN == handler.status:
                    task.sync_status = 11
                else:
                    task.sync_status = 10
                logger.error(handler.message)
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def Do201(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        # 账号已解除绑定
        if handler.bind.is_valid != 'T':
            DBTask.newSchedule(companyid, siteid, False)
            raise CompanyAccountUnBindError(companyid, siteid, u'账号已解除绑定')
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        try:
            handler.resume_import(syncid)
        except BaseException as e:
            raise UnHandleRuntimeError(e)
        if HANDLE_STATUS.SUCCESS == handler.status:
            task.succ_num += 1
            task.log_info = handler.message
            task.sync_status = 20
            #
            dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, taskid))
            logger.info(handler.message)
        else:
            task.fail_num += 1
            task.log_info = handler.message
            if HANDLE_STATUS.AGAIN == handler.status:
                task.sync_status = 11
            else:
                task.sync_status = 10
            logger.error(handler.message)
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def Do301(companyid, siteid, syncid, taskid, **kwargs):
    task = DBTask.queryWithId(syncid)
    try:
        otherid = kwargs.pop('otherid', None)
        if not otherid:
            raise InvalidParamError(companyid, siteid, u'未指定邮箱编号')
        from zpb.service.mailservice import DoMailSearcher
        DoMailSearcher(companyid, taskid, otherid, syncid)
        task.sync_status = 2010
    except BaseError as e:
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
    task.save()


def DoResumePayDown(companyid, siteid, syncid, fromsitecode, ownpay, taskid):
    try:
        if ownpay == 'T':
            handler = SiteConfig.GetTaskHandler(companyid, siteid, taskid)
        else:
            # 获取公用账号企业ID
            common_companyid = 0
            handler = SiteConfig.GetTaskHandler(common_companyid, siteid, taskid)
        # 账号未验证通过
        if handler.bind.check_status == 10:
            raise CompanyAccountInvalidError(companyid, siteid, u'账号登录失败')
        try:
            handler.resume_searcher_down(fromsitecode, syncid, companyid)
            # 任务状态回写
            task = ResSyncDistribute.queryWithId(syncid)
            if HANDLE_STATUS.SUCCESS == handler.status:
                task.sync_status = 2010
                logger.info(u'<{}>下载付费简历<{}>成功!'.format(handler.name, fromsitecode, handler.message))
            else:
                task.sync_status = 10
                task.error_message = handler.message
                logger.error(u'<{}>下载付费简历<{}>失败,原因:{}'.format(handler.name, fromsitecode, handler.message))
            task.save()
        except BaseException as e:
            raise UnHandleRuntimeError(e)
    except BaseError as e:
        task = DBTask.queryWithId(syncid)
        task.sync_status = 10
        task.fail_num += 1
        task.log_info = e.message
        task.save()