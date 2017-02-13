#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/stateservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:14
#########################################################################


# stdlib
from datetime import datetime
# zpb
from zpb.service.authservice import AuthService
from zpb.business.model.dbtask import DBTask
from zpb.business.model.cmplog import CMPLog
from zpb.business.model.emailconf import EmailConf
from zpb.business.model.imphistory import ImpHistory
from zpb.business.model.resume import ResumeBase
from zpb.business.model.ressyncdistribute import ResSyncDistribute
from zpb.business.siteconfig import SiteConfig
from zpb.cache.rediscache import ResumeRedisCli
from zpb.utils.tools import str2int
from zpb.conf import Conf, logger
# dtc
import dtc


def CheckResumeSearchStat(taskid):
    key = Conf.RESUME_IMPORT_HKEY % taskid
    stat = ResumeRedisCli.hgetall(key)
    if stat:
        # finish
        finish = int(stat.get('finish', '0'))
        if finish:
            # 统计信息
            total = int(stat['total'])
            grab = int(stat['grab'])
            succ = int(stat['success'])
            ignore = int(stat['ignore'])
            fail = int(stat['failure'])
            # 来源信息
            siteid = int(stat['siteid'])
            importid = stat['importid']
            companyid = int(stat['companyid'])
            imphistoryid = int(stat['imphistoryid'])
            #
            if total > 0 and total <= succ + ignore + fail:
                if total <= succ + ignore + fail:
                    if ResumeRedisCli.delete(key):
                        imp = ImpHistory.queryByHistoryId(imphistoryid)
                        if imp:
                            imp.succ_num = succ
                            imp.fail_num = fail
                            imp.end_time = datetime.today()
                            imp.proc_status = 1
                            if imp.succ_num == 0:
                                imp.is_valid = 'F'
                            imp.save()
                        # 更新bind简历最后导入时间(siteid取自key,task中的site_id可能=0)
                        AuthService().updateBindImportTimeByCompanyIdAndSiteId(companyid, siteid)
                        # 回写任务状态
                        msg = u'<{}>简历总数<{}>,下载数<{}>,成功数<{}>,重复数<{}>,失败数<{}>.'.format(
                            SiteConfig.getSiteNameById(siteid),
                            total, grab, succ, ignore, fail)
                        logger.info(msg)
            elif total == 0:
                if ResumeRedisCli.delete(key):
                    ImpHistory.removeByHistoryId(imphistoryid)
                    # 更新bind简历最后导入时间(siteid取自key,task中的site_id可能=0)
                    AuthService().updateBindImportTimeByCompanyIdAndSiteId(companyid, siteid)


def CheckResumeImportStat(taskid):
    key = Conf.RESUME_IMPORT_HKEY % taskid
    stat = ResumeRedisCli.hgetall(key)
    if stat:
        # finish
        finish = int(stat.get('finish', '0'))
        if finish:
            # 统计信息
            total = int(stat['total'])
            grab = int(stat['grab'])
            succ = int(stat['success'])
            ignore = int(stat['ignore'])
            fail = int(stat['failure'])
            # 来源信息
            siteid = int(stat['siteid'])
            importid = stat['importid']
            companyid = int(stat['companyid'])
            imphistoryid = int(stat['imphistoryid'])
            #
            task = DBTask.queryWithId(importid)
            if task:
                if total > 0 and total <= succ + ignore + fail:
                    if total <= succ + ignore + fail:
                        if ResumeRedisCli.delete(key):
                            imp = ImpHistory.queryByHistoryId(imphistoryid)
                            if imp:
                                imp.succ_num = succ
                                imp.fail_num = fail
                                imp.end_time = datetime.today()
                                imp.proc_status = 1
                                if imp.succ_num == 0:
                                    imp.is_valid = 'F'
                                imp.save()
                            # 更新bind简历最后导入时间(siteid取自key,task中的site_id可能=0)
                            AuthService().updateBindImportTimeByCompanyIdAndSiteId(companyid, siteid)
                            # 回写任务状态
                            msg = u'{}<{}>简历总数<{}>,下载数<{}>,成功数<{}>,重复数<{}>,失败数<{}>.'.format(
                                datetime.today(),
                                SiteConfig.getSiteNameById(siteid),
                                total, grab, succ, ignore, fail)
                            task.succ_num += 1
                            task.sync_status = 20
                            task.log_info = msg
                            task.save()
                            logger.info(msg)
                elif total == 0:
                    if ResumeRedisCli.delete(key):
                        ImpHistory.removeByHistoryId(imphistoryid)
                        # 更新bind简历最后导入时间(siteid取自key,task中的site_id可能=0)
                        AuthService().updateBindImportTimeByCompanyIdAndSiteId(companyid, siteid)
                        if task.sync_status not in [10, 11]:
                           task.sync_status = 20
                        msg = u'{}<{}>简历总数:0'.format(datetime.today(), SiteConfig.getSiteNameById(siteid))
                        task.succ_num += 1
                        task.sync_status = 20
                        task.log_info = msg
                        task.save()
                        logger.info(msg)


def CheckResumeSearchDownStat(taskid):
    key = Conf.RESUME_IMPORT_HKEY % taskid
    stat = ResumeRedisCli.hgetall(key)
    if stat:
        # finish
        finish = int(stat.get('finish', '0'))
        if finish:
            # 统计信息
            total = int(stat['total'])
            grab = int(stat['grab'])
            succ = int(stat['success'])
            ignore = int(stat['ignore'])
            fail = int(stat['failure'])
            # 来源信息
            siteid = int(stat['siteid'])
            importid = stat['importid']
            companyid = int(stat['companyid'])
            imphistoryid = int(stat['imphistoryid'])
            #
            task = ResSyncDistribute.queryWithId(importid)
            if task:
                if total > 0 and total <= succ + ignore + fail:
                    if total <= succ + ignore + fail:
                        if ResumeRedisCli.delete(key):
                            imp = ImpHistory.queryByHistoryId(imphistoryid)
                            if imp:
                                imp.succ_num = succ
                                imp.fail_num = fail
                                imp.end_time = datetime.today()
                                imp.proc_status = 1
                                if imp.succ_num == 0:
                                    imp.is_valid = 'F'
                                imp.save()
                            # 更改简历状态
                            ResumeBase.changeStat(task.resume_code)
                            # 回写任务状态
                            msg = u'<{}>下载付费简历总数<{}>,下载数<{}>,成功数<{}>,重复数<{}>,失败数<{}>.'.format(
                                SiteConfig.getSiteNameById(siteid),
                                total, grab, succ, ignore, fail)
                            task.sync_status = 20
                            task.error_message = ''
                            task.process_time = datetime.today()
                            task.save()
                            logger.info(msg)
                            dtc.async('zpb.service.handleservice.DoInfo', *(companyid, siteid, taskid))
                elif total == 0:
                    if ResumeRedisCli.delete(key):
                        ImpHistory.removeByHistoryId(imphistoryid)
                        msg = u'<{}>下载付费简历总数:0'.format(SiteConfig.getSiteNameById(siteid))
                        task.sync_status = 20
                        task.error_message = ''
                        task.process_time = datetime.today()
                        task.save()
                        logger.info(msg)


def CheckEmailImportStat(taskid):
    key = Conf.RESUME_IMPORT_HKEY % taskid
    stat = ResumeRedisCli.hgetall(key)
    if stat:
        # finish
        finish = int(stat.get('finish', '0'))
        if finish:
            # 统计信息
            total = int(stat['total'])
            grab = int(stat['grab'])
            succ = int(stat['success'])
            ignore = int(stat['ignore'])
            fail = int(stat['failure'])
            # 来源信息
            siteid = stat['siteid']
            importid = stat['importid']
            companyid = stat['companyid']
            imphistoryid = int(stat['imphistoryid'])
            syncid = int(stat.get('syncid', None))
            #
            if total > 0 and total <= succ + ignore + fail:
                if total <= succ + ignore + fail:
                    if ResumeRedisCli.delete(key):
                        imp = ImpHistory.queryByHistoryId(imphistoryid)
                        if imp:
                            imp.succ_num = succ
                            imp.fail_num = fail
                            imp.end_time = datetime.today()
                            imp.proc_status = 1
                            if imp.succ_num == 0:
                                imp.is_valid = 'F'
                            imp.save()
                        # 更新邮箱信息
                        EmailConf.updateImportTimeAndNumberByImportId(importid, succ)
                        # 回写任务状态
                        msg = u'<{}>邮箱简历总数<{}>,下载数<{}>,成功数<{}>,重复数<{}>,失败数<{}>.'.format(
                            siteid, total, grab, succ, ignore, fail)
                        logger.info(msg)
                        # 任务状态回写
                        if syncid:
                            task = DBTask.queryWithId(syncid)
                            if task:
                                task.succ_num += 1
                                task.sync_status = 20
                                task.log_info = msg
                                task.save()
            elif total == 0:
                if ResumeRedisCli.delete(key):
                    ImpHistory.removeByHistoryId(imphistoryid)
                    # 更新邮箱信息
                    EmailConf.updateImportTimeAndNumberByImportId(importid, 0)
                    msg = u'<{}>邮箱简历总数:0'.format(siteid)
                    logger.info(msg)
                    # 任务状态回写
                    if syncid:
                        task = DBTask.queryWithId(syncid)
                        if task:
                            task.succ_num += 1
                            task.sync_status = 20
                            task.log_info = msg
                            task.save()
