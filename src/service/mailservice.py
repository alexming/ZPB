#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/mailservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-24 18:49
#########################################################################


# stdlib
import os
import re
import poplib
# 设定邮件最大可读行数
poplib._MAXLINE = 40960
import email
import json
import re
import base64
import quopri
import threading
import socket
from datetime import datetime
from poplib import error_proto
# zpb
from zpb.cache.rediscache import ResumeRedisCli, MailRedisCli
from zpb.utils.tools import str_encode, makedirs, str2datetimewithzone, datetime2str
from zpb.business.model.imphistory import ImpHistory
from zpb.business.model.emailconf import EmailConf
from zpb.conf import Conf, logger
# dtc
import dtc

debug_mode = False
# 邮件uidl编号集合
MAIL_SET_KEY = 'mail:uidl:%s'


def pop3(host, port, username, password, usessl):
    try:
        conn = poplib.POP3_SSL(host) if usessl else poplib.POP3(host)
        if debug_mode:
            conn.set_debuglevel(2)
        conn.user(username)
        conn.pass_(password)
        logger.info(u'[+] 邮箱<{}> 登录成功'.format(username))
        return conn, ''
    except error_proto:
        error_message = u'登录账号或密码错误'
        logger.error(u'[+] 邮箱<{}> 登录失败,原因:'.format(username, error_message))
        return None, error_message
    except socket.gaierror:
        error_message = u'非法POP3服务器名'
        logger.error(u'[+] 邮箱<{}> 登录失败,原因:<{}>为{}'.format(username, host, error_message))
        return None, error_message
    except BaseException as e:
        # 可能网络异常,可以再次执行
        logger.error(u'[+] 邮箱<{}> 登录失败,原因:{}'.format(username, str(e)))
        return None, None


# 拉取新邮件
def pull_email(emailconf, companyid, taskid, importid, syncid):
    pop, error_message = pop3(emailconf.pop3_host, emailconf.pop3_port, emailconf.email_user,
        emailconf.email_password, emailconf.is_ssl == 'T')
    if pop:
        try:
            try:
                typ, uidls, octets = pop.uidl()
            except error_proto as e:
                logger.error(u'[-] 获取邮箱<{}>状态失败,原因:{0}'.format(emailconf.email_user, e))
                return
            if len(uidls) > 0:
                msgs = []
                setkey = MAIL_SET_KEY % emailconf.email_user
                # 过滤已下载邮件
                for item in uidls:
                    mid, uidl = item.split()
                    if not MailRedisCli.sismember(setkey, uidl):
                        msgs.append((mid, uidl))
                if len(msgs) > 0:
                    logger.info(u'[+] 邮箱<{}>待下载 {} 封未读邮件...'.format(emailconf.email_user, len(msgs)))
                    imp = ImpHistory.new(emailconf.company_id, 0, emailconf.import_id, 3)
                    imp.src_memo = emailconf.email_user
                    if imp.save():
                        key = Conf.RESUME_IMPORT_HKEY % taskid
                        ResumeRedisCli.hmset(
                            key,
                            {
                                'total': 0, 'grab': 0, 'success': 0,
                                'ignore': 0, 'failure': 0, 'finish': 0,
                                'siteid': emailconf.email_user, # 邮箱地址
                                'importid': importid,  # 来源id,用於追溯
                                'companyid': companyid,
                                'imphistoryid': imp.history_id,  # 后续存储imp_history_resume时使用
                                'syncid': syncid
                            }
                        )
                        for mid, uidl in msgs:
                            download_email(pop, emailconf, mid, uidl, taskid)
                        ResumeRedisCli.hincrby(Conf.RESUME_IMPORT_HKEY % taskid, 'finish')
                        dtc.async('zpb.service.stateservice.CheckEmailImportStat', taskid)
                        logger.info(u'[+] 邮箱<{}>已下载 {} 封未读邮件!'.format(emailconf.email_user, len(msgs)))
                else:
                    logger.info(u'[+] 邮箱<{}>没有未读邮件!'.format(emailconf.email_user))
            else:
                logger.info(u'[-] 邮箱<{}>没有任何邮件!'.format(emailconf.email_user))
        finally:
            pop.quit()
    elif error_message:
        emailconf.is_valid = 'F'
        emailconf.import_memo = error_message
        emailconf.save()


# 下载邮件
def download_email(pop, emailconf, mid, uidl, taskid):
    logger.info(u'[*] 正在下载邮箱<{}>第<{}>封邮件'.format(emailconf.email_user, mid))
    try:
        typ, data, octets = pop.retr(mid)
        msg = email.message_from_string('\n'.join(data))
    except error_proto as e:
        logger.error(u'[*] 邮箱<{}>中的第<{}>封邮件下载失败,原因:{}'.format(emailconf.email_user, mid, e))
        return
    # Parse and save email content/attachments
    try:
        parse_email(emailconf, msg, mid, taskid)
        setkey = MAIL_SET_KEY % emailconf.email_user
        MailRedisCli.sadd(setkey, uidl)
    except BaseException as e:
        logger.error(u'[*] 邮箱<{}>中的第<{}>封邮件解析失败,原因:{}'.format(emailconf.email_user, mid, e))


def parse_email(emailconf, msg, mid, taskid):
    siteid = 0
    match = re.search(r'.+<(.+)>', msg['From'], re.I|re.M)
    if match:
        _from = match.group(1)
        if _from.find('51job') > -1:
            siteid = 1
        elif _from.find('zhaopinmail') > -1:
            siteid = 2
        # 来自拉勾的简历不全,过滤掉
        elif _from.find('lagoujobs') > -1:
            return
        elif _from.find('cjol') > -1:
            siteid = 4
    global result_file
    # Parse and save email content and attachments
    for part in msg.walk():
        if not part.is_multipart():
            charset = guess_charset(part)
            # 此处存在bug,附件(中文文件名)文件名不全
            # filename = part.get_filename()
            filename = part.get_param('name')
            STORE_EMAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'email'))
            filepath = os.path.join(STORE_EMAIL_DIR, emailconf.email_user, datetime.today().strftime('%Y%m%d'))
            makedirs(filepath)
            contenttype = part.get_content_type()
            if filename:  # Attachment
                filename = decode_filename(filename)
                content = guess_content(filename, part.get_payload(decode=1))
                result_file = os.path.join(filepath, 'mail{0}_attach_{1}'.format(mid, filename))
            else:  # Mail content
                if contenttype in ['text/plain']:
                    suffix = '.txt'
                if contenttype in ['text/html']:
                    suffix = '.html'
                if charset:
                    content = part.get_payload(decode=True).decode(charset, 'ignore')
                else:
                    content = part.get_payload(decode=True)
                result_file = os.path.join(filepath, 'mail{0}_text{1}'.format(mid, suffix))
            try:
                ext = os.path.splitext(result_file)[-1]
                if ext in ['.txt', '.doc', '.html', '.htm', 'docx', '.pdf', '.wps', '.rtf', '.eml', '.msg', '.mht']:
                    with open(result_file, 'wb') as f:
                        f.write(content)
                        f.close()
                    # 写入缓存
                    data = {}
                    data['jobid'] = ''
                    data['resumeid'] = ''
                    data['username'] = mid
                    data['taskid'] = taskid
                    data['companyid'] = emailconf.company_id
                    data['filepath'] = os.path.abspath(result_file)
                    data['siteid'] = siteid
                    if msg['Date'].find(',') > -1:
                        data['postdate'] = datetime2str(str2datetimewithzone(msg['Date'], '%a, %d %b %Y %H:%M:%S'))
                    else:
                        data['postdate'] = datetime2str(str2datetimewithzone(msg['Date'], '%d %b %Y %H:%M:%S'))
                    ResumeRedisCli.hincrby(Conf.RESUME_IMPORT_HKEY % taskid, 'total')
                    ResumeRedisCli.hincrby(Conf.RESUME_IMPORT_HKEY % taskid, 'grab')
                    dtc.async('zpb.service.resumeservice.ParseResume', 'zpb.service.stateservice.CheckEmailImportStat', **data)
            except BaseException as e:
                logger.error(u'[-] 第<{}>封邮件文件存储失败,原因:{}'.format(mid, e))


def guess_content(filename, content):
    # html/htm类型附件依据内容编码格式重新编码
    if filename.find('.html') > 0 or filename.find('.htm') > 0:
        pos = content.find('charset=')
        if pos > 0:
            end = content.find('"/>', pos)
            if end > 0:
                charset = content[pos + 8: end].lower()
                content = content.decode(charset, 'ignore')
    return content


def guess_charset(part):
    charset = part.get_charset()
    if not charset:
        content_type = part.get('Content-Type', '').lower()
        pos = content_type.find('charset="')
        if pos >= 0:
            charset = content_type[pos + 9:-1].strip()
        else:
            pos = content_type.find('charset=')
            if pos >= 0:
                charset = content_type[pos + 8:].strip()
            else:
                pos = content_type.find('name="=?')
                if pos > 0:
                    end = content_type.find('?b', pos)
                    charset = content_type[pos + 8: end]
    return charset


def decode_filename(filename):
    if filename.find('=') > -1:
        params = filename.split('?=')
        values = []
        for v in params:
            match = re.search(r'=\?((?:\w|-)+)\?(Q|B)\?(.+)', v)
            if match:
                encoding, typ, code = match.groups()
                if typ == 'Q':
                    value = quopri.decodestring(code)
                elif typ == 'B':
                    value = base64.decodestring(code)
                values.append(str_encode(value, encoding, 'ignore'))
            else:
                values.append(v)
        return ''.join(values)
    else:
        return filename


def DoMailSearcher(companyid, taskid, importid, syncid):
    emailconf = EmailConf.queryByImportId(importid)
    if emailconf:
        pull_email(emailconf, companyid, taskid, importid, syncid)
    EmailConf.updateImportTimeAndNumberByImportId(importid, 0)
