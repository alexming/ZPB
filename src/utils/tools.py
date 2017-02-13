#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: utils/tools.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:18
#########################################################################


# stdlib
import os
import re
import time
import errno
import binascii
import hashlib
import uuid as stduuid
import mimetools
from datetime import datetime, date, timedelta
from Crypto.Cipher import DES
from Crypto import Random
from base64 import b64encode, b64decode
# scrapy
from scrapy import Selector
# zpb
from zpb.utils.fixedoffset import FixedOffset
from zpb.conf import logger

# datetime与time的strptime模块加载时,在多线程环境会出现异常,此处首先加载模块
datetime.strptime('', '')

# 枚举实现
# HTTP_STATUS = enum(SUCCESS = 0, FAILURE = -1)


def enum(**enums):
    return type('Enum', (), enums)


PADDING = '\0'
first_item = lambda x: x[0] if x else ''
first_text = lambda node: first_item(node.extract())
pad_it = lambda s: s + (DES.block_size - len(s) % DES.block_size) * PADDING if len(s) % DES.block_size > 0 else s

def md5(sourcestr):
    return hashlib.md5(sourcestr).hexdigest()

# 脱胎于php的同名函数,功能一致
def chunk_split(data, step=1, fill=''):
    return [data[i: min(i + step, len(data))].ljust(step) for i in range(0, len(data), step)]


# 32位uuid
def uuid():
    return str(stduuid.uuid1()).replace('-', '')


# str转int
def str2int(instr, default=0):
    if instr.isdigit():
        return int(instr)
    else:
        return default


# ifnull
def ifnull(inParam, outParam):
    if inParam is None:
        return outParam
    else:
        return inParam


# str转datetime
def str2datetime(instr, format='%Y-%m-%d %H:%M:%S'):
    if instr:
        try:
            return datetime.strptime(instr.replace('/', '-'), format)
        except BaseException as e:
            logger.error(u'时间格式化异常,message:<{}>'.format(e))
            return datetime.today()
    else:
        return datetime.today()


# str转date
def str2date(instr, format='%Y-%m-%d %H:%M:%S'):
    if instr:
        return datetime.date(datetime.strptime(instr.replace('/', '-'), format))
    else:
        return date.today()


# str转datetime后的str
def str2datetimestr(instr, format='%Y-%m-%d %H:%M:%S'):
    return datetime2str(str2datetime(instr.replace('/', '-'), format))


# datetime转str
def datetime2str(indate):
    return datetime.strftime(indate, '%Y-%m-%d %H:%M:%S')


# datetime转str
def date2str(indate):
    if indate:
        return datetime.strftime(indate, '%Y-%m-%d')
    else:
        return datetime.strftime(datetime.today(), '%Y-%m-%d')


# 带时区日期格式化
def str2datetimewithzone(instr, format):
    naive_date_str, _, offset_str = instr.rstrip(' (CST)').rpartition(' ')
    naive_dt = datetime.strptime(naive_date_str, format)
    offset = int(offset_str[-4:-2])*60 + int(offset_str[-2:])
    if offset_str[0] == "-":
        offset = -offset
    dt = naive_dt.replace(tzinfo=FixedOffset(offset))
    return dt


# 日期操作
def dateAdd(date=date.today(), day=1):
    timespan = timedelta(days=day)
    return date + timespan


# 删除特殊字符
def clsChar(inStr, chars=['\r', '\n', ' '], rep=''):
    for char in chars:
        inStr = inStr.replace(char, rep)
    return inStr


# 依据关键字查找中间字符
def substring(instr, begin, end=None):
    idx1 = instr.find(begin)
    if end is not None:
        idx2 = instr.find(end, idx1 + len(begin))
        return instr[idx1 + len(begin): idx2]
    else:
        return instr[idx1 + len(begin):]

def encryptBindPasswd(salt, original):
    enkey = b'{0}-renSHIyi.com'.format(salt)
    enkey = enkey[0: 8]
    try:
        iv = Random.new().read(DES.block_size)
        generator = DES.new(enkey, DES.MODE_ECB, iv)
        s = generator.encrypt(pad_it(original))
        return b64encode(generator.encrypt(pad_it(original)))
    except BaseException as e:
        logger.error(u'数据加密失败,原始串:{},原因:{}'.format(original, e))
	return ''


def decryptBindPasswd(salt, ciphering):
    enkey = b'{0}-renSHIyi.com'.format(salt)
    enkey = enkey[0: 8]
    try:
        passwdcrypt = b64decode(ciphering)
        iv = Random.new().read(DES.block_size)
        generator = DES.new(enkey, DES.MODE_ECB, iv)
        return generator.decrypt(passwdcrypt).rstrip('\x00')
    except BaseException as e:
        logger.error(u'数据解密失败,加密串:{},原因:{}'.format(ciphering, e))
	return ''


# 只保留必要的html标签
def clearHtmlTag(htmlstr):
    if not htmlstr:
        return ''
    # 先过滤CDATA
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br.*?/?>', re.I)  # 处理换行
    re_p1 = re.compile('<p.*?/?>', re.I)  # 处理<p>
    re_p2 = re.compile('</p>', re.I)  # 处理</p>
    re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    re_comment = re.compile('<!--[^>]*-->', re.I|re.S)  # HTML注释
    s = re_cdata.sub('', htmlstr)  # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\r\n', s)  # 将br转换为_br_
    s = re_p1.sub('\r\n', s)  # 将<p>转换为_p_
    s = re_p2.sub('', s)  # 将</p>转换为_/p_
    s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    blank_line = re.compile('\r\n+')
    s = blank_line.sub('\r\n', s)
    blank_line = re.compile('\n\r\n')
    s = blank_line.sub('\r\n', s)
    s = s.lstrip('\r\n')
    return s


# 去除script,comment,cdata
def fmtHtml(content):
    if not content:
        return ''
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I|re.S)  # 匹配CDATA
    re_comment0 = re.compile('<!--[^>]*-->', re.I|re.S)  # HTML注释
    re_comment1 = re.compile('<!--.+-->', re.I|re.S)  # HTML注释
    re_script0 = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I|re.S)  # Script
    re_script1 = re.compile('<\s*script[^>]*>(.+?)<\s*/\s*script\s*>', re.I|re.S)  # Script
    s = re_script0.sub('', content)  # 去掉SCRIPT
    s = re_cdata.sub('', s)  # 去掉CDATA
    s = re_comment0.sub('', s)  # 去掉HTML注释
    s = re_script1.sub('', s)  # 去掉SCRIPT
    #s = re_comment1.sub('', s)
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    blank_line = re.compile('\r\n+')
    s = blank_line.sub('\r\n', s)
    blank_line = re.compile('\n\r\n')
    s = blank_line.sub('\r\n', s)
    s = s.lstrip('\r\n')
    return s


# 依据薪资数值获取本系统薪资id
def getSalaryIdBySalaryValue(salary):
    if salary <= 1000:
        return 391000
    elif salary <= 2000:
        return 391001
    elif salary <= 3000:
        return 391002
    elif salary <= 5000:
        return 391003
    elif salary <= 8000:
        return 391004
    elif salary <= 12000:
        return 391005
    elif salary <= 20000:
        return 391006
    elif salary <= 25000:
        return 391007
    else:
        return 391008


# 依据本系统薪资id获取薪资区间
def getSalaryMinMaxBySalaryId(salaryid):
    if isinstance(salaryid, basestring):
        salaryid = str2int(salaryid)
    if salaryid == 391000:
        return (0, 1000)
    elif salaryid == 391001:
        return (1000, 2000)
    elif salaryid == 391002:
        return (2000, 3000)
    elif salaryid == 391003:
        return (3000, 5000)
    elif salaryid == 391004:
        return (5000, 8000)
    elif salaryid == 391005:
        return (8000, 12000)
    elif salaryid == 391006:
        return (12000, 20000)
    elif salaryid == 391007:
        return (20000, 25000)
    elif salaryid == 391008:
        return (25000, 50000)
    else:
        return (0, 0)


# 依据简历最高学历转ID
def getEduHighId(highedu):
    if highedu == u'初中':
        return 10
    elif highedu == u'高中':
        return 20
    elif highedu == [u'中专', u'中技']:
        return 21
    elif highedu in [u'大专', u'大專']:
        return 30
    elif highedu == u'本科':
        return 40
    elif highedu in [u'硕士', u'研究生']:
        return 50
    elif highedu == u'博士':
        return 60
    elif highedu == u'博士后':
        return 70
    else:
        return 0


# 依据工作年限转ID
def getWorkExpId(workexp):
    """
    返回系统工作年限ID
    :type workexp: str
    :param workexp: 工作年限(1,2,3...)
    """
    if not workexp:
        return '371006'
    if isinstance(workexp, basestring):
        workexp = str2int(workexp)
    if workexp == 0:
        return '371000'
    elif workexp in [1, 2]:
        return '371001'
    elif workexp in [3, 5]:
        return '371002'
    elif workexp in [6, 7]:
        return '371003'
    elif workexp in [8, 9]:
        return '371004'
    elif workexp >= 10:
        return '371005'

# 生日
def getBirthDay(birthstr):
    posm = birthstr.find(u'月')
    posd = birthstr.find(u'日')
    year, month, day = birthstr[0: 4], birthstr[5: posm], birthstr[posm + 1: posd]
    year = str2int(year, 1900)
    month = str2int(month, 1)
    day = str2int(day, 1)
    return date(year, month, day)

# 当前时间戳
def currentTimestamp():
    return int(time.time() * 100)


# 创建文件夹
def makedirs(filepath):
    if not os.path.isdir(filepath):
        try:
            os.makedirs(filepath)
        except OSError as exc:  # Python >2.5 (except OSError, exc: for Python <2.5)
            if exc.errno == errno.EEXIST and os.path.isdir(filepath):
                pass
            else:
                raise


# 动态获取当前运行的函数名
def currentFuncName(instance=None):
    # import sys
    # return sys._getframe().f_code.co_name
    import inspect
    if instance:
        if inspect.isclass(instance):
            return '{}.{}'.format(instance.__name__, inspect.stack()[1][3])
        else:
            return '{}.{}'.format(instance.__class__.__name__, inspect.stack()[1][3])
    else:
        return inspect.stack()[1][3]


# 获取html页面隐藏的post域
def input_field_value(data):
    ret = {}
    hxs = Selector(None, data)
    # input
    hiddenFields = hxs.xpath('//input')
    for field in hiddenFields:
        value = ''
        type = first_text(field.xpath('@type'))
        if type == 'submit' or type == 'button' or type == 'image':
            continue
        elif type == 'radio':
            checked = first_text(field.xpath('@checked'))
            if checked != 'checked':
                continue
        elif type == 'checkbox':
            checked = first_text(field.xpath('@checked'))
            if checked != 'checked':
                continue
            else:
                value = first_text(field.xpath('@value'))
                if not value:
                    value = 'on'
        name = first_text(field.xpath('@name'))
        if name != '':
            if value == '':
                value = first_text(field.xpath('@value'))
            ret[name] = value
    # select
    selectFields = hxs.xpath('//select')
    for field in selectFields:
        name = first_text(field.xpath('@name'))
        if name != '':
            value = first_text(field.xpath('option[@selected="selected"]/@value'))
            ret[name] = value
    # textarea
    textFields = hxs.xpath('//textarea')
    for field in textFields:
        name = first_text(field.xpath('@name'))
        if name != '':
            value = first_text(field.xpath('text()'))
            ret[name] = value.lstrip('\r\n').rstrip('\r\n')
    return ret

# 字符串转码


def str_encode(string='', encoding=None, errors='strict'):
    return unicode(string, encoding, errors)


def str_decode(value='', encoding=None, errors='strict'):
    return value.decode(encoding, errors)

# 依据参数组装http的post参数
# files = {'file': {'filename':filename,'size':filesize,'type':filetype,'filedata':data}}


def assemble_post_data(fields, files=None):
    # boundary = mimetools.choose_boundary()
    boundary = '----WebKitFormBoundaryLQFqklkrHdanI1nP'
    CRLF = '\r\n'
    data = []
    for key in fields:
        data.append('--' + boundary)
        data.append('Content-Disposition: form-data; name="' + key + '"')
        data.append('')
        data.append(fields[key])
    if files:
        for key in files:
            data.append('--' + boundary)
            data.append('Content-Disposition: form-data; name="' +
                        key + '"; filename="' + files[key]['filename'] + '"')
            data.append('Content-Type: "' + files[key]['type'] + '"')
            data.append('')
            data.append(files[key]['filedata'])
    # 尾部
    data.append('--' + boundary + '--')
    data.append('')
    #
    body = CRLF.join([str(item) for item in data])
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return {'content_type': content_type, 'body': body}
