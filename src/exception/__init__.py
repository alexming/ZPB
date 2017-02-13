#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: exception/__init__.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-11-14 12:55
#########################################################################


# zpb
from zpb.conf import logger


class BaseError(BaseException):

	def __init__(self, company, siteid, message):
		super(BaseError, self).__init__()
		self.code = 0
		self.company = company
		self.siteid = siteid
		self.message = message

	def __str__(self):
		from zpb.business.siteconfig import SiteConfig
		return u'企业<{}>,招聘平台<{}>,错误码<{}>,原因<{}>'.format(
			self.company, SiteConfig.getSiteNameById(self.siteid), self.code, self.message)


class CompanyAccountNotBindError(BaseError):
	
	def __init__(self, company, siteid, message):
		super(CompanyAccountNotBindError, self).__init__(company, siteid, message)
		self.code = 10000
		logger.error(self)


class CompanyAccountUnBindError(BaseError):	

	def __init__(self, company, siteid, message):
		super(CompanyAccountUnBindError, self).__init__(company, siteid, message)
		self.code = 10010
		logger.error(self)


class CompanyAccountNotSupportError(BaseError):

	def __init__(self, company, siteid, message):
		super(CompanyAccountNotSupportError, self).__init__(company, siteid, message)
		self.code = 10020
		logger.error(self)


class CompanyAccountInvalidError(BaseError):

	def __init__(self, company, siteid, message):
		super(CompanyAccountInvalidError, self).__init__(company, siteid, message)
		self.code = 10030
		logger.error(self)


class JobNotDistributeError(BaseError):

	def __init__(self, company, siteid, message):
		super(JobNotDistributeError, self).__init__(company, siteid, message)
		self.code = 10040
		logger.error(self)	


class UnHandleRuntimeError(BaseError):

	def __init__(self, e):
		self.code = 20000
		self.message = u'内部服务错误'
		import traceback
		print traceback.format_exc(e)
		self.realmsg = e.message

	def __str__(self):
		return u'错误码<{}>,原因<{}>'.format(self.code, self.realmsg)


class InvalidParamError(BaseError):

	def __init__(self, company, siteid, message):
		super(InvalidParamError, self).__init__(company, siteid, message)
		self.code = 20010
		logger.error(self)


class InvalidJobParamError(BaseError):

	def __init__(self, message):
		self.code = 20020
		self.message = u'职位同步参数错误'
		self.realmsg = message

	def __str__(self):
		return u'错误码<{}>,原因<{}>'.format(self.code, self.realmsg)


class InvalidJobLocationError(BaseError):

	def __init__(self, message):
		self.code = 20030
		self.message = u'无法查找到职位'
		self.realmsg = message

	def __str__(self):
		return u'错误码<{}>,原因<{},{}>'.format(self.code, self.message, self.realmsg)


class DBOperateError(BaseError):

	def __init__(self, frame, e):
		self.code = 30000
		self.message = u'数据操作异常'
		self.frame = frame
		self.realmsg = e.message
		logger.error(self)

	def __str__(self):
		return u'错误码<{}>,<{}>,原因<{}>'.format(self.code, self.frame, self.realmsg)


class JobOperateError(BaseError):

	def __init__(self, message):
		self.code = 30010
		self.message = message

	def __str__(self):
		return u'错误码<{}>,原因<{}>'.format(self.code, self.message)


class NetworkError(BaseError):

	def __init__(self, message):
		self.code = 40000
		self.message = u'网络请求异常'
		self.realmsg = message

	def __str__(self):
		return u'错误码<{}>,原因<{}>'.format(self.code, self.realmsg)


class DamaError(BaseError):

	def __init__(self, message):
		self.code = 50000
		self.message = u'内部服务错误'
		self.realmsg = message

	def __str__(self):
		return u'错误码<{}>,原因<{}>'.format(self.code, self.realmsg)


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