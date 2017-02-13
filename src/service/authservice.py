#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/authservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:06
#########################################################################


# zpb
from zpb.core.Singleton import Singleton
from zpb.business.model.companybind import Bind


class AuthService(Singleton):

    # 不能缓存，前端数据修改后，不能及时反馈后系统

    def getBindByCompanyIdAndSiteId(self, companyid, siteid):
        return Bind.queryOne(companyid, siteid)

    def getBindSiteByCompanyId(self, companyid):
        return Bind.loadValidSiteByCompanyId(companyid)

    # 依据key更新简历最后导入时间
    def updateBindImportTimeByCompanyIdAndSiteId(self, companyid, siteid):
        Bind.updateBindImportTimeByCompanyIdAndSiteId(companyid, siteid)


if __name__ == '__main__':
    pass