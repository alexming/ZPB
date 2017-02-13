#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/resumeagress.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-21 11:35
#########################################################################


# stdlib
import os
import os.path
import sys
sys.path.append('../../')
reload(sys)
sys.setdefaultencoding('utf-8')
from codecs import *
# zpb
from zpb.database.DBManager import TQDbPool


def run_main():
    """
    统计深圳地区求职者简历信息，并存储为html文档
    """
    filepath = '/zpb/zpb/resume/shenzhen/'
    command = u"select from_site_code, org_resume from res_resume_base "\
            u"where source=0 and now_location = '广东-深圳' and from_site_code<>'' and org_resume is not null"
    rows = TQDbPool.query('zpb', command)
    for row in rows:
        filename = row['from_site_code'] + '.html'
        fp = open(os.path.join(filepath, filename), mode='a', encoding='utf-8')
        try:
            fp.write(row['org_resume'])
        finally:
            fp.flush()
            fp.close()

if __name__ == '__main__':
    run_main()
