#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: datatools/redisdictionary.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-19 11:14
#########################################################################


''' 用於redis字典生成
    从原数据源(SQL数据库)加载系统字典,生成json格式数据导入redis

'''

# stdlib
import sys
sys.path.append('..')
reload(sys)
sys.setdefaultencoding('utf-8')
import json
# zpb
from zpb.database.DBManager import TQDbPool
from zpb.cache.rediscache import TQRedis


def getkey(siteid, typeid):
    # typeid maybe 0, but must not '' or None
    if typeid is None or typeid == '':
        return
    keytype = siteid.split('_')[0]
    if keytype == 'zl':
        keyprefix = 'RD2'
    elif keytype == '51':
        keyprefix = 'WY'
    elif keytype == 'cj':
        keyprefix = 'CJ'
    elif keytype == 'lg':
        keyprefix = 'LAG'
    elif keytype == '58':
        keyprefix = 'WUBA'
    elif keytype == 'ganj':
        keyprefix = 'GANJ'
    elif keytype == 'liep':
        keyprefix = 'LIEP'
    else:
        return
    keytype = siteid[len(keytype) + 1:]
    if keytype == 'job_type':
        keysuffix = 'JobType'
    elif keytype == 'major':
        keysuffix = 'Major'
    elif keytype == 'language':
        keysuffix = 'Lang'
    elif keytype == 'language_skill':
        keysuffix = 'LangSkill'
    elif keytype in ['edu', 'edu_limit']:
        keysuffix = 'Degree'
    elif keytype == 'area':
        keysuffix = 'Area'
    elif keytype == 'city':
        keysuffix = 'City'
    elif keytype in ['welfare', 'welfare_1', 'welfare_2']:
        keysuffix = 'Welfare'
    elif keytype in ['salary', 'salary_month']:
        keysuffix = 'MonthPay'
    elif keytype == 'salary_year':
        keysuffix = 'YearPay'
    elif keytype in ['exp', 'exp_limit']:
        keysuffix = 'WorkYear'
    elif keytype == 'computer_skill':
        keysuffix = 'Computer'
    elif keytype == 'salary_year':
        keysuffix = 'YearPay'
    elif keytype == 'salary_type':
        keysuffix = 'PayType'
    elif keytype == 'work_time':
        keysuffix = 'WorkTime'
    elif keytype == 'sex':
        keysuffix = 'Gender'
    else:
        return
    return '{}:{}:{}'.format(keyprefix, keysuffix, typeid)


def sql2redis():
    R = TQRedis.GetRedis('dict_cache_1')
    command = "select * from zhaopin_dict where siteid in ('liep_area','liep_edu','liep_sex','liep_welfare')"
    rows = TQDbPool.query('zpb', command)
    for row in rows:
        siteid = row['siteid']
        itemid = row['itemid']
        key = getkey(siteid, itemid)
        if key:
            typeid = row['typeid']
            typename = row['typename']
            jsonvalue = json.loads('{}')
            jsonvalue['id'] = typeid
            jsonvalue['name'] = typename
            R.set(key, json.dumps(jsonvalue))
        else:
            print u'没有找到字典{}:{}:{}'.format(siteid, itemid, row['typename'])
    '''
    # reverse
    # command = "select * from zhaopin_dict_reverse where dict_type='job_type' and parent_id>0"
    # 线下福利标签对应赶集线上多个福利标签
    # command = "select * from zhaopin_dict_reverse where dict_type in ('job_type', 'welfare')"
    # command = "select * from zhaopin_dict_reverse where dict_type='job_type' and parent_id>0 and 1=2"
    # command = "select * from zhaopin_dict_reverse where dict_type='industry'"
    '''
    # LAG
    # command = "select * from zhaopin_dict_reverse where dict_type = 'job_type'"
    # LIEP
    command = "select * from zhaopin_dict_reverse where dict_type in ('job_type')"
    rows = TQDbPool.query('zpb', command)
    for row in rows:
        dictid = row['dict_val']
        #
        zl_typeid = row['zl_type_id']
        zl_typename = row['zl_type_name']
        zl_parentid = row['zl_parent_id']
        #
        wy_typeid = row['51_type_id']
        wy_typename = row['51_type_name']
        #
        cj_typeid = row['cj_type_id']
        cj_typename = row['cj_type_name']
        #
        lg_typeid = row['lg_type_level']
        lg_typename = row['lg_type_name']
        lg_parentid = row['lg_parent_name']
        #
        wb_typeid = ''
        wb_typeid = row['58_type_id']
        wb_typename = row['58_type_name']
        wb_parentid = row['58_parent_id']
        #
        gj_typeid = ''
        gj_typeid = row['ganj_type_id']
        gj_typename = row['ganj_type_name']
        gj_pyname = row['ganj_py_name']
        gj_parentid = row['ganj_parent_id']
        #
        lp_typeid = ''
        lp_typeid = row['liep_type_id']
        lp_typename = row['liep_type_name']
        # 写入redis缓存
        if row['dict_type'] == 'job_type':
            fmtkey = '{{}}:JobType:{}'.format(dictid)
        elif row['dict_type'] == 'welfare':
            fmtkey = '{{}}:Welfare:{}'.format(dictid)
        else:
            raise BaseException(u'未知的字典类型<>'.format(row['dict_type']))
        # fmtkey = '{{}}:Industry:{}'.format(dictid)
        data = json.loads('{}')
        '''
        # zl
        if zl_typeid:
            data['id'] = zl_typeid
            data['name'] = zl_typename
            # data['parentid'] = zl_parentid
            R.set(fmtkey.format('RD2'), json.dumps(data))
        # wy
        if wy_typeid:
            data['id'] = wy_typeid
            data['name'] = wy_typename
            # data['parentid'] = ''
            R.set(fmtkey.format('WY'), json.dumps(data))
        # cj
        if cj_typeid:
            data['id'] = cj_typeid
            data['name'] = cj_typename
            # data['parentid'] = ''
            R.set(fmtkey.format('CJ'), json.dumps(data))
        # lg
        if lg_typename:
            data['id'] = lg_typeid
            data['name'] = lg_typename
            data['parentid'] = lg_parentid
            R.set(fmtkey.format('LAG'), json.dumps(data))
        # 58
        if wb_typeid:
            data['id'] = wb_typeid
            data['name'] = wb_typename
            data['parentid'] = wb_parentid
            R.set(fmtkey.format('WUBA'), json.dumps(data))
        # gj
        if gj_typeid:
            data['id'] = gj_typeid
            data['name'] = gj_typename
            data['pyname'] = gj_pyname
            data['parentid'] = gj_parentid
            R.set(fmtkey.format('GANJ'), json.dumps(data))
        '''
        # liep
        if lp_typeid:
            data['id'] = lp_typeid
            data['name'] = lp_typename
            R.set(fmtkey.format('LIEP'), json.dumps(data))


def sql2redis_third_my():
    R = TQRedis.GetRedis('dict_cache_2')
    #command = 'select * from zhaopin_dict'
    # command = 'select * from zhaopin_dict where siteid=\'lg_job_type\' and typeid=1'
    command = 'select * from zhaopin_dict where siteid like "liep_%"'
    rows = TQDbPool.query('zpb', command)
    for row in rows:
        siteid = row['siteid']
        typeid = row['typeid']
        if siteid == 'zl_job_type':
            typeid = '{0}:{1}'.format(row['parentid'], typeid)
        elif siteid == 'lg_job_type':
            typeid = row['typename']
        elif siteid == 'cj_welfare':
            typeid = row['typename']
        elif siteid == '58_welfare':
            typeid = row['typename']
        key = getkey(siteid, typeid)
        if key:
            itemid = row['itemid']
            if siteid == 'zl_area':
                jsonvalue = json.loads('{}')
                jsonvalue['id'] = itemid
                jsonvalue['third_parentid'] = row['parentid']
                itemid = json.dumps(jsonvalue)
            if itemid:
                R.set('@{0}'.format(key), itemid)
        else:
            print u'没有找到字典{}:{}:{}'.format(siteid, typeid, row['typename'])


# 系统区域信息上下级关系以json存储redis
def area2redis():
    R = TQRedis.GetRedis('dict_cache_1')
    command = '''select distinct b.addr_id as hkey,\
        b.addr_name as cityname,c.addr_id as provinceid,c.addr_name as provincename from sys_address a \
        inner join sys_address b on a.parent_id=b.addr_id inner join sys_address c on b.parent_id=c.addr_id'''
    rows = TQDbPool.query('zpb', command)
    print u'开始维护城市-省份上下级关系'
    for row in rows:
        jsonval = json.loads('{}')
        jsonval['cityname'] = row['cityname']
        jsonval['provinceid'] = row['provinceid']
        jsonval['provincename'] = row['provincename']
        # R.delete('City:{0}'.format(row['hkey']))
        R.set('Common:City:{0}'.format(row['hkey']), json.dumps(jsonval))
    print u'城市-省份上下级关系维护完成'


# 系统城市名称对照(用於拉勾城市模糊匹配)
def cityname2redis():
    R = TQRedis.GetRedis('dict_cache_2')
    command = '''select addr_name as hkey,addr_id,parent_id from sys_address where length(addr_code)=4'''
    rows = TQDbPool.query('zpb', command)
    print u'开始维护城市名称模糊查找关系'
    for row in rows:
        jsonval = json.loads('{}')
        jsonval['cityid'] = row['addr_id']
        jsonval['provinceid'] = row['parent_id']
        R.set('@Common:City:{0}'.format(row['hkey']), json.dumps(jsonval))
    print u'城市名称模糊查找关系维护完成'


# 职位类别大类与中类映射
def sql2redis_sub_jobtype():
    R = TQRedis.GetRedis('dict_cache_2')
    command = "select dict_val,parent_id from sys_dict "\
        "where parent_id between 1 and 11 and "\
        "dict_type='job_type' and dict_level=1"
    rows = TQDbPool.query('zpb', command)
    for row in rows:
        # R.delete('@:SubJobType:{0}'.format(row['dict_val']))
        R.set('@SubJobType:{0}'.format(row['dict_val']), row['parent_id'])


if __name__ == '__main__':
    print u'开始从数据源加载字典进行处理'
    print u'线下->线上映射'
    sql2redis()
    print u'线上->线下映射'
    sql2redis_third_my()
    # sql2redis_sub_jobtype()
    # area2redis()
    # cityname2redis()
    print u'字典生成完毕'
