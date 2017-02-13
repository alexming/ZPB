#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/decorate/downdecorator.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-01 17:54
#########################################################################


# stdlib
import json
from datetime import datetime
# zpb
from zpb.cache.rediscache import ForwardDictCli, BackwardDictCli
from zpb.business.siteconfig import SiteConfig
from zpb.utils.tools import clearHtmlTag, getSalaryIdBySalaryValue, getWorkExpId
from zpb.business.model.basemodel import DBInstance
from zpb.conf import logger


def dict_info(dict_type, key, defaultvalue=''):
    # 返回默认值
    if key is None or key == '': return defaultvalue
    value = ForwardDictCli.get('@{}:{}'.format(dict_type, key))
    if value:
        return value
    else:
        if defaultvalue:
            return defaultvalue
        else:
            raise BaseException(u'无法从缓存字典找到关键字=@{}:{}的数据,请检查缓存字典.'.format(dict_type, key))


def getProvinceCode(citycode):
    """
    依据城市Code获取省份Code
    """
    if not citycode:
        return ''
    value = BackwardDictCli.get('Common:City:{}'.format(citycode))
    if value:
        js = json.loads(value)
        return js['provinceid']
    else:
        return ''


def blur_key(dict_type, key):
    keys = ForwardDictCli.keys('@{}:*{}*'.format(dict_type, key))
    if len(keys) > 0:
        return keys[0].split(':')[-1]


class RD2Decorate(object):

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_type_level_3 = dict_info('RD2:JobType', '{0}:{1}'.format(grab.positiontype, grab.positiontype2))
        job.job_type_level_2 = job.job_type_level_3[0: -2].lstrip('0')
        job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        job.job_mode = 1 if grab.worktype == '2' else 2
        workarea = grab.workarea.split('@')
        if len(workarea) > 0:
            cityjson = dict_info('RD2:Area', workarea[0])
            cityinfo = json.loads(cityjson)
            job.location_city_id = cityinfo['id']
            # 北京
            if job.location_city_id == '3221':
                job.location_province_id = '528'
            # 上海
            elif job.location_city_id == '545':
                job.location_province_id = '2579'
            # 天津
            elif job.location_city_id == '3234':
                job.location_province_id = '3268'
            # 重庆
            elif job.location_city_id == '1244':
                job.location_province_id = '1243'
            else:
                provjson = dict_info('RD2:Area', cityinfo['third_parentid'])
                provinfo = json.loads(provjson)
                job.location_province_id = provinfo['id']
        if len(workarea) > 1:
            areajson = dict_info('RD2:Area', workarea[1])
            areainfo = json.loads(areajson)
            job.location_area_id = areainfo['id']
        job.location_detail = grab.workaddr
        # 薪资空白=薪资面议
        job.salary_id = dict_info('RD2:MonthPay', grab.salary, '391009')
        job.recruit_num = grab.need
        # -1=学历不限
        job.lmt_edu_id = dict_info('RD2:Degree', grab.education, '0')
        job.lmt_work = dict_info('RD2:WorkYear', grab.workexperience, '371006')
        job.lmt_sex_ids = '391012'
        job.receiver_emails = grab.emails
        job.work_responsibility = clearHtmlTag(grab.jobdesc)
        job.job_status = 10
        job.is_valid = 'T' if grab.status in ['3', '2'] else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.end_date = grab.dateend
        job.publish_date = grab.datepublish
        job.last_sync_time = datetime.today()
        # 福利标签
        for item in grab.welfares.split(','):
            if not item:
                continue
            tagid = dict_info('RD2:Welfare', item)
            job.props.append(tagid)

rd2 = RD2Decorate()


class WYJobDecorate(object):

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_type_level_3 = dict_info('WY:JobType', grab.positiontype)
        job.job_type_level_2 = int(job.job_type_level_3[0: -2])
        job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        job.job_mode = 1 if grab.worktype == '0' else 2
        # 直辖市与深圳
        if grab.workarea[0: 2] in ['01', '02', '04', '05', '06']:
            if grab.workarea[0: 2] == '01':
                job.location_province_id = '528'
                job.location_city_id = '3221'
            elif grab.workarea[0: 2] == '02':
                job.location_province_id = '2579'
                job.location_city_id = '545'
            elif grab.workarea[0: 2] == '04':
                job.location_province_id = '1217'
                job.location_city_id = '1513'
            elif grab.workarea[0: 2] == '05':
                job.location_province_id = '3268'
                job.location_city_id = '3234'
            elif grab.workarea[0: 2] == '06':
                job.location_province_id = '1243'
                job.location_city_id = '1244'
            if grab.workarea[2:] != '0000':
                job.location_area_id = dict_info('WY:Area', grab.workarea)
        else:
            # 省
            if grab.workarea[2:] == '0000':
                job.location_province_id = dict_info('WY:Area', grab.workarea)
            elif grab.workarea[4:] == '00':
                job.location_province_id = dict_info('WY:Area', grab.workarea[0: 2]+ '0000')
                job.location_city_id = dict_info('WY:Area', grab.workarea)
            else:
                job.location_province_id = dict_info('WY:Area', grab.workarea[0: 2] + '0000')
                job.location_city_id = dict_info('WY:Area', grab.workarea[0: 4] + '00')
                job.location_area_id =dict_info('WY:Area', grab.workarea)
        job.location_detail = grab.workaddr
        # 平均薪资,用於自定义薪资/日薪/时薪解析
        avgsalary = 0
        # 薪资年薪
        if grab.salarytype == '4':
            job.salary_id = dict_info('WY:YearPay', grab.yearsalary, '391009')
        # 薪资月薪
        elif grab.salarytype == '1':
            job.salary_id = dict_info('WY:MonthPay', grab.monthsalary, '391009')
        # 薪资日薪
        elif grab.salarytype == '3':
            avgsalary = int(round(grab.customsalary * 30 / 1000.00) * 1000.00)
        # 薪资时薪
        elif grab.salarytype == '2':
            avgsalary = int(round(grab.customsalary * 8 * 30 / 1000.00) * 1000.00)
        # 自定义薪资
        if grab.salarymin and grab.salarymax:
            minsalary = int(round(grab.salarymin / 1000.00) * 1000.00)
            maxsalary = int(round(grab.salarymax / 1000.00) * 1000.00)
            avgsalary = int(round(minsalary + maxsalary) / 2)
        if avgsalary > 0:
            job.salary_id = getSalaryIdBySalaryValue(avgsalary)
        job.recruit_num = grab.need
        # -1=学历不限
        job.lmt_edu_id = dict_info('WY:Degree', grab.education, '0')
        job.lmt_work = dict_info('WY:WorkYear', grab.workexperience, '371006')
        job.lmt_sex_ids = '391012'
        job.receiver_emails = grab.emails
        job.work_responsibility = grab.jobdesc
        job.lmt_age_min = grab.agefrom if grab.agefrom else 0
        job.lmt_age_max = grab.ageto if grab.ageto else 0
        if grab.language1 == '01':
            job.lmt_lang_ids = dict_info('WY:LangSkill', grab.languagelevel1)
        if grab.language2 == '01':
            job.lmt_lang_ids = dict_info('WY:LangSkill', grab.languagelevel2)
        lmt_major_ids = []
        if grab.major1:
            lmt_major_ids.append(dict_info('WY:Major', grab.major1))
        if grab.major2:
            lmt_major_ids.append(dict_info('WY:Major', grab.major2))
        job.lmt_major_ids = ','.join(lmt_major_ids)
        job.search_keywords = ','.join([item for item in grab.keywords.split(' ') if item])
        job.job_status = 10
        job.is_valid = 'T' if grab.status == 10 else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.last_sync_time = datetime.today()
        # 福利标签
        for item in grab.welfares.split(';'):
            if not item:
                continue
            tagid = dict_info('WY:Welfare', item, 'None')
            if tagid != 'None':
                job.props.append(tagid)
            else:
                job.tags.append(item)

wyjob = WYJobDecorate()


class CJOLDecorate(object):

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_mode = grab.worktype
        # CJOL可能未填写
        if grab.positiontype:
            positiontype = grab.positiontype.split('|')[0]
            if len(positiontype) == 4:
                job.job_type_level_1 = dict_info('CJ:JobType', positiontype[0: 2])
                job.job_type_level_2 = dict_info('CJ:JobType', positiontype)
                job.job_type_level_3 = dict_info('CJ:JobType', positiontype + '99')
            elif len(positiontype) == 6:
                job.job_type_level_1 = dict_info('CJ:JobType', positiontype[0: 2])
                job.job_type_level_2 = dict_info('CJ:JobType', positiontype[0: 4])
                job.job_type_level_3 = dict_info('CJ:JobType', positiontype)
        else:
            job.job_type_level_1 = 11
            job.job_type_level_2 = 19
            job.job_type_level_3 = 1901
        # 直辖市处理
        if grab.workarea[0: 2] in ['30', '31', '32', '63']:
            # 北京
            if grab.workarea[0: 2] == '31':
                job.location_province_id = '528'
                job.location_city_id = '3221'
            # 上海
            elif grab.workarea[0: 2] == '30':
                job.location_province_id = '2579'
                job.location_city_id = '545'
            # 重庆
            elif grab.workarea[0: 2] == '63':
                job.location_province_id = '1243'
                job.location_city_id = '1244'
            # 天津
            elif grab.workarea[0: 2] == '32':
                job.location_province_id = '3268'
                job.location_city_id = '3234'
            if len(grab.workarea) == 6:
                job.location_area_id = dict_info('CJ:Area', grab.workarea)
        else:
            if len(grab.workarea) == 2:
                job.location_province_id = dict_info('CJ:Area', grab.workarea)
            elif len(grab.workarea) == 4:
                job.location_province_id = dict_info('CJ:Area', grab.workarea[0: 2])
                job.location_city_id = dict_info('CJ:Area', grab.workarea)
            elif len(grab.workarea) == 6:
                job.location_province_id = dict_info('CJ:Area', grab.workarea[0: 2])
                job.location_city_id = dict_info('CJ:Area', grab.workarea[0: 4])
                job.location_area_id = dict_info('CJ:Area', grab.workarea)
        job.location_detail = grab.workaddr
        job.salary_id = dict_info('CJ:MonthPay', grab.monthsalary, '391009')
        # 自定义薪资
        if grab.salarymin and grab.salarymax:
            minsalary = int(round(grab.salarymin / 1000.00) * 1000.00)
            maxsalary = int(round(grab.salarymax / 1000.00) * 1000.00)
            avgsalary = int(round(minsalary + maxsalary) / 2)
            if avgsalary > 0:
                job.salary_id = getSalaryIdBySalaryValue(avgsalary)
        job.recruit_num = grab.need
        # -1=学历不限
        job.lmt_edu_id = dict_info('CJ:Degree', grab.education, '0')
        job.lmt_work = dict_info('CJ:WorkYear', grab.workexperience, '371006')
        job.lmt_sex_ids = dict_info('CJ:Gender', grab.sex, '391012')
        job.receiver_emails = grab.emails
        job.work_responsibility = grab.jobdesc
        job.lmt_age_min = grab.agefrom
        job.lmt_age_max = grab.ageto
        # 仅处理英语,默认熟练
        if grab.language1 == '5':
            job.lmt_lang_ids = '391024'
        lmt_major_ids = []
        for major in grab.major1.split(','):
            lmt_major_ids.append(dict_info('CJ:Major', major))
        job.lmt_major_ids = ','.join(lmt_major_ids)
        job.search_keywords = ','.join([item for item in grab.keywords.split(' ') if item])
        job.job_status = 10
        job.is_valid = 'T' if grab.status == 10 else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.last_sync_time = datetime.today()
        # 福利标签
        for item in grab.welfares.split(';'):
            if not item:
                continue
            tagid = dict_info('CJ:Welfare', item, 'None')
            if tagid != 'None':
                job.props.append(tagid)
            else:
                job.tags.append(item)

cjol = CJOLDecorate()


class LAGDecorate(object):

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_mode = 1 if grab.worktype == u'全职' else 2
        job.job_status = 10
        job.is_valid = 'T' if grab.status == 10 else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        if grab.positiontype2:
            job.job_type_level_3 = dict_info('LAG:JobType', grab.positiontype2)
            job.job_type_level_2 = int(job.job_type_level_3[0: -2])
            job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        elif grab.positiontype:
            job.job_type_level_3 = dict_info('LAG:JobType', grab.positiontype)
            job.job_type_level_2 = int(job.job_type_level_3[0: -2])
            job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        # 模糊匹配城市
        citykey = blur_key('Common:City', grab.workarea)
        if citykey:
            cityjson = dict_info('Common:City', citykey)
            cityinfo = json.loads(cityjson)
            job.location_city_id = cityinfo['cityid']
            job.location_province_id = cityinfo['provinceid']
        else:
            job.location_province_id = 4241
            logger.error(u'拉勾职位工作城市{0}没有模糊匹配结果!'.format(grab.workarea))
        job.location_detail = ''
        # 薪资范围
        avgsalary = int(round(grab.salarymin * 1000.00 + grab.salarymax * 1000.00) / 2)
        if avgsalary > 0:
            job.salary_id = getSalaryIdBySalaryValue(avgsalary)
        job.recruit_num = 0
        # -1=学历不限
        job.lmt_edu_id = dict_info('LAG:Degree', grab.education, '0')
        job.lmt_work = dict_info('LAG:WorkYear', grab.workexperience, '371006')
        job.lmt_sex_ids = '391012'
        job.receiver_emails = grab.emails.replace(',', ';')
        job.work_responsibility = clearHtmlTag(grab.jobdesc.lstrip("'").rstrip("'"))
        job.last_sync_time = datetime.today()

lag = LAGDecorate()


class WubaDecorate(object):

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_mode = grab.worktype
        job.job_status = 10
        job.is_valid = 'T' if grab.status == 10 else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.location_detail = grab.workaddr
        job.recruit_num = grab.need
        job.job_type_level_3 = dict_info('WUBA:JobType', grab.positiontype2)
        job.job_type_level_2 = int(job.job_type_level_3[0: -2])
        job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        arr_area = grab.workarea.split('|')
        # 市|区|地段
        if len(arr_area) == 3:
            job.location_city_id = dict_info('WUBA:Area', arr_area[0])
            job.location_area_id = dict_info('WUBA:Area', arr_area[1])
        # 全国|市
        elif len(arr_area) == 2:
            if arr_area[1] != '-1':
                job.location_city_id = dict_info('WUBA:Area', arr_area[1])
            else:
                job.location_province_id = '4241'
        if job.location_city_id:
            job.location_province_id = getProvinceCode(job.location_city_id)
        job.salary_id = dict_info('WUBA:MonthPay', grab.monthsalary, '391009')
        job.lmt_edu_id = dict_info('WUBA:Degree', grab.education, '0')
        job.lmt_work = dict_info('WUBA:WorkYear', grab.workexperience, '371006')
        job.lmt_sex_ids = '391012'
        job.receiver_emails = grab.emails
        job.work_responsibility = grab.jobdesc
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.last_sync_time = datetime.today()
        # 福利标签
        for item in grab.welfares.split('|'):
            if not item:
                continue
            tagid = dict_info('WUBA:Welfare', item, 'None')
            if tagid != 'None':
                job.props.append(tagid)
            else:
                job.tags.append(item)

wuba = WubaDecorate()


class GanJDecorate(object):

    # private welfares dictionary
    WELFARES = {
         '2': u'带薪年假',
         '8': u'包吃',
         '9': u'包住',
        '10': u'餐补',
        '12': u'房补',
        '13': u'养老保险',
        '14': u'医疗保险',
        '15': u'工伤保险',
        '16': u'失业保险',
        '17': u'生育保险',
        '18': u'住房公积金',
        '20': u'交通补助',
        '21': u'话补',
        '22': u'加班补助',
        '23': u'做五休二',
        '24': u'年底双薪',
        '27': u'员工体检',
    }

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_mode = grab.worktype
        job.job_status = 10
        job.is_valid = 'T' if grab.status == 10 else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.location_detail = grab.workaddr
        job.recruit_num = grab.need
        job.job_type_level_3 = dict_info('GANJ:JobType', grab.positiontype2)
        job.job_type_level_2 = int(job.job_type_level_3[0: -2])
        job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        arr_area = grab.workarea.split('|')
        # 市|区|地段
        if len(arr_area) == 3:
            job.location_city_id = dict_info('GANJ:Area', arr_area[0])
            job.location_area_id = dict_info('GANJ:Area', arr_area[1])
        # 全国|市
        elif len(arr_area) == 2:
            if arr_area[1] != '-1':
                job.location_city_id = dict_info('GANJ:Area', arr_area[1])
            else:
                job.location_province_id = '4241'
        if job.location_city_id:
            job.location_province_id = getProvinceCode(job.location_city_id)
        job.salary_id = dict_info('GANJ:MonthPay', grab.monthsalary, '391009')
        job.lmt_edu_id = dict_info('GANJ:Degree', grab.education, '0')
        job.lmt_work = dict_info('GANJ:WorkYear', grab.workexperience, '371006')
        if grab.language1:
            job.lmt_lang_ids = '391024'
        job.lmt_age_min = grab.agefrom
        job.lmt_age_max = grab.ageto
        job.receiver_emails = grab.emails
        job.work_responsibility = grab.jobdesc
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.last_sync_time = datetime.today()
        job.lmt_sex_ids = '391012'
        # 福利标签
        for item in grab.welfares.split(','):
            if not item:
                continue
            tagid = dict_info('GANJ:Welfare', item, 'None')
            if tagid != 'None':
                job.props.append(tagid)
            elif item in self.WELFARES:
                job.tags.append(self.WELFARES[item])

ganj = GanJDecorate()


class LiePDecorate(object):

    def pack_position(self, grab, job):
        job.company_id = grab.companyid
        job.from_site_id = grab.siteid
        job.from_site_code = grab.jobid
        job.job_name = grab.jobtitle
        job.job_mode = grab.worktype
        job.job_status = 10
        job.is_valid = 'T' if grab.status == 10 else 'F'
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.location_detail = grab.workaddr
        job.recruit_num = grab.need
        # 仅取第一个
        jobtype = grab.positiontype2.split(',')[0]
        # 其他
        if jobtype == '350070':
            jobtype = '340080'
        job.job_type_level_3 = dict_info('LIEP:JobType', jobtype)
        job.job_type_level_2 = int(job.job_type_level_3[0: -2])
        job.job_type_level_1 = dict_info('SubJobType', job.job_type_level_2)
        # 区域
        # 直辖市
        if grab.workarea in ['010', '020', '030', '040']:
            job.location_city_id = dict_info('LIEP:Area', grab.workarea)
            job.location_province_id = getProvinceCode(job.location_city_id)
        # 省
        elif len(grab.workarea) == 3:
            job.location_province_id = dict_info('LIEP:Area', grab.workarea)
        # 市
        elif len(grab.workarea) == 6:
            job.location_city_id = dict_info('LIEP:Area', grab.workarea)
            job.location_province_id = getProvinceCode(job.location_city_id)
        # 区
        elif len(grab.workarea) == 9:
            job.location_area_id = dict_info('LIEP:Area', grab.workarea)
            # 取猎聘市级
            # 直辖市所在区
            citycode = grab.workarea[0: 3]
            # 非直辖市
            if citycode not in ['010', '020', '030', '040']:
                citycode = grab.workarea[0: 6]
            job.location_city_id = dict_info('LIEP:Area', citycode)
            job.location_province_id = getProvinceCode(job.location_city_id)
        # 年薪
        avgsalary = int((grab.salarymin + grab.salarymax) * 10000 / 12 / 2)
        job.salary_id = getSalaryIdBySalaryValue(avgsalary)
        job.lmt_edu_id = dict_info('LIEP:Degree', grab.education, '0')
        job.lmt_work = getWorkExpId(grab.workexperience)
        job.lmt_age_min = grab.agefrom
        job.lmt_age_max = grab.ageto
        job.receiver_emails = grab.emails
        job.work_responsibility = grab.jobdesc
        job.start_date = datetime.date(grab.datepublish)
        job.publish_date = grab.datepublish
        job.last_sync_time = datetime.today()
        job.lmt_sex_ids = dict_info('LIEP:Gender', grab.sex, '391012')
        # 福利标签
        for item in grab.welfares.split(','):
            if not item:
                continue
            tagid = dict_info('LIEP:Welfare', item, 'None')
            if tagid != 'None':
                job.props.append(tagid)
            else:
                job.tags.append(item)

liep = LiePDecorate()



def PackPositionDown(grab, job):
    try:
        if grab.siteid == 1:
            wyjob.pack_position(grab, job)
        elif grab.siteid == 2:
            rd2.pack_position(grab, job)
        elif grab.siteid == 3:
            lag.pack_position(grab, job)
        elif grab.siteid == 4:
            cjol.pack_position(grab, job)
        elif grab.siteid == 5:
            wuba.pack_position(grab, job)
        elif grab.siteid == 6:
            ganj.pack_position(grab, job)
        elif grab.siteid == 7:
            liep.pack_position(grab, job)
        return True
    except BaseException as e:
        logger.error(u'<{}>下载职位<{}>失败,原因:<{}>'.format(SiteConfig.getSiteNameById(grab.siteid), grab.jobid, e))
        return True


if __name__ == '__main__':
    from zpb.business.model.position import GrabPosition
    session = DBInstance.session
    grab = session.query(GrabPosition).filter(GrabPosition.pk_id == 77).first()
    session.close()
    try:
        wyjob.pack_position(grab)
    except BaseException as e:
        from traceback import format_exc
        print format_exc(e)
