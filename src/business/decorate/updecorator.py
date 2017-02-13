#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: business/decorate/updecorator.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-01 17:54
#########################################################################


# stdlib
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
# zpb
from zpb.cache.rediscache import ForwardDictCli, BackwardDictCli
from zpb.business.model.basejob import BaseJob
from zpb.business.model.resumesearcher import ResumeSearcherCond
from zpb.utils.tools import str2int, date2str, getSalaryMinMaxBySalaryId, getSalaryIdBySalaryValue, ifnull
from zpb.business.siteconfig import SiteConfig
from zpb.exception import *
from zpb.conf import logger


def dict_info(dict_type, key, defaultvalue=''):
    # 返回默认值
    if key is None or key == '':
        return json.loads('{{"id": "{}", "name": "", "parentid":""}}'.format(defaultvalue))
    jsonstring = BackwardDictCli.get('{}:{}'.format(dict_type, key))
    if jsonstring:
        try:
            jsonvalue = json.loads(jsonstring)
            return jsonvalue
        except:
            raise BaseException(u'从缓存字典加载的数据非法,必须提供json格式数据(关键字={}:{}).'.format(dict_type, key))
    else:
        if defaultvalue is not None:
            return json.loads('{{"id": "{}", "name": "", "parentid":""}}'.format(defaultvalue))
        else:
            raise BaseException(u'无法从缓存字典找到关键字={}:{}的数据,请检查缓存字典.'.format(dict_type, key))

def getCityCodeForWuba(ownercitycode):
    """
    依据城市Code获取城市站点Code
    """
    if not ownercitycode:
        return ''
    value = ForwardDictCli.get('@WUBA:City:{}'.format(ownercitycode))
    if value:
        return value
    else:
        return ''

# 依据多项条件获取对应字典数据
def dict_infos(dict_type, keys, symbol):
    return symbol.join(map(lambda key: dict_info(dict_type, key)['id'], keys.split('#')))

# 依据多项条件获取对应字段数据(id|name)
def dict_info_all(dict_type, keys, symbol, innersymbol):
    return symbol.join(map(lambda key: innersymbol.join(dict_info(dict_type, key).values()[::-1]), keys.split('#')))


class RD2Decorate(object):

    __edu_dict = {
            '20': '3', '21': '4', '30': '5',
            '40': '7', '50': '9', '60': '15',
            '10': '1', '70': '15', '0': ''}

    def __init__(self):
        self.message = ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            # 传递第三方职位ID,用於职位修改请求参数
            if job.from_site_code:
                params['thirdjobcode'] = job.from_site_code
            params['EmploymentType'] = 2 if job.job_mode == '1' else 1
            params['JobTitle'] = job.job_name
            if not job.job_type_level_2 or not job.job_type_level_3:
                self.message = u'职位类别必须填写'
                return
            jobType = dict_info('RD2:JobType', job.job_type_level_3)
            params['JobTypeMain'] = jobType['parentid']
            params['SubJobTypeMain'] = jobType['id']
            params['Quantity'] = job.recruit_num if job.recruit_num else 1
            params['EducationLevel'] = dict_info('RD2:Degree', job.lmt_edu_id, '-1')['id']
            params['WorkYears'] = dict_info('RD2:WorkYear', job.lmt_work, '-1')['id']
            params['MonthlyPay'] = dict_info('RD2:MonthPay', job.salary_id)['id']
            if job.work_responsibility:
                params['JobDescription'] = u'<p>工作职责：</p><p> {}'.format(job.work_responsibility.replace('\n', '</p><p>'))
            if job.work_qualification:
                params['JobDescription'] += u'<p>任职要求：</p><p>{}</p>'.format(job.work_qualification.replace('\n', '</p><p>'))
            # 福利 IDs
            welfares = []
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('RD2:Welfare', tag.tag_id, 'None')
                if welfare['id'] and welfare['id'] != 'None':
                    welfares.append(welfare['id'])
            params['welfaretab'] = ','.join(welfares)
            # 必须填写职位发布地点
            if not job.location_city_id and not job.location_area_id:
                self.message = u'发布城市必须填写'
                return
            # 可选择到区级
            city = dict_info('RD2:Area', job.location_city_id)['id']
            area = dict_info('RD2:Area', job.location_area_id)['id']
            params['PositionPubPlace'] = '{}{}{}'.format(city, '@' if area else '', area)
            if job.location_detail:
                params['WorkAddress'] = job.location_detail
            else:
                params['WorkAddress'] = ''
            if job.end_date:
                params['DateEnd'] = datetime.strftime(job.end_date, '%Y-%m-%d')
            params['ApplicationMethod'] = 1
            if job.receiver_emails:
                params['ApplicationMethod'] = 2
                params['EmailList'] = job.receiver_emails
            params['btnAddClick'] = u'saveandpub'
            params['ESUrl'] = ''
            # 返回POST参数字典
            return params

    def pack_position_modify(self, jobid):
        params = self.pack_position_add(jobid)
        if params:
            # 岗位职责赋予新数据,原参数需要删除以保持页面原数据
            params['editorValue'] = params['JobDescription']
            params['btnAddClick'] = 'saveasnotpub'
            # 职位发布地点不可修改
            del params['PositionPubPlace']
            return params

    def pack_resume_search(self, conditions):
        params = {}
        params['SF_1_1_7'] = '4,9'
        for cond in conditions:
            if not cond.condition_val: continue
            if cond.condition_type == 'keywords':
                params['SF_1_1_1'] = cond.condition_val
            elif cond.condition_type == 'education':
                min_edu = cond.condition_val.split('#')[0]
                max_edu = cond.condition_val.split('#')[1]
                params['SF_1_1_5'] = ','.join([
                    self.__edu_dict[min_edu],
                    self.__edu_dict[max_edu]
                ])
            elif cond.condition_type == 'update':
                params['SF_1_1_7'] = '{},9'.format(cond.condition_val)
            elif cond.condition_type == 'age':
                params['SF_1_1_8'] = cond.condition_val.replace('#', ',')
            elif cond.condition_type == 'area':
                # 居住地点 SF_1_1_18为期望工作地点
                params['SF_1_1_6'] = dict_info('RD2:Area', cond.condition_val)['id']
            elif cond.condition_type == 'industry':
                params['SF_1_1_3'] = dict_infos('RD2:Industry', cond.condition_val, ';')
            elif cond.condition_type == 'sex':
                params['SF_1_1_9'] = dict_info('RD2:Gender', cond.condition_val)['id']
            elif cond.condition_type == 'salary':
                minmax = cond.condition_val.split('#')
                if len(minmax) == 2:
                    minsalary = str2int(minmax[0])
                    maxsalary = str2int(minmax[1]) if minmax[1] != '' else 25000
                    avg = int(round(minsalary + maxsalary) / 2)
                    pay = getSalaryIdBySalaryValue(avg)
                    params['SF_1_1_20'] = dict_info('RD2:MonthPay', pay)['id']
        # 去重
        params['exclude'] = '1'
        # 每页数量
        params['pageSize'] = '60'
        # 排序
        params['orderBy'] = 'DATE_MODIFIED,1'
        return params

rd2 = RD2Decorate()


class WYJobDecorate(object):

    __min_salary_dict = {
        0: '01', 1500: '02', 2000: '03', 3000: '04',
        4500: '05', 6000: '06', 8000: '07', 10000: '08',
        15000: '09', 20000: '13', 25000: '10', 30000: '14',
        40000: '11', 50000: '12', 70000: '15', 100000: '16'
    }

    __max_salary_dict = {
        0: '99', 1499: '01', 1999: '02', 2999: '03',
        4499: '04', 5999: '05', 7999: '06', 9999: '07',
        14999: '08', 19999: '09', 24999: '13', 29999: '10',
        39999: '14', 49999: '11', 69999: '12', 99999: '15'
    }

    # 取距离参数salary最近的值
    def __GetNearbyMinSalary(self, salary):
        key = min(map(lambda kk: abs(kk-salary), self.__min_salary_dict.keys()))
        if key + salary in self.__max_salary_dict.keys():
            return self.__min_salary_dict[salary + key]
        else:
            return self.__min_salary_dict[salary - key]

    def __GetNearbyMaxSalary(self, salary):
        key = min(map(lambda kk: abs(kk-salary), self.__max_salary_dict.keys()))
        if key + salary in self.__max_salary_dict.keys():
            return self.__max_salary_dict[salary + key]
        else:
            return self.__max_salary_dict[salary - key]

    def __init__(self):
        self.message = ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            # 传递第三方职位ID,用於职位修改请求参数
            if job.from_site_code:
                params['thirdjobcode'] = job.from_site_code
            params['CJOBNAME'] = job.job_name
            params['POSCODE'] = job.job_id
            params['JOBNUM'] = job.recruit_num
            if job.location_area_id:
                area = dict_info('WY:Area', job.location_area_id)
            elif job.location_city_id:
                area = dict_info('WY:Area', job.location_city_id)
            elif job.location_province_id:
                area = dict_info('WY:Area', job.location_province_id)
            else:
                self.message = u'职位工作地点必须填写!'
                return
            params['JobAreaSelectValue'] = area['id']
            params['txtSelectedJobAreas'] = area['name']
            params['DEGREEFROM'] = dict_info('WY:Degree', job.lmt_edu_id)['id']
            params['txtWorkAddress'] = job.location_detail
            if job.lmt_age_min:
                params['AGEFROM'] = job.lmt_age_min
            if job.lmt_age_max:
                params['AGETO'] = job.lmt_age_max
            if not job.job_type_level_3:
                self.message = u'职位类别必须填写'
                return
            func1 = dict_info('WY:JobType', job.job_type_level_3)
            params['FuncType1Value'] = func1['id']
            params['FuncType1Text'] = func1['name']
            params['FuncType2Value'] = ''
            params['FuncType2Text'] = ''
            if job.lmt_lang_ids:
                langs = job.lmt_lang_ids.split(',')
                if len(langs) > 0:
                    # 目前本系统仅支持英语
                    params['FL1'] = '01'
                    params['FLevel1'] = dict_info('WY:LangSkill', langs[0])['id']
                if len(langs) > 1:
                    params['FL2'] = '09'
                    params['FLevel2'] = dict_info('WY:LangSkill', langs[1])['id']
            params['WORKYEAR'] = dict_info('WY:WorkYear', job.lmt_work)['id']
            if job.lmt_major_ids:
                majors = job.lmt_major_ids.split(',')
                if len(majors) > 0:
                    major = dict_info('WY:Major', majors[0])
                    params['Major1Value'] = major['id']
                    params['Major1Text'] = major['name']
                if len(majors) > 1:
                    major = dict_info('WY:Major', majors[1])
                    params['Major2Value'] = major['id']
                    params['Major2Text'] = major['name']
            params['Term'] = 0 if job.job_mode == '1' else 1
            # 仅有月薪
            params['DdrSalaryType'] = 1
            params['ProvideSalary'] = dict_info('WY:MonthPay', job.salary_id)['id']
            # 福利 POST参数为中文,以空格join
            welfares = []
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('WY:Welfare', tag.tag_id, 'None')
                if welfare['name']:
                    welfares.append(welfare['name'])
                else:
                    welfares.append(tag.tag_name)
            params['hidWelfare'] = ' '.join(welfares)
            params['TxtJobKeywords'] = job.search_keywords
            if job.work_responsibility:
                params['CJOBINFO'] = u'工作职责： \r\n{}\r\n'.format(job.work_responsibility)
            if job.work_qualification:
                params['CJOBINFO'] += u'任职要求： \r\n{}'.format(job.work_qualification)
            # 邮件转发
            if job.receiver_emails:
                params['cbxEmail'] = 'on'
                params['radEmail1'] = 2
                params['JOBEMAIL'] = job.receiver_emails
            return params

    def pack_position_modify(self, jobid):
        params = self.pack_position_add(jobid)
        if params:
            # 发布城市/上班地址不可修改
            del params['JobAreaSelectValue']
            del params['txtSelectedJobAreas']
            del params['txtWorkAddress']
            return params

    def pack_resume_search(self, conditions):
        params = {}
        hidparams = {}
        hidparams['KEYWORDTYPE'] = '0'
        hidparams['KEYWORD'] = ''
        hidparams['LASTMODIFYSEL'] = '5'
        hidparams['AGE'] = ''
        hidparams['SEX'] = '99'
        hidparams['TOPDEGREE'] = ''
        hidparams['EXPECTSALARY'] = '01|99'
        hidparams['WORKYEAR'] = '0|99'
        hidparams['LASTMODIFYSEL'] = '5'
        hidparams['WORKINDUSTRY1'] = ''
        for cond in conditions:
            if not cond.condition_val: continue
            if cond.condition_type == 'keywords':
                hidparams['KEYWORD'] = cond.condition_val.decode('utf-8')
            elif cond.condition_type == 'education':
                hidparams['TOPDEGREE'] = dict_infos('WY:Degree', cond.condition_val, '|')
            elif cond.condition_type == 'update':
                if cond.condition_val <= 2:
                    hidparams['LASTMODIFYSEL'] = '1'
                elif cond.condition_val == 3:
                    hidparams['LASTMODIFYSEL'] = '2'
                elif cond.condition_val == 4:
                    hidparams['LASTMODIFYSEL'] = '3'
                elif cond.condition_val == 5:
                    hidparams['LASTMODIFYSEL'] = '4'
                elif cond.condition_val <= 7:
                    hidparams['LASTMODIFYSEL'] = '5'
                else:
                    hidparams['LASTMODIFYSEL'] = '2016'
            elif cond.condition_type == 'age':
                hidparams['AGE'] = cond.condition_val.replace('#', '|')
            elif cond.condition_type == 'area':
                # 包含期望工作地点
                params['chkExpectJobArea'] = 'on'
                hidparams['AREA'] = dict_info('WY:Area', cond.condition_val)['id']
            elif cond.condition_type == 'industry':
                # WORKINDUSTRY1#计算机软件|01$互联网/电子商务|32
                hidparams['WORKINDUSTRY1'] = dict_info_all('WY:Industry', cond.condition_val, '$', '|')
            elif cond.condition_type == 'sex':
                hidparams['SEX'] = dict_info('WY:Gender', cond.condition_val)['id']
            elif cond.condition_type == 'salary':
                min_salary = str2int(cond.condition_val.split('#')[0])
                max_salary = str2int(cond.condition_val.split('#')[1])
                hidparams['EXPECTSALARY'] = '|'.join([
                    self.__GetNearbyMinSalary(min_salary),
                    self.__GetNearbyMaxSalary(max_salary)
                ])
        hidValue = u'*'.join(u'#'.join([key, value]) for key, value in hidparams.items())
        params['hidValue'] = hidValue
        return params

wyjob = WYJobDecorate()


class CJOLDecorate(object):

    def __init__(self):
        self.message = ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            # 必填参数
            params['StatusID'] = 5
            params['JobName'] = job.job_name
            cityname = ''
            areaname = ''
            if job.location_area_id:
                location = dict_info('CJ:Area', job.location_area_id)
                locationid = location['id']
                areaname = location['name']
                if not locationid:
                    location = dict_info('CJ:Area', job.location_city_id)
                    locationid = location['id']
                    cityname = location['name']
                else:
                    cityname = dict_info('CJ:Area', job.location_city_id)['name']
                if not locationid:
                    location = dict_info('CJ:Area', job.location_province_id, None)
                    locationid = location['id']
                    cityname = location['name']
            elif job.location_city_id:
                location = dict_info('CJ:Area', job.location_city_id)
                locationid = location['id']
                cityname = location['name']
                if not locationid:
                    location = dict_info('CJ:Area', job.location_province_id, None)
                    locationid = location['id']
                    cityname = location['name']
            elif job.location_city_id:
                location = dict_info('CJ:Area', job.location_province_id, None)
                locationid = location['id']
                cityname = location['name']
            else:
                self.message = u'职位工作地点必须填写!'
                return
            params['JobLocation_CODE'] = locationid
            params['JobCategory_FullTime'] = 0
            params['JobCategory_Parttime'] = 0
            params['JobCategory_Graduate'] = 0
            params['JobCategory_Trainee'] = 0
            if job.job_mode == '1':
                # 全职(填写或注释)
                params['JobCategory_FullTime'] = 1
            else:
                # 兼职(同上)
                params['JobCategory_Parttime'] = 1
            # 工作岗位(必填)
            params['JobFunctions'] = dict_info('CJ:JobType', job.job_type_level_3)['id']
            if not params['JobFunctions']:
                params['JobFunctions'] = dict_info('CJ:JobType', job.job_type_level_2)['id']
                if not params['JobFunctions']:
                    self.message = u'没有找到职位类别<{}>映射!'.format(job.job_type_level_3)
                    return
            # 薪资等级(填写或注释)
            (params['MinProvidedSalary'], params['MaxProvidedSalary']) = getSalaryMinMaxBySalaryId(job.salary_id)
            # 年龄(填写或注释)
            params['MinAge'] = job.lmt_age_min if job.lmt_age_min else ''
            params['MaxAge'] = job.lmt_age_max if job.lmt_age_max else ''
            # 性别(必填)
            params['GenderRequirement_CODE'] = dict_info('CJ:Gender', job.lmt_sex_ids, '2')['id']  # (2=不限,0=女,1=男)
            # 工作经验(填写1-10或注释)
            params['MinYearsOfExperience'] = 0#dict_info('CJ:WorkYear', job.lmt_work)['id']
            # 外语(填写或注释)
            params['ForeignLanguage_CODE'] = '0'
            if job.lmt_lang_ids:
                langs = job.lmt_lang_ids.split(',')
                if langs != '391026':
                    params['ForeignLanguage_CODE'] = '5'
            # 学历(必填,10,20..80)
            params['EducationRequirement_CODE'] = dict_info('CJ:Degree', job.lmt_edu_id, '10')['id']
            # 专业(填写或注释)
            params['Specialty_CODE'] = ''
            if job.lmt_major_ids:
                majorsvalue = []
                majors = job.lmt_major_ids.split(',')
                if len(majors) > 0:
                    majorsvalue.append(dict_info('CJ:Major', majors[0])['id'])
                if len(majors) > 1:
                    majorsvalue.append(dict_info('CJ:Major', majors[1])['id'])
                if len(majors) > 2:
                    majorsvalue.append(dict_info('CJ:Major', majors[2])['id'])
                params['Specialty_CODE'] = ','.join(majorsvalue)
            # 具体要求(纯文本,可有换行符)
            if job.work_responsibility:
                params['Requirement'] = u'工作职责： <p></p>{}<p></p>'.format(job.work_responsibility)
            if job.work_qualification:
                params['Requirement'] += u'任职要求： <p></p>{}'.format(job.work_qualification)
            # 关键字(填写或注释)
            params['KeyWord'] = job.search_keywords if job.search_keywords else ''
            # 邮箱(填写或注释)
            params['ApplyThroughEmail'] = job.receiver_emails
            params['JobWorkLocation'] = cityname + areaname + (job.location_detail if job.location_detail else '')
            # 福利 POST参数为ids
            welfares = []
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('CJ:Welfare', tag.tag_id, '')
                if welfare['id']:
                    welfares.append(welfare['id'])
            params['JobLabel_CODE'] = ','.join(welfares)
            params['ApplyThroughURL'] = ''
            params['IsFilter'] = 0
            params['IsOpenEntrust'] = 1
            params['DepartmentId'] = 1
            params['JobPostingPeriod'] = 31
            params['PostTime'] = date2str(datetime.today())
            params['DisableTime'] = date2str(datetime.today() + relativedelta(months=1))
            return params

    def pack_position_modify(self, jobid):
        return self.pack_position_add(jobid)

    def pack_resume_search(self, conditions):
        params = {}
        params['fn'] = 'd'
        params['UpdateTime'] = '31'
        for cond in conditions:
            if not cond.condition_val: continue
            if cond.condition_type == 'keywords':
                params['Keyword'] = cond.condition_val
            elif cond.condition_type == 'education':
                cs = cond.condition_val.split('#')
                if cs[0]:
                    params['MinEducation'] = dict_info('CJ:Degree', cs[0])['id']
                if cs[1]:
                    params['MaxEducation'] = dict_info('CJ:Degree', cs[1])['id']
            elif cond.condition_type == 'update':
                if cond.condition_val == '1':
                    params['UpdateTime'] = '3'
                elif cond.condition_val == '2':
                    params['UpdateTime'] = '7'
                elif cond.condition_val == '3':
                    params['UpdateTime'] = '14'
                elif cond.condition_val == '4':
                    params['UpdateTime'] = '31'
                elif cond.condition_val == '5':
                    params['UpdateTime'] = '62'
                elif cond.condition_val == '6':
                    params['UpdateTime'] = '93'
                elif cond.condition_val == '7':
                    params['UpdateTime'] = '180'
                elif cond.condition_val == '8':
                    params['UpdateTime'] = '360'
                else:
                    params['UpdateTime'] = '31'
            elif cond.condition_type == 'age':
                cs = cond.condition_val.split('#')
                if cs[0]:
                    params['MinAge'] = cs[0]
                if cs[1]:
                    params['MaxAge'] = cs[1]
            elif cond.condition_type == 'area':
                params['CurrentLocation'] = dict_info('CJ:Area', cond.condition_val)['id']
            elif cond.condition_type == 'industry':
                cs = cond.condition_val.split('#')
                if len(cs) > 0:
                    params['ExpectedIndustry'] = dict_info('CJ:Industry', cs[0])['id']
            elif cond.condition_type == 'sex':
                params['Gender'] = dict_info('CJ:Gender', cond.condition_val)['id']
            elif cond.condition_type == 'salary':
                cs = cond.condition_val.split('#')
                if cs[0]:
                    params['MinExpectedSalary'] = cs[0]
                if cs[1]:
                    params['MaxExpectedSalary'] = cs[1]
        return params

cjol = CJOLDecorate()


class LAGDecorate(object):

    def __init__(self):
        self.message = ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            if job.job_type_level_3:
                jobtype = dict_info('LAG:JobType', job.job_type_level_3)
                if not jobtype['name']:
                    self.message = u'无法找到职位类别的映射!'
                    return
                # 第1/2级关系
                if jobtype['parentid'] in [u'技术', u'产品', u'设计', u'运营', u'市场与销售', u'职能', u'金融']:
                    params['positionType'] = jobtype['name']
                else:
                    params['positionThreeType'] = jobtype['name']
                    params['positionType'] = jobtype['parentid']
                params['positionTypeId'] = jobtype['id']
            elif job.job_type_level_2:
                jobtype = dict_info('LAG:JobType', job.job_type_level_2)
                if not jobtype['name']:
                    self.message = u'无法找到职位类别的映射!'
                    return
                params['positionType'] = jobtype['name']
            else:
                self.message = u'职位类别必须填写!'
                return
            params['positionName'] = job.job_name
            params['department'] = ''
            params['jobNature'] = u'全职' if job.job_mode == '1' else u'兼职'
            params['salaryMin'], params['salaryMax'] = getSalaryMinMaxBySalaryId(job.salary_id)
            params['salaryMin'] /= 1000
            params['salaryMax'] /= 1000
            params['workAddress'] = dict_info('Common:City', job.location_city_id)['cityname']
            params['workYear'] = dict_info('LAG:WorkYear', job.lmt_work)['name']
            params['education'] = dict_info('LAG:Degree', job.lmt_edu_id)['name']
            advs = []
            advlen = 0
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('WY:Welfare', tag.tag_id, 'None')
                if welfare['name']:
                    advlen += len(welfare['name'])
                    if advlen < 20:
                        advs.append(welfare['name'])
                    else:
                        break
                else:
                    advlen += len(tag.tag_name)
                    if advlen < 20:
                        advs.append(tag.tag_name)
                    else:
                        break
            if len(advs) == 0 and job.search_keywords:
                for tag in job.search_keywords.split(','):
                    advlen += len(tag)
                    if advlen < 20:
                        advs.append(tag)
                    else:
                        break
            params['positionAdvantage'] = ' '.join(advs) if len(advs) > 0 else u'发展前景好'
            if job.work_responsibility:
                params['positionDetail'] = u'工作职责： \r\n{}\r\n'.format(job.work_responsibility)
            if job.work_qualification:
                params['positionDetail'] += u'任职要求： \r\n{}'.format(job.work_qualification)
            params['positionAddress'] = job.location_detail
            params['positionLng'] = ''
            params['positionLat'] = ''
            params['forwardEmail'] = job.receiver_emails
            # 必填参数
            return params

    def pack_position_modify(self, jobid):
        params = self.pack_position_add(jobid)
        if params:
            return params

    def pack_resume_search(self, conditions):
        return {}


lag = LAGDecorate()


class WubaDecorate(object):

    def __init__(self):
        self.message = ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            params['Title'] = job.job_name.decode('utf-8')[0: 12]
            jobtype = dict_info('WUBA:JobType', job.job_type_level_3)
            if not jobtype:
                self.message = u'无法找到职位类别的映射!'
                return
            params['xiaozhiwei'] = jobtype['id']
            params['jobcateID'] = jobtype['parentid']
            # 58没有的职位,默认为其他
            if not params['xiaozhiwei']:
                params['xiaozhiwei'] = '12265'
            if params['xiaozhiwei'] == '12265':
                params['jobcateID'] = '13961'
            params['zhaopinrenshu'] = job.recruit_num if job.recruit_num > 0 else 999
            params['xueliyaoqiu'] = dict_info('WUBA:Degree', job.lmt_edu_id)['id']
            params['gongzuonianxian'] = dict_info('WUBA:WorkYear', job.lmt_work)['id']
            # 可接受应届生
            params['yingjiesheng'] = ''
            if job.work_responsibility:
                params['Content'] = u'工作职责： \r\n{}'.format(job.work_responsibility)
            if job.work_qualification:
                params['Content'] += u'\r\n任职要求： \r\n{}'.format(job.work_qualification)
            citycode = getCityCodeForWuba(job.location_city_id)
            if citycode:
                params['postcity'] = citycode
            else:
                params['postcity'] = '2258'
            params['localcity'] = dict_info('WUBA:Area', job.location_city_id)['id']
            params['localarea'] = dict_info('WUBA:Area', job.location_area_id)['id']
            params['localdiduan'] = ''
            params['gongzuodizhi'] = job.location_detail
            params['minxinzi'] = dict_info('WUBA:MonthPay', job.salary_id, u'面议')['id']
            # 福利 POST参数为中文,以空格join
            welfares0 = []
            welfares1 = []
            welother = 0
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('WUBA:Welfare', tag.tag_id, 'None')
                if welfare['name']:
                    welfares0.append(welfare['id'])
                    welfares1.append(welfare['name'])
                else:
                    if welother < 3:
                        welfares1.append(tag.tag_name)
                        welother += 1
            params['fulibaozhang'] = u'|'.join(welfares0)
            params['zhiweiliangdian'] = u'|'.join(welfares1)
            params['Email'] = job.receiver_emails
            if params['Email'].find(';') > -1:
                params['Email'] = params['Email'].split(';')[0]
            if params['Email'].find(',') > -1:
                params['Email'] = params['Email'].split(',')[0]
            if params['Email'].find('@') == -1:
                params['Email'] = ''
            params['jz_refresh_post_key'] = 0
            params['disablecheck'] = ''
            params['captcha_type'] = ''
            params['captcha_input'] = ''
            params['showcontact'] = ''
            params['onlinetalkway'] = 0
            params['GTID'] = ''
            return params

    def pack_position_modify(self, jobid):
        params = self.pack_position_add(jobid)
        if params:
            return params

    def pack_resume_search(self, conditions):
        return {}

wuba = WubaDecorate()


class GanJDecorate(object):

    def __init__(self):
        self.message = ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            params['client_company_id'] = ''
            # the category of the job(2: fulltime job)
            params['category_id'] = '2'
            # the major of the job
            major_job = dict_info('GANJ:JobType', job.job_type_level_3)
            params['category'] = major_job['name']
            params['major_category_id'] = major_job['parentid']
            params['tag_id'] = major_job['id']
            if not params['tag_id']:
                self.message = u'暂不支持的职位类别'
                return
            params['category_url'] = major_job['pyname']
            params['title'] = job.job_name
            params['need_num'] = job.recruit_num if job.recruit_num > 0 else 999
            params['price'] = dict_info('GANJ:MonthPay', job.salary_id)['id']
            params['degree'] = dict_info('GANJ:Degree', job.lmt_edu_id)['id']
            params['work_years'] = dict_info('GANJ:WorkYear', job.lmt_work)['id']
            params['sex'] = 0
            params['age_min'] = ifnull(job.lmt_age_min, 0)
            params['age_max'] = ifnull(job.lmt_age_max, 0)
            params['probation_price'] = ''
            params['probation_time'] = ''
            params['pub_help_tpl'] = 0
            params['charge_items'] = ''
            params['is_eidt_charge'] = 1
            params['fee_money'] = ''
            city = dict_info('GANJ:Area', job.location_city_id)
            cityid = city['id']
            cityname = city['name']
            district = dict_info('GANJ:Area', job.location_area_id)
            districtid = district['id']
            districtname = district['name']
            streetid = '-1'
            params['cityid'] = cityid
            params['cityname'] = cityname
            params['districtid'] = districtid
            params['districtname'] = districtname
            params['streetid'] = streetid
            params['location'] = ifnull(job.location_detail, '')
            if job.receiver_emails:
                params['email'] = job.receiver_emails
                if params['email'].find(';') > -1:
                    params['email'] = params['email'].split(';')[0]
                if params['email'].find(',') > -1:
                    params['email'] = params['email'].split(',')[0]
                if params['email'].find('@') == -1:
                    params['email'] = ''
            params['phone2'] = ''
            params['phonecode'] = ''
            params['is_auto_save'] = 1
            params['confirm_taobao_auth'] = 1
            params['from'] = 'wb'
            params['login'] = ''
            params['returl'] = ''
            params['city_id'] = cityid
            params['district_id'] = districtid
            params['street_id'] = streetid
            # the job welfare
            welfares = list()
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('GANJ:Welfare', tag.tag_id, '')
                if welfare and welfare['id']:
                    welfares.append(welfare['id'])
            params['tag_info'] = ','.join(welfares)
            params['work_place'] = 0
            if job.work_responsibility:
                params['description'] = u'工作职责： \r\n{}\r\n'.format(job.work_responsibility.replace('\n\n', '\r\n'))
            if job.work_qualification:
                params['description'] += u'任职要求： \r\n{}'.format(job.work_qualification.replace('\n\n', '\r\n'))
            params['fee_status'] = 0
            return params

    def pack_position_modify(self, jobid):
        params = self.pack_position_add(jobid)
        if params:
            params['tag'] = params.pop('tag_id')
            params['pub_help_tpl'] = 0
            return params

    def pack_resume_search(self, conditions):
        return {}

ganj = GanJDecorate()


class LiePDecorate(object):

    def __init__(self):
        self.message = ''

    def _getMinMaxSalary(self, salaryid):
        if salaryid == 391000:
            return (1, 2)
        elif salaryid == 391001:
            return (2, 4)
        elif salaryid == 391002:
            return (4, 6)
        elif salaryid == 391003:
            return (6, 8)
        elif salaryid == 391004:
            return (8, 10)
        elif salaryid == 391005:
            return (10, 15)
        elif salaryid == 391006:
            return (15, 25)
        elif salaryid == 391007:
            return (25, 30)
        elif salaryid == 391008:
            return (30, 50)
        elif salaryid == 391009:
            return (0, 0)

    def _getMinWorkYear(self, lmtwork):
        if lmtwork == 371000:
            return '0'
        elif lmtwork == 371001:
            return '1'
        elif lmtwork == 371002:
            return '3'
        elif lmtwork == 371003:
            return '6'
        elif lmtwork == 371004:
            return '8'
        elif lmtwork == 371005:
            return '10'
        elif lmtwork == 371006:
            return ''
        else:
            return ''

    def pack_position_add(self, jobid):
        job = BaseJob.queryOne(jobid)
        if job:
            params = {}
            params['actionType'] = 'publish'
            params['ejob_title'] = job.job_name
            params['ejob_dq'] = dict_info('LIEP:Area', job.location_area_id)['id']
            if not params['ejob_dq']:
                params['ejob_dq'] = dict_info('LIEP:Area', job.location_city_id)['id']
            if not params['ejob_dq']:
                params['ejob_dq'] = dict_info('LIEP:Area', job.location_province_id)['id']
            if not params['ejob_dq']:
                self.message = u'职位工作地点匹配错误'
                return
            params['ejob_jobtitle'] = dict_info('LIEP:JobType', job.job_type_level_3)['id']
            if not params['ejob_jobtitle']:
                self.message = u'职位分类匹配错误'
                return
            params['detail_dept_id'] = ''
            params['ejob_monthlysalary_low'], params['ejob_monthlysalary_high'] = self._getMinMaxSalary(job.salary_id)
            params['detail_agelow'] = job.lmt_age_min if job.lmt_age_min else ''
            params['detail_agehigh'] = job.lmt_age_max if job.lmt_age_max else ''
            params['detail_sex_par'] = dict_info('LIEP:Gender', job.lmt_sex_ids)['id']
            params['detail_special'] = u'不限'
            params['detail_industrys'] = ''
            params['detail_workyears'] = self._getMinWorkYear(job.lmt_work)
            params['detail_edulevel'] = dict_info('LIEP:Degree', job.lmt_edu_id)['id']
            if job.lmt_lang_ids in ['391022', '391023', '391024', '391025']:
                params['detail_language_english'] = 1
            if job.work_responsibility:
                params['detail_duty_qualify'] = u'工作职责： \r\n{}\r\n'.format(job.work_responsibility.replace('\n\n', '\r\n'))
            if job.work_qualification:
                params['detail_duty_qualify'] += u'任职要求： \r\n{}'.format(job.work_qualification.replace('\n\n', '\r\n'))
            # the job welfare
            welfares = list()
            for tag in BaseJob.queryTagsByJobId(jobid):
                welfare = dict_info('LIEP:Welfare', tag.tag_id, '')
                if welfare and welfare['name']:
                    welfares.append(welfare['name'])
                else:
                    welfares.append(tag.tag_name)
            # 仍需与默认值进行比对
            welfares = map(lambda welfare: '"' + welfare + '"', welfares)
            params['detail_tag_json'] = '['+ ','.join(welfares) + ']'
            #
            params['ejob_dq_mails'] = params['ejob_dq'] + ',' + job.receiver_emails.replace(';', ',')
            # 应聘反馈时长
            params['feedback_period'] = 3
            return params

    def pack_position_modify(self, jobid):
        params = self.pack_position_add(jobid)
        if params:
            params['actionType'] = 'update'
            # 职位名称与职位工作地点无法修改
            del params['ejob_title']
            # del params['ejob_dq']
            # del params['detail_dept_id']
            del params['detail_industrys']
            return params

    def pack_resume_search(self, conditions):
        return {}

liep = LiePDecorate()


def _GetDecoratorService(siteid):
    if siteid == 1:
        return wyjob
    elif siteid == 2:
        return rd2
    elif siteid == 3:
        return lag
    elif siteid == 4:
        return cjol
    elif siteid == 5:
        return wuba
    elif siteid == 6:
        return ganj
    elif siteid == 7:
        return liep
    else:
        raise BaseException(u'还未支持的招聘平台<{}>'.format(siteid))


def PackPositionNew(siteid, jobid):
    DecoratorService = _GetDecoratorService(siteid)
    params = DecoratorService.pack_position_add(jobid)
    if not params:
        raise InvalidJobParamError(DecoratorService.message)
    return params


def PackPositionModify(siteid, jobid):
    DecoratorService = _GetDecoratorService(siteid)
    params = DecoratorService.pack_position_modify(jobid)
    if not params:
        raise InvalidJobParamError(DecoratorService.message)
    return params

def PackResumeSearch(siteid, searcherid):
    conditions = ResumeSearcherCond.queryBySearcherId(searcherid)
    if conditions:
        DecoratorService = _GetDecoratorService(siteid)
        params = DecoratorService.pack_resume_search(conditions)
        return params


if __name__ == '__main__':
    try:
        params = rd2.pack_position_add(37)
        if params:
            print params
        else:
            print wyjob.message
    except Exception as e:
        print str(e)
