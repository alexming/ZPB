# -*- encoding=utf-8 -*-


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text, update
from sqlalchemy.dialects.mysql import TIMESTAMP, DATE, BLOB
from sqlalchemy.sql import func, and_
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.model.resumememo import ResumeMemo
from zpb.business.model.jobsyncdistribute import JobSyncDistribute
from zpb.business.siteconfig import SiteConfig
from zpb.utils.tools import uuid, fmtHtml, str2int, str2date, getEduHighId, getBirthDay, dateAdd
from zpb.exception import *
from zpb.conf import logger
# dtc
import dtc


class ResumeBase(BaseModel):
    # 表名
    __tablename__ = 'res_resume_base'
    # 表结构
    resume_code = Column(String(32), primary_key=True)
    company_id= Column(Integer)
    from_site_id= Column(Integer)
    from_site_code = Column(String(50))
    from_site_jobcode = Column(String(50))
    apply_job_id = Column(Integer)
    apply_job_status = Column(Integer, default=0)
    apply_time = Column(String(10))
    apply_type = Column(String(10))
    resume_type = Column(Integer)
    resume_grade = Column(String(20))
    married_status = Column(String(20))
    id_no = Column(String(20))
    get_encouragement = Column(Text)
    join_team = Column(Text)
    volunteer_info= Column(String(100))
    graduate_year = Column(Integer)
    graduate_month= Column(Integer)
    begin_work_year= Column(Integer)
    begin_work_month = Column(Integer)
    last_update = Column(String(20))
    third_score = Column(String(20))
    certificate_name = Column(String(1000))
    person_memo = Column(Text)
    lesson_name = Column(String(500))
    computer_level = Column(String(500))
    english_level = Column(String(500))
    graduate_school = Column(String(100))
    school_rankings = Column(String(100))
    addr_postcode = Column(String(10))
    speciality_name= Column(String(100))
    contact_addr = Column(String(200))
    native_place= Column(String(100))
    national_name = Column(String(100))
    nationality_name = Column(String(100))
    birth_day = Column(String(20))
    person_name = Column(String(50))
    family_name = Column(String(50))
    person_href = Column(String(200))
    hope_title= Column(String(100))
    hope_title2 = Column(String(100))
    title_standard = Column(String(100))
    aim_institution = Column(String(100))
    person_sex = Column(String(10))
    person_age= Column(Integer)
    body_high = Column(String(20))
    body_weight = Column(String(20))
    mobile_no = Column(String(20))
    other_phone = Column(String(20))
    fax_no = Column(String(20))
    email_addr= Column(String(50))
    now_location= Column(String(100))
    hope_location = Column(String(100))
    high_education = Column(String(100))
    high_edu_id = Column(Integer)
    advance_degree= Column(String(100))
    exp_name = Column(String(100))
    now_vocation= Column(String(100))
    hope_vocation = Column(String(200))
    vocation_standard= Column(String(100))
    now_salary= Column(String(100))
    hope_salary = Column(String(100))
    periods_of_time = Column(String(100))
    political_name= Column(String(50))
    start_from = Column(String(100))
    apply_switch = Column(String(50))
    qq_no = Column(String(20))
    student_type = Column(String(50))
    photo_url = Column(String(100))
    apply_letter = Column(Text)
    last_company = Column(String(100))
    last_title = Column(String(100))
    overseas_work = Column(String(10))
    job_hope_frequency= Column(Integer)
    integrity_ratio= Column(Integer)
    work_type = Column(String(20))
    birth_date = Column(DATE)
    work_years= Column(Integer)
    local_provice_id = Column(Integer)
    local_city_id = Column(Integer)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))
    is_new = Column(String(1))
    source = Column(Integer)
    matching_degree = Column(Integer)

    @staticmethod
    def isNew(companyid, siteid, websiteresumeid, applytime):
        if companyid and websiteresumeid:
            session = DBInstance.session
            try:
                # 来源于网站
                if siteid:
                    rm = session.query(ResumeBase.apply_time).filter(
                        and_(
                            ResumeBase.company_id == companyid,
                            ResumeBase.from_site_id == siteid,
                            ResumeBase.from_site_code == websiteresumeid
                            )).first()
                # 来源于邮件
                else:
                    rm = session.query(ResumeBase.apply_time).filter(
                        and_(
                            ResumeBase.company_id == companyid,
                            ResumeBase.from_site_code == websiteresumeid
                            )).first()
                if rm:
                    # 最新简历
                    if str2date(rm.apply_time, '%Y-%m-%d') < str2date(applytime):
                        return True
                else:
                    return True
            finally:
                session.close()
        else:
            return True

    @classmethod
    def queryAndCreate(cls, session, companyid, siteid, websiteresumeid, applytime):
        try:
            if companyid and websiteresumeid:
                # 来源于网站
                if siteid:
                    rm = session.query(ResumeBase).filter(
                        and_(
                            ResumeBase.company_id == companyid,
                            ResumeBase.from_site_id == siteid,
                            ResumeBase.from_site_code == websiteresumeid
                            )).first()
                # 来源于邮件
                else:
                    rm = session.query(ResumeBase).filter(
                        and_(
                            ResumeBase.company_id == companyid,
                            ResumeBase.from_site_code == websiteresumeid
                            )).first()
            # 来源于邮件且为手动简历(非系统生成)需要进一步依据简历内容过滤
            else:
                rm = None
            if not rm:
                # 新增简历
                rm = ResumeBase()
                rm.resume_code = uuid()
                rm.company_id = companyid
                rm.from_site_id = siteid
                rm.from_site_code = websiteresumeid
                rm.apply_time = str2date(applytime)
                rm.is_new = 'T'
                rm.create_user = 'zpb'
                rm.create_time = datetime.today()
                # 新增resume_memo
                rmm = ResumeMemo(rm.resume_code)
                rmm.memo_content = u'来源于[{}]'.format(SiteConfig.getSiteNameById(siteid))
                session.add(rmm)
                return (True, rm)
            else:
                # 简历刷新
                if str2date(rm.apply_time, '%Y-%m-%d') < str2date(applytime):
                    rm.apply_time = str2date(applytime)
                    # 删除历史附加信息
                    session.query(ResumeExtend).filter(ResumeExtend.resume_code==rm.resume_code).delete()
                    session.query(ResumeEdu).filter(ResumeEdu.resume_code==rm.resume_code).delete()
                    session.query(ResumeEnglish).filter(ResumeEnglish.resume_code==rm.resume_code).delete()
                    session.query(ResumeExp).filter(ResumeExp.resume_code==rm.resume_code).delete()
                    session.query(ResumeIT).filter(ResumeIT.resume_code==rm.resume_code).delete()
                    session.query(ResumeLang).filter(ResumeLang.resume_code==rm.resume_code).delete()
                    session.query(ResumeProject).filter(ResumeProject.resume_code==rm.resume_code).delete()
                    session.query(ResumeTrain).filter(ResumeTrain.resume_code==rm.resume_code).delete()
                    return (True, rm)
                else:
                    return (False, rm)
        except BaseException as e:
            raise DBOperateError(currentFuncName(cls), e)

    # 查找未查看的简历,并返回结果
    @classmethod
    def queryAndExport(cls, companyid, pagesize=5):
        data = []
        session = DBInstance.session
        try:
            rows = session.query(ResumeBase).filter(and_(ResumeBase.company_id==companyid,ResumeBase.is_new=='T')).limit(pagesize).all()
            for row in rows:
                resume = row.as_dict()
                # 组装
                for table in [ResumeEdu, ResumeEnglish, ResumeExp, ResumeIT, ResumeLang, ResumeProject, ResumeTrain]:
                    resume[table.__tablename__] = []
                    items = session.query(table).filter(table.resume_code==row.resume_code).all()
                    for item in items:
                        resume[table.__tablename__].append(item.as_dict())
                data.append(resume)
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()
            return data

    @classmethod
    def queryAndExportByResumeCode(cls, resumecode):
        data = []
        session = DBInstance.session
        try:
            row = session.query(ResumeBase).filter(ResumeBase.resume_code==resumecode).first()
            if row:
                resume = row.as_dict()
                # 组装
                for table in [ResumeEdu, ResumeEnglish, ResumeExp, ResumeIT, ResumeLang, ResumeProject, ResumeTrain]:
                    resume[table.__tablename__] = []
                    items = session.query(table).filter(table.resume_code==row.resume_code).all()
                    for item in items:
                        resume[table.__tablename__].append(item.as_dict())
                #
                data.append(resume)
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()
            return data

    # 处理结果确认
    @classmethod
    def ackExport(cls, companyid, resumecodes):
        session = DBInstance.session
        try:
            stmt = update(ResumeBase).where(ResumeBase.resume_code.in_(resumecodes)).values(is_new='F')
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    # 变更简历状态(未下载简历->已下载简历)
    @classmethod
    def changeStat(cls, resumecode):
        session = DBInstance.session
        try:
            stmt = update(ResumeBase).where(ResumeBase.resume_code==resumecode).values(source=0)
            session.execute(stmt)
            session.commit()
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(cls), e)
        finally:
            session.close()

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['create_time','create_user',
                'modify_time','modify_user','is_new','read_time','birth_date',
                'source','matching_degree']}


class ResumeExtend(BaseModel):
    # 表名
    __tablename__ = 'res_resume_extend'
    # 表结构
    resume_code = Column(String(32), primary_key=True)
    org_resume= Column(BLOB)
    edu_detail_full= Column(Text)
    exp_detail_full= Column(Text)
    train_detail_full= Column(Text)
    proj_detail_full = Column(Text)
    skill_detail_full= Column(Text)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))


class ResumeEdu(BaseModel):
    # 表名
    __tablename__ = 'res_resume_edu'
    # 表结构
    edu_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    start_date = Column(String(20))
    end_date = Column(String(20))
    school_name = Column(String(200))
    major_name = Column(String(200))
    adv_degree = Column(String(200))
    diplomas_name = Column(String(200))
    depart_name = Column(String(200))
    edu_summary = Column(Text)
    is_studii = Column(String(200))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['edu_id','create_time','create_user','modify_time','modify_user']}


class ResumeEnglish(BaseModel):
    # 表名
    __tablename__ = 'res_resume_english'
    # 表结构
    skill_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    certificate_name = Column(String(100))
    certificate_score = Column(String(50))
    rec_date = Column(String(20))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['skill_id','create_time','create_user','modify_time','modify_user']}


class ResumeExp(BaseModel):
    # 表名
    __tablename__ = 'res_resume_exp'
    # 表结构
    exp_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    start_date = Column(String(20))
    end_date = Column(String(20))
    periods_of_time = Column(String(100))
    company_name = Column(String(200))
    work_location = Column(String(200))
    vocation_name = Column(String(200))
    company_scale = Column(String(100))
    company_type = Column(String(100))
    depart_name = Column(String(100))
    work_title = Column(String(100))
    salary_name = Column(String(100))
    work_summary = Column(Text)
    leader_name = Column(String(100))
    underling_num = Column(Integer)
    leaving_reason = Column(String(100))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['exp_id','create_time','create_user','modify_time','modify_user']}


class ResumeIT(BaseModel):
    # 表名
    __tablename__ = 'res_resume_it'
    # 表结构
    skill_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    skill_name = Column(String(100))
    use_time = Column(String(50))
    competency_level = Column(String(50))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['skill_id','create_time','create_user','modify_time','modify_user']}


class ResumeLang(BaseModel):
    # 表名
    __tablename__ = 'res_resume_lang'
    # 表结构
    lang_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    lang_name = Column(String(100))
    listen_speak = Column(String(100))
    write_read = Column(String(100))
    lang_score = Column(String(20))
    lang_skill = Column(String(20))
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['lang_id','create_time','create_user','modify_time','modify_user']}


class ResumeProject(BaseModel):
    # 表名
    __tablename__ = 'res_resume_project'
    # 表结构
    proj_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    start_date = Column(String(20))
    end_date = Column(String(20))
    project_name = Column(String(200))
    work_title = Column(String(100))
    proj_desc = Column(Text)
    work_responsibility = Column(Text)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['proj_id','create_time','create_user','modify_time','modify_user']}


class ResumeTrain(BaseModel):
    # 表名
    __tablename__ = 'res_resume_train'
    # 表结构
    train_id = Column(Integer, primary_key=True)
    resume_code = Column(String(32))
    start_date = Column(String(20))
    end_date = Column(String(20))
    train_institution = Column(String(200))
    train_location = Column(String(200))
    train_course = Column(String(200))
    train_certificate = Column(String(200))
    train_desc = Column(Text)
    is_valid = Column(String(1))
    create_time = Column(TIMESTAMP, server_default=func.now())
    create_user = Column(String(50))
    modify_time = Column(TIMESTAMP, server_onupdate=func.now())
    modify_user = Column(String(50))

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns \
            if c.name not in ['train_id','create_time','create_user','modify_time','modify_user']}


# 以json数据组装全量简历并存储
def AssembelResumeByJson(js):
    """
    : params json js
    : return (bool, str, bool)->(处理状态,错误原因或简历ID,是否新简历)
    """
    # 噪声过滤
    if js['Sex'] == '' and js['Age'] == '':
        if js['LastUpdate'] == '199001':
            return (False, u'非法的简历!', False)
        if js['siteid'] == 0:
            return (False, u'非法的简历!', False)
    #
    session = DBInstance.session
    try:
        # 强制刷新简历(用於付费简历下载)
        force = js.get('force', False)
        (ret, rm) = ResumeBase.queryAndCreate(session, js['companyid'], js['siteid'], js['websiteresumeid'], js['apply_time'])
        if not ret and not force:
            return (True, rm.resume_code, False)
        # 填充简历
        rm.apply_job_status = 0
        rm.apply_job_id = 0
        # 简历关联的职位ID,用最新的覆盖
        if js['jobid']:
            rm.from_site_jobcode = js['jobid']
        if not rm.from_site_jobcode:
            rm.from_site_jobcode = ''
        # 来源可能制定本系统关联职位ID(imp_local_file)
        if js['apply_job_id']:
            rm.apply_job_id = js['apply_job_id']
            rm.apply_job_status = 1
        else:
            # 依据招聘平台职位ID查找本系统职位ID
            if rm.from_site_jobcode:
                jobid = JobSyncDistribute.queryJobIdByThirdJobCode(rm.company_id, rm.from_site_id, rm.from_site_jobcode)
                if jobid:
                    rm.apply_job_status = 1
                    rm.apply_job_id = jobid
        # 判定简历投递时间
        if rm.apply_job_status == 1:
            if rm.apply_time <= dateAdd(day=-7):
                rm.pre_apply_job_id = rm.apply_job_id
                rm.apply_job_id = 0
                rm.apply_job_status = 0
        #
        rm.resume_type = js['Type']
        rm.resume_grade = js['ResumeGrade']
        rm.married_status = js['Married']
        rm.id_no = js['IDNO']
        rm.get_encouragement = js['Encouragement'].replace('\r\n', '<br />')
        rm.join_team = js['Team'].replace('\r\n', '<br />')
        rm.volunteer_info = js['Volunteer']
        rm.graduate_year = str2int(js['Graduatetime'][0: 4])
        rm.graduate_month = str2int(js['Graduatetime'][5: 7])
        rm.begin_work_year = str2int(js['Beginworktime'][0: 4])
        rm.begin_work_month = str2int(js['Beginworktime'][5: 7])
        rm.last_update = js['LastUpdate']
        rm.third_score = js['Score']
        rm.certificate_name = js['Certificate'].replace('\r\n', '<br />')
        rm.person_memo = js['Personal'].replace('\r\n', '<br />')
        rm.lesson_name = js['Lesson']
        rm.computer_level = js['Computer']
        rm.english_level = js['English']
        rm.graduate_school = js['School']
        rm.school_rankings = js['SchoolRankings']
        rm.addr_postcode = js['PostCode']
        rm.speciality_name = js['Speciality']
        rm.contact_addr = js['Address']
        rm.native_place = js['Jiguan']
        rm.national_name = js['National']
        rm.nationality_name = js['Nationality']
        rm.birth_day = js['Birth']
        # 来源于简历搜索器(简历解析服务器返回结果有噪声)
        if js['source'] == 9:
            if rm.source is None or rm.source == 9:
                rm.person_name = ''
                rm.family_name = ''
        else:
            rm.person_name = js['Name']
            rm.family_name = js['FamilyName']
        #
        rm.person_href = js['Href']
        rm.hope_title = js['Title']
        rm.hope_title2 = js['Title2']
        rm.title_standard = js['TitleStandard']
        rm.aim_institution = js['AimInstitution']
        rm.person_age = str2int(js['Age'])
        rm.person_sex = js['Sex']
        rm.body_high = js['High']
        rm.body_weight = js['Weight']
        if js['Mobile']:
            rm.mobile_no = js['Mobile']
        if rm.mobile_no is None:
            rm.mobile_no = ''
        if js['Phone']:
            rm.other_phone = js['Phone']
        if rm.other_phone is None:
            rm.other_phone = ''
        rm.fax_no = js['Fax']
        if js['Email']:
            rm.email_addr = js['Email']
        if rm.email_addr is None:
            rm.email_addr = ''
        if rm.email_addr == 'vivi@lagou.com':
            rm.email_addr = ''
        rm.now_location = js['NowLocation']
        rm.hope_location = js['Forwardlocation']
        rm.high_education = js['Education']
        if rm.high_education == u'大專':
            rm.high_education = u'大专'
        rm.high_edu_id = getEduHighId(rm.high_education)
        rm.advance_degree = js['AdvancedDegree']
        rm.exp_name = ''
        rm.now_vocation = js['Vocation']
        rm.hope_vocation = js['ForwardVocation']
        rm.vocation_standard = js['VocationStandard']
        rm.now_salary = js['Salary']
        rm.hope_salary = js['AimSalary']
        rm.political_name = js['Political']
        rm.start_from = js['StartFrom']
        rm.apply_switch = js['Switch']
        rm.qq_no = js['QQ']
        rm.student_type = js['StudentType']
        rm.photo_url = js['PhotoUrl']
        rm.apply_letter = js['AppLetter']
        rm.last_company = js['LastCompany']
        rm.last_title = js['LastTitle']
        rm.overseas_work = js['OverseasWork']
        rm.job_hope_frequency = js['JobHoppingFrequency']
        rm.integrity_ratio = js['Integrity']
        rm.work_type = js['WorkType']
        rm.birth_date = getBirthDay(js['Birth'])
        rm.work_years = str2int(js['Experience'])
        rm.source = js.get('source', 0)
        rm.matching_degree = js.get('matching', 0)
        rm.is_valid = 'T'
        rm.modify_user = 'zpb'
        rm.modify_time = datetime.today()
        session.add(rm)
        # 简历扩展
        rext = ResumeExtend()
        rext.resume_code = rm.resume_code
        rext.org_resume = fmtHtml(js['Original'])
        rext.edu_detail_full = js['EducationDetail'].replace('\r\n', '<br />')
        rext.exp_detail_full = js['ExperienceDetail'].replace('\r\n', '<br />')
        rext.train_detail_full = js['Training'].replace('\r\n', '<br />')
        rext.proj_detail_full = js['Project'].replace('\r\n', '<br />')
        rext.skill_detail_full = js['Skill'].replace('\r\n', '<br />')
        rext.is_valid = 'T'
        rext.create_user = 'zpb'
        rext.create_time = datetime.today()
        session.add(rext)
        #
        if js['EducationInfo']:
            for (idx, item) in enumerate(js['EducationInfo']):
                redu = ResumeEdu()
                redu.resume_code = rm.resume_code
                redu.start_date = item['StartDate']
                redu.end_date = item['EndDate']
                redu.school_name = item['School']
                redu.major_name = item['Speciality']
                redu.adv_degree = item['AdvancedDegree']
                redu.diplomas_name = item['Education']
                redu.depart_name = item['Department']
                redu.edu_summary = item['Summary'].replace('\r\n', '<br />')
                redu.is_studii = item['IsStudii']
                redu.is_valid = 'T'
                session.add(redu)
                # 异步增加百科超链接
                dtc.async('zpb.business.model.hyperlink.AppendSchoolLink', redu.school_name)
        #
        if js['ExperienceInfo']:
            for (idx, item) in enumerate(js['ExperienceInfo']):
                rexp = ResumeExp()
                rexp.resume_code = rm.resume_code
                rexp.start_date = item['StartDate']
                rexp.end_date = item['EndDate']
                rexp.periods_of_time = item['PeriodsOfTime']
                # 最近工作的时间长度写入resume_base表
                if idx == 0:
                    rm.periods_of_time = rexp.periods_of_time
                rexp.company_name = item['Company']
                rexp.work_location = item['Location']
                rexp.vocation_name = item['Vocation']
                rexp.company_scale = item['Size']
                rexp.company_type = item['Type']
                rexp.depart_name = item['Department']
                rexp.work_title = item['Title']
                rexp.salary_name = item['Salary']
                rexp.work_summary = item['Summary'].replace('\r\n', '<br />')
                rexp.leader_name = item['Leader']
                rexp.underling_num = str2int(item['UnderlingNumber'])
                rexp.leaving_reason = item['ReasonOfLeaving']
                rexp.is_valid = 'T'
                session.add(rexp)
                # 异步增加百科超链接
                dtc.async('zpb.business.model.hyperlink.AppendCompanyLink', rexp.company_name)
        #
        if js['TrainingInfo']:
            for item in js['TrainingInfo']:
                rtra = ResumeTrain()
                rtra.resume_code = rm.resume_code
                rtra.start_date = item['StartDate']
                rtra.end_date = item['EndDate']
                rtra.train_institution = item['TrainingInstitution']
                rtra.train_location = item['TrainingLocation']
                rtra.train_course = item['TrainingCourse'].replace('\r\n', '<br />')
                rtra.train_certificate = item['Certificate']
                rtra.train_desc = item['DescriptionInDetails'].replace('\r\n', '<br />')
                rtra.is_valid = 'T'
                session.add(rtra)
        #
        if js['ProjectInfo']:
            for item in js['ProjectInfo']:
                rpro = ResumeProject()
                rpro.resume_code = rm.resume_code
                rpro.start_date = item['StartDate']
                rpro.end_date = item['EndDate']
                rpro.project_name = item['ProjectName']
                rpro.work_title = item['Title']
                rpro.proj_desc = item['ProjectDescription'].replace('\r\n', '<br />')
                rpro.work_responsibility = item['Responsibilities'].replace('\r\n', '<br />')
                rpro.is_valid = 'T'
                session.add(rpro)
        #
        if js['LanguagesSkills']:
            for item in js['LanguagesSkills']:
                rlan = ResumeLang()
                rlan.resume_code = rm.resume_code
                rlan.lang_name = item['Languages']
                rlan.listen_speak = item['ListeningSpeakingSkills']
                rlan.write_read = item['ReadingWritingSkills']
                rlan.lang_score = item['Score']
                rlan.lang_skill = item['Skills']
                rlan.is_valid = 'T'
                session.add(rlan)
        #
        if js['ITSkills']:
            for item in js['ITSkills']:
                rit = ResumeIT()
                rit.resume_code = rm.resume_code
                rit.skill_name = item['SkillType']
                rit.use_time = item['TimeOfUse']
                rit.competency_level = item['CompetencyLevel']
                rit.is_valid = 'T'
                session.add(rit)
        #
        if js['GradeOfEnglish']:
            item = js['GradeOfEnglish']
            if item['NameOfCertificate']:
                reng = ResumeEnglish()
                reng.resume_code = rm.resume_code
                reng.certificate_name = item['NameOfCertificate']
                reng.certificate_score = item['Score']
                reng.rec_date = item['ReceivingDate']
                reng.is_valid = 'T'
                session.add(reng)
        session.commit()
        resume_code = rm.resume_code
        return (True, resume_code, True)
    except BaseException as e:
        session.rollback()
        logger.error(u'简历解析失败,原因:{}'.format(e))
        return (False, u'简历解析失败!', False)
    finally:
        session.close()
