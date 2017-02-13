# -*- encoding=utf-8 -*-


# stdlib
from datetime import datetime
# SQLAlchemy
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.dialects.mysql import DATETIME
# zpb
from zpb.business.model.basemodel import BaseModel, DBInstance
from zpb.business.model.basejob import BaseJob
from zpb.business.model.jobsyncdistribute import JobSyncDistribute
from zpb.business.decorate.downdecorator import PackPositionDown
from zpb.conf import logger


# 虚拟过渡表,用於职位下载
class GrabPosition(BaseModel):
    # 表名
    __tablename__ = 'grab_position'
    # 表结构
    pk_id = Column(Integer, primary_key=True)
    companyid = Column(Integer)
    siteid = Column(Integer)
    jobid = Column(String(50))
    jobtitle = Column(String(50))
    datepublish = Column(DATETIME)
    dateend = Column(String(10))
    status = Column(Integer, default=0)
    welfares = Column(String(200))
    salary = Column(String(20))
    workarea = Column(String(100))
    workaddr = Column(String(100))
    worktype = Column(String(10))
    workexperience = Column(String(10))
    education = Column(String(10))
    need = Column(String(10))
    positiontype = Column(String(50))
    positiontype2 = Column(String(50))
    jobdesc = Column(Text)
    agefrom = Column(String(2))
    ageto = Column(String(2))
    language1 = Column(String(20))
    languagelevel1 = Column(String(10))
    language2 = Column(String(20))
    languagelevel2 = Column(String(10))
    major1 = Column(String(50))
    major2 = Column(String(50))
    salarytype = Column(String(10))
    salarymin = Column(String(10))
    salarymax = Column(String(10))
    yearsalary = Column(String(2))
    monthsalary = Column(String(2))
    customsalary = Column(String(20))
    keywords = Column(String(200))
    emails = Column(String(150))
    sex = Column(String(5))

    def save(self):
        job = None
        jobid = JobSyncDistribute.queryJobIdByThirdJobCode(self.companyid, self.siteid, self.jobid)
        if jobid:
            job = BaseJob.queryOne(jobid)
        if not job:
            job = BaseJob()
            job.create_user = 'zpb'
            job.create_time = datetime.today()
        else:
            # 职位发布时间或职位状态变更
            if datetime.date(self.datepublish) > datetime.date(job.publish_date) or self.status != job.job_status:
                job.modify_user = 'zpb'
                job.modify_time = datetime.today()
            else:
                return True
        job.props = []
        job.tags = []
        if PackPositionDown(self, job):
            return job.save()

    # ORM序列化
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
            

if __name__ == '__main__':
    session = DBInstance.session
    try:
        p = session.query(GrabPosition).first()
        print p.pk_id, p.companyid, p.jobtitle
    except Exception, e:
        print str(e)
    session.close()
