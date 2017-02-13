# -*- encoding=utf-8 -*-

# stdlib
from urllib import quote_plus as urlquote
# SQLAlchemy
from sqlalchemy import create_engine, inspect, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.ext.declarative import declarative_base
# zpb
from zpb.core.Singleton import Singleton
from zpb.exception import *
from zpb.conf import Conf, logger

# 创建模型基类
_BaseModel = declarative_base()

ConnectionURI = '{dialect}+{driver}://{username}:%s@{host}:{port}/{database}?charset={charset}&use_unicode=0'.format(**Conf.DATABASE)
ConnectionURI = ConnectionURI % urlquote(Conf.DATABASE['password'])


def checkout_listener(dbapi_con, con_record, con_proxy):
    try:
        try:
            dbapi_con.ping(False)
        except TypeError:
            dbapi_con.ping()
    except dbapi_con.OperationalError as exc:
        if exc.args[0] in (2006, 2013, 2014, 2045, 2055):
            raise DisconnectionError()
        else:
            raise

class DB(Singleton):

    def initialization(self):
        # MYSQL数据库设置30秒连接超时,设定pool_recycle秒后自动重连
        self.db_engine = create_engine(ConnectionURI, pool_size=20, max_overflow=10, pool_recycle=10, echo=False)
        event.listen(self.db_engine, 'checkout', checkout_listener)
        self.DBSession = sessionmaker(bind=self.db_engine)

    def ping(self):
        session = DBInstance.session
        session.execute('SELECT 1')

    @property
    def session(self):
        return self.DBSession()

# 全部DB事例
DBInstance = DB()
# DBInstance.ping()

# ORM基础模型,提供通用的保存接口
class BaseModel(_BaseModel):

    __abstract__ = True

    # 保存
    def save(self):
        session = DBInstance.session
        try:
            insp = inspect(self)
            if insp.detached or insp.persistent:
                self = session.merge(self)
            session.add(self)
            session.commit()
            return True
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(self), e)
        finally:
            session.close()

    # 保存并再次刷新
    def saveAndRefresh(self):
        session = DBInstance.session
        try:
            insp = inspect(self)
            if insp.detached or insp.persistent:
                self = session.merge(self)
            session.add(self)
            session.commit()
            session.refresh(self)
            return True
        except BaseException as e:
            session.rollback()
            raise DBOperateError(currentFuncName(self), e)
        finally:
            session.close()
