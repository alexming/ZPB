#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: app.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-18 16:31
#########################################################################


# stdlib
import signal
from time import sleep
from multiprocessing import Event, Process, current_process
# zpb
from zpb.service import webservice
from zpb.business.model.dbtask import DBTask
from zpb.business.model.emailconf import EmailConf
from zpb.business.model.implocalfile import ImpLocalFile
from zpb.business.model.resumesearcher import ResumeSearcher
from zpb.business.model.ressyncdistribute import ResSyncDistribute
from zpb.business.model.hyperlink import HyperLink
from zpb.conf import logger
# dtc
import dtc
from dtc import Cluster, async
from dtc.conf import get_ppid


class Application(object):

    def __init__(self):
        self.sentinel = None
        self.pid = current_process().pid
        self.stop_event = None
        self.start_event = None
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)

    def start(self):
        # start Sentinel
        self.stop_event = Event()
        self.start_event = Event()
        self.sentinel = Process(target=Sentinel, args=(self.stop_event, self.start_event))
        self.sentinel.start()
        logger.info('Application-{} starting.'.format(self.pid))
        while not self.start_event.is_set():
            sleep(0.1)
        return self.pid

    def stop(self):
        if not self.sentinel.is_alive():
            return False
        logger.info('Application-{} stopping.'.format(self.pid))
        self.stop_event.set()
        self.sentinel.join()
        logger.info('Application-{} has stopped.'.format(self.pid))
        self.start_event = None
        self.stop_event = None
        return True

    def sig_handler(self, signum, frame):
        logger.debug('{} got signal {}'.format(current_process().name, signum))
        self.stop()


class Sentinel(object):
    def __init__(self, stop_event, start_event, start=True):
        # Make sure we catch signals for the pool
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        self.pid = current_process().pid
        self.parent_pid = get_ppid()
        self.name = current_process().name
        self.stop_event = stop_event
        self.start_event = start_event
        self.event_out = Event()
        self.cluster = None
        self.dispatcher = None
        self.webapper = None
        if start:
            self.start()

    def start(self):
        self.cluster = self.spawn_cluster()
        self.dispatcher = self.spawn_dispatcher()
        self.webapper = self.spawn_webapper()
        self.guard()

    def spawn_process(self, target, *args):
        """
        :type target: function or class
        """
        p = Process(target=target, args=args)
        if target == cluster:
            p.daemon = False
        else:
            p.daemon = True
        p.start()
        return p

    def spawn_cluster(self):
        return self.spawn_process(cluster)

    def spawn_dispatcher(self):
        return self.spawn_process(dispatcher, self.event_out)

    def spawn_webapper(self):
        return self.spawn_process(webapper)

    def guard(self):
        logger.info('{} guarding Application at {}'.format(current_process().name, self.pid))
        self.start_event.set()
        logger.info('Application-{} running.'.format(self.parent_pid))
        cycle = 0.5 # guard loop sleep in seconds
        # guard loop. Runs at least once
        while not self.stop_event.is_set():
            # Check dispatcher
            if not self.dispatcher.is_alive():
                self.dispatcher = self.spawn_dispatcher()
                logger.error('reincarnated dispatcher {} after sudden death'.format(self.dispatcher.name))
            sleep(cycle)
        self.stop()

    def stop(self):
        name = current_process().name
        logger.info('{} stopping application processes'.format(name))
        # Stopping pusher
        self.event_out.set()
        # Wait for it to stop
        while self.cluster.is_alive():
            sleep(0.1)
        while self.webapper.is_alive():
            sleep(0.1)


def cluster():
    c = Cluster()
    c.start()

def webapper():
    webservice.RunWebApp()

def dispatcher(event):
    def _async_task(tasks):
        # 分发异步任务
        for task in tasks:
            taskname = task.get('name', False)
            if taskname:
                logger.debug(u'dispatcher task named <{}>'.format(taskname))
            args = task.get('args', ())
            kwargs = task.get('kwargs', {})
            kwargs['uid'] = task['id']
            dtc.async(task['func'], *args, **kwargs)
    #
    name = current_process().name
    pid = current_process().pid
    logger.info('{} dispatch tasks at {}'.format(name, pid))
    while not event.is_set():
        try:
            # 任务指令中心
            task_set = DBTask.queryPending() or []
            _async_task(task_set)
            # 手动简历解析
            task_set = ImpLocalFile.queryPending() or []
            _async_task(task_set)
            # 付费简历下载
            task_set = ResSyncDistribute.queryPending() or []
            _async_task(task_set)
            # 邮件搜索定时任务
            task_set = EmailConf.queryPending() or []
            _async_task(task_set)
            # 简历搜索定时任务
            task_set = ResumeSearcher.queryPending() or []
            _async_task(task_set)
            # 关键词百科超链接
            task_set = HyperLink.queryPending() or []
            _async_task(task_set)
            #
            sleep(2)
        except BaseException as e:
            logger.error(e)
            break
    logger.info('{} stopped dispatch tasks'.format(name))
