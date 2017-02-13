#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/threadpool.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-31 16:39
#########################################################################


# stdlib
import sys
import threading
import Queue
import traceback
# zpb
from zpb.conf import logger


# 自定义异常处理
class NoResponsePending(Exception):
    pass

class NoWorkerAvailable(Exception):
    pass

def _handle_thread_exception(request, exc_info):
    traceback.print_exception(*exc_info)


class WorkerThread(threading.Thread):

    def __init__(self, requestQueue, responseQueue, poll_timeout = 5, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.setDaemon(True)
        self._requestQueue = requestQueue
        self._responseQueue = responseQueue
        self._poll_timeout = poll_timeout
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        while 1:
            if self._dismissed.is_set():
                break
            try:
                request = self._requestQueue.get(True, self._poll_timeout)
            except Queue.Empty:
                continue
            else:
                if self._dismissed.is_set():
                    self._requestQueue.put(request)
                    break
                try:
                    response = request.callable(*request.args, **request.kwargs)
                    self._responseQueue.put((request, response))
                except:
                    request.exception = True
                    self._responseQueue.put((request, sys.exc_info()))

    def dismiss(self):
       self._dismissed.set()


class WorkRequest(object):

    def __init__(self, callable_, args = None, kwargs = None, requestID = None, callback = None, exc_callback = _handle_thread_exception):
        if requestID is None:
            self.requestID = id(self)
        else:
            try:
                self.requestID = hash(requestID)
            except TypeError:
                raise TypeError("requestId must be hashable")
        self.exception = False
        self.callback = callback
        self.exc_callback = exc_callback
        self.callable = callable_
        self.args = args or []
        self.kwargs = kwargs or {}

    def __str__(self):
        return "WorkRequest id=%s args=%r kwargs=%r exception=%s" % \
                (self.requestID, self.args, self.kwargs, self.exception)


class ThreadPool(object):

    def __init__(self, numOfWorkers, q_size = 0, resq_size = 0, poll_timeout = 5):
        self._requestQueue = Queue.Queue(q_size)
        self._responseQueue = Queue.Queue(resq_size)
        self.workers = []
        self.dismissedWorkers = []
        self.workRequests = {}
        self.createWorkers(numOfWorkers, poll_timeout)

    def createWorkers(self, numOfWorkers, poll_timeout = 5):
        for _ in xrange(numOfWorkers):
            self.workers.append(WorkerThread(self._requestQueue, self._responseQueue, poll_timeout = poll_timeout))

    def dismissWorkers(self, numOfWorkers, doJoin = False):
        dismiss_list = []
        for _ in xrange(numOfWorkers):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)
        if doJoin:
            for worker in dismiss_list:
                worker.join()
        else:
            self.dismissedWorkers.extend(dismiss_list)

    def joinAllDismissedWorkers(self):
        for worker in self.dismissedWorkers:
            worker.join()
        self.dismissedWorkers = []

    def putRequest(self, request, block = True, timeout = None):
        assert isinstance(request, WorkRequest)
        assert not getattr(request, 'exception', None)
        self._requestQueue.put(request, block, timeout)
        self.workRequests[request.requestID] = request

    def poll(self, block = False):
        while 1:
            if not self.workRequests:
                raise NoResponsePending
            elif block and not self.workers:
                raise NoWorkerAvailable
            try:
                request, response = self._responseQueue.get(block = block)
                if request.exception and request.exc_callback:
                    request.exc_callback(request, response)
                if request.callback and not (request.exception and request.exc_callback):
                    request.callback(request, response)
                del self.workRequests[request.requestID]
            except Queue.Empty:
                break

    def wait(self):
        while 1:
            try:
                self.poll(True)
            except NoResponsePending:
                logger.info(u'Thread Pool stoped by NoResponsePending')
                break
            except NoWorkerAvailable:
                logger.info(u'Thread Pool stoped by NoWorkerAvailable')
                break

    def workersize(self):
        return len(self.workers)

    def stop(self):
        self.dismissWorkers(self.workersize(), True)
        self.joinAllDismissedWorkers()
