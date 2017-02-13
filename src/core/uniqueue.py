#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: /Users/tangming/work/zpb/core/uniqueue.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-28 17:33
#########################################################################


# zpb
from zpb.cache.rediscache import TQRedis
from zpb.conf import Conf, logger


# unique queue with redis SortedSet
class UniSortedQueue(object):

    __cache__ = {}

    def __new__(cls, prefix):
        self = cls.__cache__.get(prefix)
        if self is not None:
            return self
        else:
            cls.__cache__[prefix] = self = object.__new__(cls)
            self.redisCli = TQRedis.GetRedis(Conf.REDIS_TASK_UNI_SOURCE)
            self.prefix = prefix
            self.queuekey = '{}_unique_queue'.format(self.prefix)
            self.setkey = '{}_unique_set'.format(self.prefix)
            # note: just one producter
            self.redisCli.delete(self.queuekey)
            self.redisCli.delete(self.setkey)
        return self

    def enqueue(self, key, task, score = 0):
        if self.redisCli.sadd(self.setkey, key):
            self.redisCli.zadd(self.queuekey, task, score)

    # 支持弹出多项任务(默认一项任务,number=0表示弹出所有任务)
    def dequeue(self, number = 1):
        items = []
        tasks = self.redisCli.zrange(self.queuekey, 0, number - 1)
        if tasks:
            for task in tasks:
                if self.redisCli.zrem(self.queuekey, task):
                    items.append(task)
        return items

    # 任务处理完成后,需要回调该函数
    def remove(self, key):
        self.redisCli.srem(self.setkey, key)


if __name__ == '__main__':
    taskqueue = UniSortedQueue('task')
    taskqueue.push('a', 1000)
    taskqueue.push('b', 1100)
    taskqueue.push('c', 1200)
    for i in xrange(10):
        taskqueue.push(i, i * 100)
    items = taskqueue.pop(3)
    print items
    taskqueue = UniSortedQueue('task')
    taskqueue.push('d', 1300)
    items = taskqueue.pop(3)
    print items
    print '======one====='
    print taskqueue.pop()
