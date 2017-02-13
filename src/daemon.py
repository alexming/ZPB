#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: daemon.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-26 12:42
#########################################################################


# stdlib
import sys
import os
import os.path
import atexit


class Daemon:

    def __init__(self, pidfile, appname, callback, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        # 需要获取调试信息,修改为标准输入/输出(/dev/stdin, /dev/stdout, /dev/stderr)
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.appname = appname
        self.callback = callback

    def _daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            sys.stderr.write('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
        # 修改工作目录
        os.chdir('/')
        # 设置新的回话连接
        os.setsid()
        # 重新设置文件创建权限
        os.umask(0)
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
        # 重定向文件描述符
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        # 注册退出函数
        # atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write('%s\n' % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        # 检查pid文件是否存在进程
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if pid:
            message = 'pidfile %s already exists. Deamon already running!\n'
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        if not self.appname:
            message = 'Deamon must assign param: appname!\n'
            sys.stderr.write(message)
            sys.exit(1)
        if not self.callback:
            message = 'Deamon must assign param: callback!\n'
            sys.stderr.write(message)
            sys.exit(1)
        if not callable(self.callback):
            message = 'Deamon callback must be callable!\n'
            sys.stderr.write(message)
            sys.exit(1)
        # 启动监控
        self._daemonize()
        self._run()

    def stop(self):
        # 从pid文件获取pid
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = 'pidfile %s does not exists. Deamon not running!\n'
            sys.stderr.write(message % self.pidfile)
            return
        # 杀进程
        try:
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            os.system("ps -ef|grep {}|grep -v grep|awk '{{print $2}}'|xargs kill -15".format(self.appname))
        except OSError as err:
            print str(err)

    def restart(self):
        self.stop()
        self.start()

    def _run(self):
        self.callback()