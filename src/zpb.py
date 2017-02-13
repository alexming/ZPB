#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: zpb.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2016-01-08 17:49
#########################################################################


# stdlib
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 系统配置
os.environ['SETTINGS_MODULE'] = 'settings'
# zpb
from zpb.daemon import Daemon
from zpb.app import Application


def app_callback():
    Application().start()


if __name__ == '__main__':
    app_name = os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0]
    daemon = Daemon('/tmp/zpb.pid', app_name, app_callback, stderr='/dev/stderr')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print 'unkown command'
            sys.exit(2)
        sys.exit(0)
    elif len(sys.argv) == 1:
        daemon.stdin = '/dev/stdin'
        daemon.stdout = '/dev/stdout'
        daemon.stderr = '/dev/stderr'
        daemon._run()
    else:
        print 'usage: %s start|stop|restart' % sys.argv[0]
        sys.exit(2)
