#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: timerloggerhandler.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-25 19:14
#########################################################################


# stdlib
import os
import os.path
import logging
import datetime
# zpb
import zpb

_filefmt = os.path.join(zpb.root_path, 'logs', '%Y-%m-%d', '%H')


class TimerLoggerHandler(logging.Handler):

  def __init__(self, fmtsuffix=None):
    if fmtsuffix is None:
      self.filefmt = _filefmt + '.log'
    else:
      self.filefmt = _filefmt + fmtsuffix
    logging.Handler.__init__(self)

  def emit(self, record):
    msg = self.format(record)
    _filePath = datetime.datetime.now().strftime(self.filefmt)
    _dir = os.path.dirname(_filePath)
    try:
      if os.path.exists(_dir) is False:
        os.makedirs(_dir)
    except Exception:
      print "can not make dirs"
      print "filepath is "+_filePath
      pass
    try:
      _fobj = open(_filePath, 'a')
      _fobj.write(msg)
      _fobj.write("\n")
      _fobj.flush()
      _fobj.close()
    except Exception:
      print "can not write to file"
      print "filepath is " + _filePath
      pass
