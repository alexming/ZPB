#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: utils/fixedoffset.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-12-21 16:11
#########################################################################


# stdlib
from datetime import timedelta, tzinfo


class FixedOffset(tzinfo):
    """Fixed offset in minutes: `time = utc_time + utc_offset`."""
    def __init__(self, offset):
        self.__offset = timedelta(minutes=offset)
        hours, minutes = divmod(offset, 60)
        self.__name = '<%+03d%2d>%+d' % (hours, minutes, -hours)

    def utcoffset(self, dt=None):
        return self.__offset

    def tzname(self, dt=None):
        return self.__name

    def dst(self, dt=None):
        return timedelta(0)

    def __repr__(self):
        return 'FixedOffset(%d)' % (self.utcoffset().total_seconds() / 60)
