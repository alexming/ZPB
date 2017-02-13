#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: core/Singleton.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:02
#########################################################################


class Singleton(object):

    def __new__(cls, *args, **kwargs):
        it = cls.__dict__.get('__it__')
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.initialization()
        return it

    # 初始化函数
    def initialization(self):
        pass

    # 终结者函数
    def finalization(self):
        pass
