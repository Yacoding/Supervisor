#! /usr/bin/env python
# --*-- coding:utf-8 --*--

__author__ = 'jeff.yu'

import pymongo



class SupervisorDao(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.setConn()

    def setConn(self):
        try:
            db = pymongo.Connection(self.host, self.port)
        except Exception as e:
            raise e
        else:
            self.cursor = db.monitor

    def insertCollection(self, dataSet):
        self.cursor.insert(dataSet)