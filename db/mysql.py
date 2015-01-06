#! /usr/bin/env python
# --*-- coding:utf-8 --*--

__author__ = 'jeff.yu'

import MySQLdb



class SupervisorDao(object):

    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.setConn()

    def setConn(self):
        try:
            conn = MySQLdb.connect(host = self.host,
                                   user = self.user,
                                   passwd = self.password,
                                   db = self.db,
                                   charset = "utf8")
        except Exception as e:
            raise e
        else:
            self.cursor = conn.cursor()

    def writeDataMonitor(self, dataList):
        sql = 'insert into datamonitor values(%s, %s, %s, %s)'
        n = self.cursor.execute(sql, dataList)
        return n


    def writeQueryMonitor(self, dataList):
        sql = 'insert into querymonitor values(%s, %s, %s)'
        n = self.cursor.execute(sql, dataList)
        return n
