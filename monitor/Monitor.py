#! /usr/bin/env python
# --*-- coding:utf-8 --*--


__author__ = 'jeff'

import sys
sys.path.append('/home/dev/yufeng/Supervisor')

import os
import datetime
import socket
import gzip
import time
from db.mysql import SupervisorDao



def get_ip_address():
    return socket.gethostbyname(socket.gethostname())

def datetime_timestamp(dt):
     s = time.mktime(time.strptime(dt, '%Y-%m-%d'))
     return int(s)



class Compare(object):

    def __init__(self, yesterday = None, today = None):
        if not yesterday and not today:
            self.yesterday, self.today = self.getDate()
        else:
            self.yesterday = yesterday
            self.today = today
        self.localip = get_ip_address()
        self.dao = SupervisorDao('10.1.5.60', 'monitor', 'monitor', 'monitordb')

    def getDate(self):
        today = datetime.datetime.now()
        yesterday = today + datetime.timedelta(days=-1)
        return yesterday.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

    def write2DB(self):
        # get cmd
        kafkaFileName = '/data2/kafkaService/kafka.log.{0}.gz'.format(self.yesterday)
        kafkaFileHandler = gzip.open(kafkaFileName, 'rt')
        kafkaCount = 0
        for line in kafkaFileHandler:
            if 'success' in line:
                kafkaCount += 1

        batchFileList = os.listdir('/data2/druidBatchData/')
        batchCurrentFileList = [filename for filename in batchFileList if filename.startswith('ip-{0}.ec2.internaldata_{1}T'.format(self.localip, self.yesterday))]
        batchCount = 0
        for batchFile in batchCurrentFileList:
            fobj = open(os.path.join('/data2/druidBatchData/', batchFile), 'r')
            for data in fobj:
                batchCount += 1

        hadoopFileList = os.listdir('/data1/ymds_logs/yfnormalpf')
        hadoopCurrentFileList = [filename for filename in hadoopFileList if filename.startswith('ip-{0}.ec2.internal_{1}T'.format(self.localip, self.yesterday))]
        hadoopCount = 0
        for hadoopFile in hadoopCurrentFileList:
            fobj = open(os.path.join('/data1/ymds_logs/yfnormalpf', hadoopFile), 'r')
            for data in fobj:
                hadoopCount += 1

        if self.localip.endswith('12'):
            collectorList = [self.yesterday, kafkaCount, 0, 'collector']
            batchList = [self.yesterday, batchCount, 0, 'batch']
            hadoopList = [self.yesterday, hadoopCount, 0, 'hadoop']

        print collectorList
        self.dao.writeDataMonitor(collectorList)

if __name__ == '__main__':
    c = Compare()
    c.write2DB()

