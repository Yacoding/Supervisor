#! /usr/bin/env python
# --*-- coding:utf-8 --*--


__author__ = 'jeff'

import os
import sys
sys.path.append(os.path.split(os.path.split(__file__)[0])[0])

import datetime
import socket
import gzip
import time
import urllib
import urllib2
from db.mongo import SupervisorDao



def get_ip_address():
    return socket.gethostbyname(socket.gethostname()).replace('.', '-')

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
        self.dao = SupervisorDao('10.1.5.60', 27017)

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

        dataSet = dict()
        dataSet['datetime'] = self.yesterday
        dataSet['ip'] = self.localip
        dataSet['collector'] = kafkaCount
        dataSet['batch'] = batchCount
        dataSet['hadoop'] = hadoopCount
        self.dao.insertCollection(dataSet)


class Query():

    def __init__(self):
        pass

    def convert2timestamp(self):
        c = Compare()
        self.start, self.end = c.getDate()
        self.unix_start, self.unix_end = datetime_timestamp(self.start), datetime_timestamp(self.end)
        self.dao = SupervisorDao('10.1.5.60', 27017)


    def getHttpData(self, ip, param):
        self.convert2timestamp()
        report_param = param % (self.unix_start, self.unix_end)
        url = 'http://{0}:18080/report/report?'.format(ip)
        encodeParam = urllib.urlencode({'report_param':report_param})
        rsp = urllib2.urlopen(url, encodeParam).read()
        return rsp

    def getDateTime(self, dayOffset = 0):
        now = datetime.datetime.today()
        OffsetDateTime = now + datetime.timedelta(days = dayOffset)
        return OffsetDateTime.strftime('%Y-%m-%dT%H').split('T')[0]

    def write2DB(self):
        start,end = self.getDateTime(-1), self.getDateTime()
        hasofferRepeatConv = self.getHttpData('10.1.15.15', '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":["transaction_id","day"],"data":["conversion2","conversion"],"filters":{"$and":{"datasource":{"$eq":"hasoffer"},"log_tye":{"$eq":1},"conversion":{"$gt":1}}},"sort":[]}')
        yeahmobiRepeatConv = self.getHttpData('10.1.15.15', '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":["transaction_id","day"],"data":["click","conversion"],"filters":{"$and":{"datasource":{"$neq":"hasoffer"},"status":{"$eq":"Confirmed"},"log_tye":{"$eq":1},"conversion":{"$gt":1}}},"sort":[]}')
        druidTotal = self.getHttpData('10.1.15.15', '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":[],"data":["click","conversion"],"filters":{"$and":{}},"sort":[]}')
        hourData = self.getHttpData('10.1.15.15', '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":["day","hour"],"data":["click","conversion"],"filters":{"$and":{}},"sort":[]}')
        TdData = self.getHttpData('10.1.15.29', '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"eve_druid_datasource_ds","pagination":{"size":1000000,"page":0}},"group":[],"data":["clicks","convs"],"filters":{"$and":{}},"sort":[]}')
        dataSet = dict()
        dataSet['hasofferRepeatConv'] = hasofferRepeatConv
        dataSet['yeahmobiRepeatConv'] = yeahmobiRepeatConv
        dataSet['druidTotal'] = druidTotal
        dataSet['hourData'] = hourData
        dataSet['TdData'] = TdData
        self.dao.insertCollection(dataSet)

if __name__ == '__main__':
    c = Compare()
    c.write2DB()
    q = Query()
    q.write2DB()

