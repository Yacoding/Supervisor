#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import pymongo
import json
import smtplib,email,sys
from email.mime.text import MIMEText


def getdbdata():
    try:
        conn = pymongo.Connection('10.1.5.60', 27017)
    except Exception as e:
        raise e
    else:
        cursor = conn.monitordb.monitor


    find = cursor.find()
    day, collector_12, collector_13, total_collector, batch_12, batch_13, total_batch, hadoop_12, hadoop_13, total_hadoop, ymtotal, repeatConv, tdtotal = 13 * [0]
    missHour = ''
    for data in find:
        del data['_id']
        if data.get('ip') == '10-1-15-13':
            collector_13 = data.get('collector')
            batch_13 = data.get('batch')
            hadoop_13 = data.get('hadoop')
            day = data.get('datetime')
        if data.get('ip') == '10-1-15-12':
            collector_12 = data.get('collector')
            batch_12 = data.get('batch')
            hadoop_12 = data.get('hadoop')
        if 'druidTotal' in data:
            ymtemp = json.loads(data.get('druidTotal')).get('data').get('data')[1]
            ymtotal = '{0} {1}'.format(ymtemp[0], ymtemp[1])
        if 'repeatConv' in data:
            repeatConv = len(json.loads(data.get('repeatConv')).get('data').get('data')) - 1
        if 'TdData' in data:
            tdtemp = json.loads(data.get('TdData')).get('data').get('data')[1]
            tdtotal = '{0} {1}'.format(tdtemp[0], tdtemp[1])
        if 'hourData' in data:
            dataSet = json.loads(data.get('hourData')).get('data').get('data')[1:]
            for data in dataSet:
                if data[2] == 0 or data[3] == 0:
                    missHour += 'UTC: {0}-{1}\n'.format(data[0], data[1])

    total_collector = collector_12 + collector_13
    total_batch = batch_12 + batch_13
    total_hadoop = hadoop_12 + hadoop_13
    cursor.remove()
    return day, collector_12, collector_13, total_collector, batch_12, batch_13, total_batch, hadoop_12, hadoop_13, total_hadoop, ymtotal, repeatConv, tdtotal, missHour

def getHtmlContent():
    dataSet = dict(zip(["day","collector_12","collector_13","total_collector","batch_12","batch_13","total_batch","hadoop_12","hadoop_13","total_hadoop","ymtotal","repeatConv","tdtotal","missHour"],
                       getdbdata()))
    html_template = """
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <title></title>
</head>
<body>
    <table border="1">
        <tr>
            <td>{0}</td>
            <td></td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>采集器</td>
            <td>{1}</td>
            <td>{2}</td>
            <td>{3}</td>
        </tr>
        <tr>
            <td>批量导数</td>
            <td>{4}</td>
            <td>{5}</td>
            <td>{6}</td>
        </tr>
        <tr>
            <td>HADOOP</td>
            <td>{7}</td>
            <td>{8}</td>
            <td>{9}</td>
        </tr>
        <tr>
            <td>联盟点击转化总量</td>
            <td>{10}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>HASOFFER重复转化</td>
            <td>{11}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>EVE点击转化总量</td>
            <td>{12}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>按小时查询DRUID是否有点击或者转化为0</td>
            <td>{13}</td>
            <td></td>
            <td></td>
        </tr>
    </table>
</body>
</html>
                    """
    return html_template.format(dataSet.get('day'),
                                dataSet.get('collector_12'),
                                dataSet.get('collector_13'),
                                dataSet.get('total_collector'),
                                dataSet.get('batch_12'),
                                dataSet.get('batch_13'),
                                dataSet.get('total_batch'),
                                dataSet.get('hadoop_12'),
                                dataSet.get('hadoop_13'),
                                dataSet.get('total_hadoop'),
                                dataSet.get('ymtotal'),
                                dataSet.get('repeatConv'),
                                dataSet.get('tdtotal'),
                                dataSet.get('missHour'))

smtpserver='smtp.163.com'
smtpuser='15251826346@163.com'
smtppass='bmeB500!'
smtpport='25'

def connect():
    server=smtplib.SMTP(smtpserver,smtpport)
    server.ehlo()
    server.login(smtpuser,smtppass)
    return server

def sendmessage(server,to,subj,content):
    msg = MIMEText(content, _subtype='html', _charset='gb2312')
    msg['Subject'] = subj
    try:
        failed = server.sendmail(smtpuser,to,msg.as_string())   # may also raise exc
    except Exception as ex:
        print 'Error - send failed'
    else:
        print 'send success'

if __name__=="__main__":
    to='jeff.yu@ndpmedia.com'
    subj='每日统计'
    text = getHtmlContent()
    server=connect()
    sendmessage(server,to,subj,text)
