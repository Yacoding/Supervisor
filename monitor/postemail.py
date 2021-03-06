#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import pymongo
import json
import datetime
import smtplib, email, sys
from email.mime.text import MIMEText


def getDate():
    t = datetime.datetime.today()
    return t.strftime('%Y-%m-%d')


def getdbdata():
    try:
        conn = pymongo.Connection('10.1.5.60', 27017)
    except Exception as e:
        raise e
    else:
        cursor = conn.monitordb.monitor

    find = cursor.find()
    day, collector_12, collector_13, total_collector, batch_12, batch_13, total_batch, hadoop_12, hadoop_13, total_hadoop, ymtotal, hasofferRepeatConv, yeahmobiRepeatConv, tdtotal = 14 * [
        0]
    hasredundantConv = False
    missHour = False
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
            if ymtemp[0] > ymtemp[1]:
                ymtotal = '{0} {1}'.format(ymtemp[0], ymtemp[1])
            else:
                ymtotal = '{0} {1}'.format(ymtemp[1], ymtemp[0])
        if 'hasofferRepeatConv' in data:
            try:
                hasofferRepeatConv = len(json.loads(data.get('hasofferRepeatConv')).get('data').get('data')) - 1
            except ValueError as e:
                raise e
        if 'yeahmobiRepeatConv' in data:
            try:
                yeahmobiRepeatConv = len(json.loads(data.get('yeahmobiRepeatConv')).get('data').get('data')) - 1
            except ValueError as e:
                raise e
        if 'hasredundantConv' in data:
            try:
                hasredundantConvData = json.loads(data.get('hasredundantConv')).get('data').get('data')
                for convdata in hasredundantConvData[1:]:
                    if convdata[-1] > 0:
                        hasredundantConv = True
                    else:
                        pass
            except ValueError as e:
                raise e
        if data.has_key('TdData'):
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
    return day, collector_12, collector_13, total_collector, batch_12, batch_13, total_batch, hadoop_12, hadoop_13, total_hadoop, ymtotal, hasofferRepeatConv, yeahmobiRepeatConv, hasredundantConv, tdtotal, missHour


def getHtmlContent():
    dataSet = dict(zip(
        ["day", "collector_12", "collector_13", "total_collector", "batch_12", "batch_13", "total_batch", "hadoop_12",
         "hadoop_13", "total_hadoop", "ymtotal", "hasofferRepeatConv", "yeahmobiRepeatConv", "hasredundantConvData",
         "tdtotal", "missHour"],
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
            <td>10.1.15.12</td>
            <td>10.1.15.13</td>
            <td>总量</td>
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
            <td>Hasoffer重复转化</td>
            <td>{11}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>Yeahmobi重复转化</td>
            <td>{12}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>是否存在除Confirmed状态以外的转化</td>
            <td>{13}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>EVE点击转化总量</td>
            <td>{14}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>按小时查询DRUID是否有点击或者转化为0</td>
            <td>{15}</td>
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
                                dataSet.get('hasofferRepeatConv'),
                                dataSet.get('yeahmobiRepeatConv'),
                                convert(dataSet.get('hasredundantConv')),
                                dataSet.get('tdtotal'),
                                convert(dataSet.get('missHour')))


smtpserver = 'smtp.163.com'
smtpuser = '15251826346@163.com'
smtppass = 'bmeB500!'
smtpport = '25'


def convert(boolean):
    if boolean:
        return "Exist"
    else:
        return "Not Exist"


def connect():
    server = smtplib.SMTP(smtpserver, smtpport)
    server.ehlo()
    server.login(smtpuser, smtppass)
    return server


def sendmessage(server, to, subj, content):
    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['Subject'] = subj
    msg['To'] = ','.join(to)
    try:
        failed = server.sendmail(smtpuser, to, msg.as_string())  # may also raise exc
    except Exception as ex:
        print 'Error - send failed'
    else:
        print '{0}: send success'.format(getDate())


if __name__ == "__main__":
    # toList = ['bigdata@ndpmedia.com','robin.hu@ndpmedia.com', 'jeff.yu@ndpmedia.com', 'hardy.tan@ndpmedia.com']
    toList = ['jeff.yu@ndpmedia.com']
    subj = 'Daily Statistics Of {0}'.format(getDate())
    text = getHtmlContent()
    server = connect()
    sendmessage(server, toList, subj, text)
