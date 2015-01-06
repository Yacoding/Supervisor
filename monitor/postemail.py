#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import pymongo

def getdbdata():
    try:
        conn = pymongo.Connection('10.1.5.60', 27017)
    except Exception as e:
        raise e
    else:
        cursor = conn.monitordb.monitor


    find = cursor.find()
    dataSet = dict()
    for data in find:
        return None

def setHtmlContent():
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
            <td>collector</td>
            <td>{1}</td>
            <td>{2}</td>
            <td>{3}</td>
        </tr>
        <tr>
            <td>batch</td>
            <td>{4}</td>
            <td>{5}</td>
            <td>{6}</td>
        </tr>
        <tr>
            <td>hadoop</td>
            <td>{7}</td>
            <td>{8}</td>
            <td>{9}</td>
        </tr>
        <tr>
            <td>YM Total</td>
            <td>{10}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>Hasoffer Repeat Conv</td>
            <td>{11}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>TD TOTAL</td>
            <td>{12}</td>
            <td></td>
            <td></td>
        </tr>
    </table>
</body>
</html>
                    """




if __name__ == '__main__':
    getContent()
