#!/usr/bin/env python

# -*- coding: utf-8 -*-

'''
@File         : api.py
@Date         : 2018-11-29
@Author       : tzy
@Description  : python client DB
'''

import pymysql
from dbutil import DbUtil
from pyhive import hive, presto

'''
Use pyhive

'''


class Hive(DbUtil):
    def __init__(self, host=None, port=None, username=None, database='default', auth=None,
                 configuration=None, kerberos_service_name=None, password=None,
                 thrift_transport=None):
        conn = hive.connect(host, port, username, database, auth,
                            configuration, kerberos_service_name, password,
                            thrift_transport)

        cursor = conn.cursor()

        super(Hive, self).__init__(conn, cursor)


'''
Use pymysql
'''


class Mysql(DbUtil):
    def __init__(self,
                 host=None,
                 username=None,
                 password="",
                 database=None,
                 port=3306,
                 charset='utf8',
                 **kwargs):
        conn = pymysql.connect(host,
                               user=username,
                               password=password,
                               database=database,
                               port=port,
                               charset=charset,
                               **kwargs)

        cursor = conn.cursor()

        super(Mysql, self).__init__(conn, cursor)


'''
Use pyhive.presto
'''


class Presto(DbUtil):
    def __init__(self,
                 host, port=8080, username=None, catalog='hive',
                 schema='default', poll_interval=1, source='pyhive', session_props=None,
                 protocol='http', password=None, requests_session=None, requests_kwargs=None,
                 **kwargs):
        conn = presto.connect(host, port, username, catalog,
                              schema, poll_interval, source, session_props,
                              protocol, password, requests_session, requests_kwargs,
                              **kwargs)

        cursor = conn.cursor()

        super(Presto, self).__init__(conn, cursor)


