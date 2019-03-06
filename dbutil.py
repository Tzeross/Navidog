#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@File         : db.py
@Date         : 2018-11-29
@Author       : tzy
@Description  : Description
'''
import logging
import pandas as pd

import sys

if sys.version_info.major == 2:
    reload(sys)
    sys.setdefaultencoding('utf-8')

from pyhive.hive import Connection as hvconn
from pymysql import Connect as mysqlconn

_logger = logging.getLogger(__file__)


def check_instance(func):
    '''
    mysql断线重连
    :param func:
    :return:
    '''

    def check(self, *args, **kwargs):
        conn, cursor = self.get_instance
        if isinstance(conn, mysqlconn):
            conn.ping(reconnect=True)
        return func(self, *args, **kwargs)

    return check


class DbUtil(object):
    def __init__(self, conn, cursor):
        self._conn = conn
        self._cursor = cursor

    def insert(self, table_name, items=None, **kwargs):
        '''
        增加数据
        :param table_name:table_name

        :param items:字典类型,当此字段不为空时，忽略kwargs，键名为字段名，键值为字段值

        :param kwargs:参数名为数据表字段名,参数值为字段值

        :example: insert('job_company', city='上海1', company='公司1')
                 insert('job_company', items={'city':'上海','company':'公司'})

        :return:如果有自增id则返回自增id

        '''
        if items:
            assert isinstance(items, dict)
            kwargs = items

        values = ('%s,' * len(kwargs.keys())).strip(',')

        if len(kwargs.keys()) <= 1:
            params = f"""({list(kwargs.keys())[0]})"""
        else:
            params = str(tuple(kwargs.keys())).replace('\'', '')

        data = tuple(kwargs.values())

        sql = "insert into {} {}  values ({})".format(table_name, params, values)

        if isinstance(self._conn, hvconn):
            # hive不支持缺失字段添加，缺失字段用None补齐
            sql = "insert into {} values ({})".format(table_name, values)

        return self.excute(sql, data=data)

    def delete(self, table_name, where=None):
        '''
        删除
        :param table_name:
        :param where:
        :return:
        '''
        where = 'where %s' % where if where else ''
        sql = 'delete from {} {}'.format(table_name, where)
        return self.excute(sql)

    def update(self, table_name=None, where=None, **kwargs):
        '''

        :param table_name: 表名
        :param where: 字符串 id>1
        :param kwargs: 需要更新的数据
        :return:
        '''
        data = ','.join(['{}="{}"'.format(key, val) for key, val in kwargs.items()])
        sql = 'update {} set {} where {}'.format(table_name, data, where)
        return self.excute(sql)

    def select(self, table_name, where='', data='', limit=None, use_pandas=0):
        '''

        :param table_name: 表名
        :param where: 条件 id>1
        :param data: 元组 ，字段名
        :param limit: 元组 （1，10）
        :return: 列表
        '''
        if data:
            assert isinstance(data, tuple)
        else:
            data = ('*',)

        if not isinstance(limit, int):
            if limit and len(limit) == 2:
                limit = 'limit {},{}'.format(limit[0], limit[1])
            elif limit and len(limit) == 1:
                limit = 'limit {}'.format(limit[0])
            elif isinstance(limit, int):
                limit = limit
            else:
                limit = ''
        else:
            limit = 'limit %d' % limit

        where = 'where %s' % where if where else ''

        sql = 'select {} from {} {} {}'.format(",".join(data), table_name, where, limit)
        return self.excute(sql, use_pandas=use_pandas)

    # @check_instance
    def excute(self, sql, data=None, use_pandas=0):
        '''

        :param sql:
        :param data:
        :param use_pandas: 是否使用dataframe
        :return:
        '''
        _logger.info(sql)
        _logger.info(data)

        sqltype = sql.split(' ', 1)[0].lower()

        if sqltype == 'insert':
            # 增
            assert data
            try:
                self._cursor.execute(sql, data)
                lastid = self._cursor.lastrowid
                self._conn.commit()
                return lastid
            except Exception as e:
                self._conn.rollback()
                raise e

        elif sqltype in ['delete', 'update']:
            # 删 或 改 返回影响行数
            try:
                self._cursor.execute(sql, data)
                rowcount = self._cursor.rowcount
                self._conn.commit()
                return rowcount
            except Exception as e:
                self._conn.rollback()
                raise e

        else:
            if use_pandas == 0:
                try:
                    self._cursor.execute(sql)
                    self._conn.commit()
                    return self._cursor.fetchall()
                except Exception as e:
                    self._conn.rollback()
                    raise e
            else:
                return pd.read_sql(sql, self._conn)

    @property
    def get_instance(self):
        '''
        获取连接句柄
        :return:
        '''
        return self._conn, self._cursor
