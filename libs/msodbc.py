#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import pyodbc

# 初始化日志
logger = logging.getLogger('datasync')

class ODBC_MS:
    """
    对pyodbc库的操作进行简单封装
    pyodbc库的下载地址:http://code.google.com/p/pyodbc/downloads/list
    使用该库时，需要在Sql Server Configuration Manager里面将TCP/IP协议开启
    此类完成对数据库DB的连接/查询/执行操作
    正确的连接方式如下:
    cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=ZHANGHUAMIN\MSSQLSERVER_ZHM;DATABASE=AdventureWorks2008;UID=sa;PWD=wa1234')
    cnxn = pyodbc.connect(DRIVER='{SQL SERVER}',SERVER=r'ZHANGHUAMIN\MSSQLSERVER_ZHM',DATABASE='AdventureWorks2008',UID='sa',PWD='wa1234',charset="utf-8")
    """

    def __init__(self, driver, server, database, uid, pwd):
        self.DRIVER = driver
        self.SERVER = server
        self.DATABASE = database
        self.UID = uid
        self.PWD = pwd
        self.conn = None

    def get_connect(self):
        """
        Connect to the DB
        :return:
        """

        if not self.DATABASE:
            raise(NameError, "no setting db info")

        self.conn = pyodbc.connect(DRIVER=self.DRIVER,
                                   SERVER=self.SERVER,
                                   DATABASE=self.DATABASE,
                                   UID=self.UID,
                                   PWD=self.PWD,
                                   charset="UTF-8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError, "connected failed!")
        else:
            return cur

    def exec_query(self, sql):
        """
        Perform one Sql statement
        :param sql:
        :return:
        """
        # 建立链接并创建数据库操作指针
        cur = self.get_connect()
        # 通过指针来执行sql指令
        cur.execute(sql)
        # 通过指针来获取sql指令响应数据
        ret = cur.fetchall()
        # 游标指标关闭
        cur.close()
        # 关闭数据库连接
        self.conn.close()
        return ret

    def exec_one_sql(self, sql):
        """
        Person one Sql statement like write data, or create table, database and so on
        :param sql:
        :return:
        """
        cur = self.get_connect()
        cur.execute(sql)
        # 连接句柄来提交
        result = self.conn.commit()
        cur.close()
        self.conn.close()
        return result

    def test_db(self):
        try:
            self.get_connect()
            self.conn.close()
            return True
        except Exception, e:
            logger.debug("test_db faile: %r" % e)
            return False

