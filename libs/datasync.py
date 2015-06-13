#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import json
import logging
import sqlite3

from libs.utils import convert

# 初始化日志
logger = logging.getLogger('datasync')

# 获取配置项
def load_config(config_file_name):
    if os.path.exists(config_file_name):
        config_file = open(config_file_name, "r+")
        content = config_file.read()
        config_file.close()
        try:
            config_info = convert(json.loads(content.encode("utf-8")))
            logger.debug("load config info success，%s" % content)
            return config_info
        except Exception, e:
            logger.error("load config info fail，%r" % e)
            return None
    else:
        logger.error("config file is not exist. Please check!")
        return None

# 检查表是否存在
def check_sqlite_table():
    con = sqlite3.connect('sqldb.db3')
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) as count FROM sqlite_master where type='table' and name='sql_bak'")
    res = cur.fetchone()
    if res["count"] == 0:
        # 如果表不存在则创建表
        cur.execute("""
        CREATE TABLE sql_bak (id integer primary key autoincrement,
                                sql VARCHAR(256),
                                timestamp VARCHAR(20))
        """)
        con.commit()
    con.close()
    return True


# 异常数据保存到sqlite3
def save_sql(sql, timestamp):
    con = sqlite3.connect('sqldb.db3')
    cur = con.cursor()
    cur.execute("INSERT INTO sql_bak (sql, timestamp) VALUES(NULL, '%s', '%s')" % (sql, timestamp))
    con.commit()
    con.close()

# 加载异常数据
def load_sql(timestamp):
    con = sqlite3.connect('sqldb.db3')
    cur = con.cursor()
    cur.execute("SELECT id, sql, process_flag FROM sql_bak WHERE timestamp <= '%s'" % timestamp)
    records = cur.fetchall()
    con.close()
    return records

# 删除过期数据
def remove_sql(timestamp):
    con = sqlite3.connect('sqldb.db3')
    cur = con.cursor()
    cur.execute("DELETE FROM sql_bak WHERE timestamp <= '%s'" % timestamp)
    con.commit()
    con.close()
