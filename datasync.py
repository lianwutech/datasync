#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
lianwuyun与业务平台间的数据同步

系统实时监控mqtt平台，按照指定格式，将数据写到sql server中。
当数据来时才打开sqlserver

"""

from __future__ import print_function

import time
import json
import threading
import logging
import paho.mqtt.client as mqtt

import setting
from libs.datasync import *
from libs.msodbc import ODBC_MS

# 全局变量
config_file_name = "datasync.cfg"

# 初始化日志
logger = logging.getLogger('datasync')

# 配置信息
config_info = load_config(config_file_name)

# 数据库对象
sql_server = ODBC_MS(driver="{SQL SERVER}",
                     server=config_info["sqlserver"]["host"],
                     database=config_info["sqlserver"]["database"],
                     uid=config_info["sqlserver"]["uid"],
                     pwd=config_info["sqlserver"]["pwd"])

# 停止标记
run_flag = False

def stop():
    """
    插件停止
    :return:
    """
    run_flag = False

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
    logger.debug("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(config_info["mqtt"]["topic"])


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.debug("收到数据消息" + msg.topic + " " + str(msg.payload))
    # csv解码
    if msg.topic == "datapoint":
        data_point = json.loads(msg.payload)
        timestamp = data_point["timestamp"]
        component_id = data_point["component_id"]
        data = data_point["data"]
        sql = """
            insert into ebs_extend..mps_compcustomer(cc_tagi,cc_ctid,cc_createdate, cc_custusercode, cc_ext01)
            Values(102,33, %s, %d, %r)
            """ % (timestamp, component_id, data)
        result = sql_server.exec_one_sql(sql)
        if not result:
            logger.error("insert fail.")
            save_sql(sql, time.strftime('%Y-%m-%d %H:%M:%S'))

# mqtt消息处理函数
def process_mqtt_message():
    # 监控mqtt队列
    try:
        client = mqtt.Client(client_id=config_info["mqtt"]["client_id"] + "_datasync")
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(host=config_info["mqtt"]["host"], port=config_info["mqtt"]["port"], keepalive=60)
        client.loop_forever()
    except Exception, e:
        logger.error("mqtt客户端启动失败，错误内容：%r" % e)
        sys.exit()

# sql重试函数
def process_retry_sql():
    while True:
        be_success = False
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        sql_records = load_sql(timestamp)
        try:
            for record in sql_records:
                result = sql_server.exec_one_sql(record)
                if not result:
                    # 如果一条不成功，则中止操作。
                    be_success = False
                    break
                else:
                    be_success = True

            if be_success:
                remove_sql(timestamp)

        except Exception, e:
            logger.error("process_retry_sql faile, exception:%r" % e)

        # 休眠1s
        time.sleep(5)

def run():
    # 初始化标记
    run_flag = True

    # 测试sql server
    if not sql_server.test_db():
        logger.error("database test fail.")
        sys.exit()

    # 检查备份数据库
    if not check_sqlite_table():
        logger.error("check_sqlite_table fail.")
        sys.exit()

    mqtt_thread = None
    retry_thread = None

    while True:
        if mqtt_thread is None or not mqtt_thread.isAlive():
            logger.debug("启动MQTT消息处理进程")
            mqtt_thread = threading.Thread(target=process_mqtt_message)
            mqtt_thread.start()

        if retry_thread is None or not retry_thread.isAlive():
            logger.debug("启动sql重试进程")
            retry_thread = threading.Thread(target=process_retry_sql)
            retry_thread.start()

        if run_flag == False:
            mqtt_thread.join(1)
            retry_thread.join(1)
            break

        time.sleep(5)


if __name__ == '__main__':
    run()

