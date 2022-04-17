#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:20-1-16 下午6:12
import json

def get_queue(target, server):
    return "{}_feed_queue_{}".format(target, server.replace(":", "__").replace(".", "_"))

def get_server(target, queue):
    return queue.replace("{}_feed_queue_".format(target), "").replace("__", ":").replace("_", ".")

def get_message_type_id_time(message, timestamp):
    mess_list = json.loads(message)
    return [mess_list[0], mess_list[1], timestamp]

def build_message(message_type, id):
    return json.dumps([message_type, id])

class QMQ_util:
    def __init__(self):
        pass

    def get_message(self, subject):
        pass

    def ack_message(self, subject, message):
        pass

mq_dealer = QMQ_util()


