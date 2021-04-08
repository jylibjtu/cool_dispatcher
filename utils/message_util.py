#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:20-1-16 下午6:12
import json
import requests

def get_queue(target, server):
    return "{}_feed_queue_{}".format(target, server.replace(":", "__").replace(".", "_"))

def get_server(target, queue):
    return queue.replace("{}_feed_queue_".format(target), "").replace("__", ":").replace("_", ".")

def get_message_type_id_time(message, timestamp):
    mess_list = json.loads(message)
    return [mess_list[0], mess_list[1], timestamp]

def build_message(message_type, id):
    return json.dumps([message_type, id])

def do_message_consuming(url, message_list):
    ret = requests.post(url, data=json.dumps(message_list)).json()
    return ret, ret.get("success", False), ret.get("code",None), ret.get("message","")

