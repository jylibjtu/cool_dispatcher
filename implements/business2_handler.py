#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-4-8 下午8:42
import json
from core.abstract_business_handler import AbstractHandler

class Business2Handler(AbstractHandler):
    def __init__(self, name, port_list, deal_batch_num, wait_second, watcher, mq_dealer):
        super(Business2Handler, self).__init__(name, port_list, deal_batch_num, wait_second, watcher, mq_dealer)

    def deal_messages(self, messages):
        print(json.dumps(messages))

    def get_message_id(self, message):
        return hash(json.dumps(message))