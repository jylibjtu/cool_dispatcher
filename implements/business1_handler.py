#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-4-8 下午8:42
from core.abstract_handler import AbstractHandler
import json

class Business1Handler(AbstractHandler):
    def __init__(self, name, port_list, deal_batch_num, wait_second, watcher, mq_dealer):
        super(Business1Handler, self).__init__(name, port_list, deal_batch_num, wait_second, watcher, mq_dealer)

    def deal_messages(self, messages):
        for mess in messages:
            print(mess)

    def get_message_id(self, message):
        return hash(message.get("businessId", json.dumps(message)))