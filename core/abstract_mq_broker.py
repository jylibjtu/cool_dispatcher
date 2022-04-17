#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-4-8 下午9:22

class MQDealer:
    def __init__(self):
        pass

    def get_message(self, queue_name):
        pass

    def ack_message(self, queue_name, message):
        pass

    def get_messages_batch(self, queue_name, count):
        pass

    def ack_messages_batch(self, queue_name, messages):
        pass

    def publish_message(self, queue_name, message, timestamp):
        pass

    def publish_messages_batch(self, queue_message_timestmap_pairs):
        pass