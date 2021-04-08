#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-4-8 下午9:32

from core.abstract_mq_dealer import MQDealer

class RedisZsetMQDealer(MQDealer):
    def __init__(self, redis_conn):
        super(RedisZsetMQDealer, self).__init__()
        self.conn = redis_conn

    def get_message(self, queue_name):
        messages = self.conn.zrange(queue_name, 0, 0)
        message_timestamp_pairs = [(m.decode(), self.redis_conn.zscore(queue_name, m.decode())) for m in messages]
        return message_timestamp_pairs

    def ack_message(self, queue_name, message):
        self.conn.zrem(queue_name, message)

    def get_messages_batch(self, queue_name, count):
        messages = self.conn.zrange(queue_name, 0, count - 1)
        message_timestamp_pairs = [(m.decode(), self.redis_conn.zscore(queue_name, m.decode())) for m in messages]
        return message_timestamp_pairs

    def ack_messages_batch(self, queue_name, messages):
        for mess in messages:
            self.ack_message(queue_name, mess)

    def publish_message(self, queue_name, message, timestamp):
        self.conn.zadd(queue_name, message, timestamp)

    def publish_messages_batch(self, queue_message_timestmap_pairs):
        for queue_name, message, timestamp in queue_message_timestmap_pairs:
            self.publish_message(queue_name, message, timestamp)

