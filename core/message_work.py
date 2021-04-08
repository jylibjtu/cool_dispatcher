#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:20-1-8 下午4:56
from config.config_and_register import redis_conn, get_logger, mq_subject_dict, app_code as group, watcher_dict
from utils.common_util import list_slice, do_mail_post, list_join_by_comma, do_message_consuming
from utils.message_util import get_queue, get_server, get_message_type_id_time, build_message, qmq_util
from traceback import format_exc as error_info
from threading import Thread, Lock
from uhashring import HashRing
from core.abstract_handler import AbstractHandler
from core.abstract_mq_dealer import MQDealer
import json
import time
import re
import requests
import socket
logger = get_logger("root")
dispatch_logger = get_logger("dispatch")


# 抽象的消息处理模块
class MessageWorker(Thread):
    def __init__(self, handler, server, living_list):
        super(MessageWorker, self).__init__(target=self.run, args=(), daemon=True)

        # 具体业务handler对象
        self.handler = handler

        # 指定服务点
        self.server = server

        # 接到最新通知的zk存活服务集合
        self.living_list = living_list

        # 使用中的服务存活状态
        self.status_dict = {k : True for k in living_list}

        # 应用中的hash环，注意每个worker应用的环并不是统一的，各点可能有自己的更新进度
        self.ring = HashRing(living_list)

        # 控制hash环状态锁
        self.update_lock = Lock()


    def get_server(self, message):
        id = self.handler.get_message_id(message)
        return self.ring.get_node(id)


    # 启用新服务状态
    def nodes_status_update(self):
        if len(self.status_dict.keys() - self.living_list) != 0 or len(self.living_list - self.status_dict.keys()) != 0:
            new_dict = {k: True for k in self.living_list}
            new_list = list(set(self.living_list) - set(self.status_dict.keys()))
            gone_list = list(set(self.status_dict.keys()) - set(self.living_list))

            with self.update_lock:
                self.status_dict = new_dict
                for gone in gone_list:
                    self.ring.remove_node(gone)
                for new in new_list:
                    self.ring.add_node(new)
                dispatch_logger.info("[{}] [{}] [{}] enable new change.".format(self.handler.name, self.server, self.__class__))
        return True

    # 接受广播函数，更新服务状态
    def update_living_list(self, living_list):
        with self.update_lock:
            self.living_list = living_list
        dispatch_logger.info("[{}] [{}] [{}] got nodes change.".format(self.handler.name, self.server, self.__class__))


    def get_server_is_living(self, server):
        return self.status_dict.get(server, False)


    def run(self):
        pass


# 消息hash分散的分发者
class MessageProducer(MessageWorker):
    def __init__(self, handler, server, living_list):
        super(MessageProducer, self).__init__(handler, server, living_list)


    def run(self):
        while True:
            self.nodes_status_update()
            try:
                qmq_message_ids, id_type_timestamp_list = self.handler.get_standardized_message_from_upstream(qmq_util)
                if qmq_message_ids:
                    for (id, message_type, m_timestamp) in id_type_timestamp_list:
                        server = self.get_server(id)
                        while not server:
                            time.sleep(5)
                            dispatch_logger.info("Producer [{}] no living node, dispatcher hang up 5s.".format(self.handler.name))
                            self.nodes_status_update()
                            server = self.get_server(id)

                        # 子队列分发
                        self.handler.mq_dealer.publish_message(get_queue(self.handler.name, server), build_message(message_type, id), m_timestamp)
                        self.handler.watcher.counter("qmq_dispatch_count")

                    #上游业务消息ack
                    AbstractHandler.ack_upstream_messages(qmq_util, qmq_message_ids)
                    dispatch_logger.info("Producer [{}] dispatch [{}] message id [{}] to [{}] nodes.".format(self.handler.name, len(qmq_message_ids), qmq_message_ids, len(self.ring.nodes)))

            except:
                logger.error("dispatch feeder : {} error, info : {}".format(self.handler.name, error_info()))


# 针对已分散消息的服务调用（消费者）
class MessageConsumer(MessageWorker):
    def __init__(self, target, server, living_list):
        super(MessageConsumer, self).__init__(target, server, living_list)
        self.url = "http://{}/deal_{}_message".format(self.server, self.handler.name)

    def run(self):
        """
        通用的产品信息变更通知，单进程处理单队列模式
        :param target:  业务线标识
        :param index: 监听的目标队列数/进程模块编号
        :return:
        """
        queue_batch_num = 800
        watcher = self.handler.watcher
        wait_seconds = self.handler.wait_second
        while True:
            self.nodes_status_update()
            if self.get_server_is_living(self.server):
                try:
                    # 从子队列获取消息
                    message_timestamp_pairs = self.handler.mq_dealer.get_messages_batch(get_queue(self.handler.name, self.server), self.handler.deal_batch_num)

                    # 拼接服务请求参数
                    req_param_list = [get_message_type_id_time(m,timestamp) for m,timestamp in message_timestamp_pairs]

                    # 返回内容
                    ret, success, ret_code, ret_message = do_message_consuming(self.url, req_param_list)

                    # 成功,ack消息
                    if success:
                        self.handler.mq_dealer.ack_messages_batch([m for (m,timestamp) in message_timestamp_pairs])
                        dispatch_logger.info("Consumer send to [{}] [{}] {} messges.".format(self.handler.name, self.server, len(req_param_list)))
                    # 失败,日志记录,等待重新消费
                    else:
                        dispatch_logger.error("Consumer send to [{}] [{}] {} messges failed, [{}], aborted {} , waiting 5s for retry".format(self.handler.name, self.server, len(req_param_list), list_join_by_comma(req_param_list) , json.dumps(ret)))

                except Exception as e:
                    # 异常捕获多见于服务点崩溃而Consumer未读取zk变化而消亡时,此时持续日志记录并等待自我消亡
                    watcher.counter("{}_deal_error".format(self.handler.name))
                    dispatch_logger.warn("Consumer [{}] [{}] deal error :{}".format(self.handler.name, self.server, e))
                    time.sleep(0.5)
            else:
                dispatch_logger.info("[{}] [{}] consumer dispatch stop.".format(self.handler.name, self.server))
                break


# 针对死亡模块的消息重分散（转移者）
class MessageTransferer(MessageWorker):
    def __init__(self, target, server, living_list, general_producer, consumer_dict):
        super(MessageTransferer, self).__init__(target, server, living_list)
        self.general_producer = general_producer
        self.consumer_dict = consumer_dict

    def has_other_dealer(self):
        """
        判断转移者对应的服务,是否生产者仍在分发,或者消费者仍在读取
        :return:
        """
        return ((self.general_producer.is_alive() and self.general_producer.get_server_is_living(self.server))
                     or
                    self.consumer_dict[self.server].is_alive())

    def run(self):
        """
        消息转移/重分发
        :return:
        """
        counter = 0
        finish = False
        while True:
            try:
                self.nodes_status_update()


                if self.get_server_is_living(self.server):
                    dispatch_logger.info("Transfer [{}] [{}] now service alive, stop transfer.".format(self.handler.name, self.server))
                    break

                # 必须等到服务消亡被生产者/消费者所应用,才能开始进行转移
                if self.has_other_dealer():
                    dispatch_logger.info("Transfer [{}] [{}] with alive producer or consumer, sleep 10s for wait.".format(self.handler.name, self.server))
                    time.sleep(10)
                    continue

                deliver_dict = {s: [] for s in self.status_dict if self.status_dict[s]}

                messages_timestamp_1000_pairs = self.handler.mq_dealer.get_messages_batch(get_queue(self.handler.name, self.server), 1000)

                # 取待转移队列1000条,并按照最新hash环进行重分配
                for mess, timestamp in messages_timestamp_1000_pairs:
                    message_type, id, qmq_time = get_message_type_id_time(mess, timestamp)
                    new_server = self.get_server(id)

                    # 环为空,所有点都挂掉了,也就不用重新分发了
                    if not new_server:
                        dispatch_logger.info("Transfer [{}] [{}] no living node, shut.".format(self.handler.name, self.server))
                        finish = True
                        break
                    else:
                        deliver_dict[new_server].append((message, timestamp))

                if finish:
                    break


                # 根据重分配结果进行分发
                for server, message_list in deliver_dict.items():
                    for (message, score) in message_list:
                        self.handler.mq_dealer.publish_message(get_queue(self.handler.name, server), message, score)
                        self.handler.mq_dealer.ack_message(get_queue(self.handler.name, server), message)

                else:
                    dispatch_logger.info("Transfer [{}] [{}] queue empty, finished.".format(self.handler.name, self.server))
                    break

            except Exception as e:
                dispatch_logger.error("Transfer [{}] [{}] error, {}".format(self.handler.name, self.server, e))
