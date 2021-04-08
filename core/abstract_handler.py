#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-3-29 下午2:53
#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:19-8-9 下午7:34
import math
import requests
from utils.common_util import list_join_by_comma, list_slice, get_logger
from config.config_and_register import mq_subject_dict, app_code as group
from traceback import format_exc as error_info
from urllib.parse import quote
import time
import json

class AbstractHandler:
    def __init__(self, name, port_list, deal_batch_num, wait_second, watcher, mq_dealer):

        # 具体标识,用于标注某一个消息处理业务
        self.name = name

        # 日志记录器
        self.logger = get_logger(name)

        # 该业务能够使用的机器端口列表
        self.port_list = port_list

        # 该业务单次处理的消息数量
        self.deal_batch_num = deal_batch_num

        # 该业务用于时延削峰的最大时长
        self.wait_second = wait_second

        # 该业务用于监控打点的组件
        self.watcher = watcher

        # 该业务使用的内部子队列组件(调度模块内部,而非业务上游使用的队列)
        self.mq_dealer = mq_dealer

    # 对批量消息的处理模块
    def consume_messages(self, messages):
        pass

    # 从消息体中获取用于判断同一消息的标识,用于hash分发
    def get_message_id(self, message):
        pass

    # 从具体的业务上游获取业务消息的一个实现,可根据具体场景进行更改
    def get_standardized_message_from_upstream(self, qmq_util):
        qmq_message_ids, id_type_timestamp_list = [], []

        ret = qmq_util.get_message(mq_subject_dict[self.name])
        results = ret.get('data', []) if int(ret['state']) == 0 else []
        for result in results:
            qmq_time_long = result['attrs'].get('qmq_createTIme', int(1000 * time.time()))

            qmq_data = json.loads(result['attrs']['data'])
            message_type = qmq_data.get("type", "")
            ids = qmq_data.get("ids", "").split(",")

            qmq_message_ids.append(result['messageId'])
            for id in ids:
                id_type_timestamp_list.append((id, message_type, qmq_time_long))

        return (qmq_message_ids, id_type_timestamp_list)

    # 在对上游消息作分发后的ack操作
    def ack_upstream_messages(self, qmq_util, qmq_message_ids):
        qmq_util.ack_many(qmq_message_ids)
