#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-3-31 下午5:15
#!/usr/bin/env python
# -*- encoding: utf8 -*-


import sys
work_dir = '/home/q/cool_dispatcher'
if work_dir not in sys.path:
    sys.path.insert(0, work_dir)

import redis
from kazoo.client import KazooClient
from utils.watcher_util import StatsdWatcher
from utils.common_util import get_logger
from implements.business1_handler import Business1Handler
from implements.business2_handler import Business2Handler
from implements.redis_zset_mq_dealer import RedisZsetMQDealer


logger = get_logger("root")

PROD = False


app_code = "cool_dispatcher"
env = 'prod' if PROD else 'beta'
watcher = StatsdWatcher('cool_dispatcher', env)


###################### register table ######################

zk_hosts = "your zk cluster hosts"
zk_app_name = "cool_dispatcher"
zk_module_name_web = "web"
zk_module_name_dispatch = "dispatcher"

zk = KazooClient(hosts=zk_hosts)


redis_host = "your redis host"
redis_port = "6379"
redis_message_time_db = "1"
redis_pwd = "your redis password"

redis_conn = redis.Redis(host=redis_host, port=int(redis_port),
                             db=int(redis_message_time_db),
                             password=redis_pwd)

mq_dealer_impl = RedisZsetMQDealer(redis_conn)

acl_ips = ["127.0.0.1", "other ip1", "other ip2"]

busi1 = "business1"
busi2 = "business2"

business_register_table = [busi1, busi2]

mq_subject_dict = {
    busi1 : "test.business1_craw_message",
    busi2 : "test.business2_craw_message",
}

# 具体业务 -> 一次性处理消息条数
batch_num_dict = {
    busi1 : 50,
    busi2 : 20,
}

# 具体业务 -> 子队列去重等待时长
wait_dict = {
    busi1 : 180,
    busi2 : 20,
}

# 具体业务 -> 在每台机器的所有启用端口
port_dict = {busi1: [21340, 21341, 21342],
             busi2: [21440, 21441, 21442],}

# 具体业务 -> 监控工具
watcher_dict = {
    busi1 : StatsdWatcher('cool_dispatcher.business1', env),
    busi2 : StatsdWatcher('cool_dispatcher.business2', env),
}

# todo
# 具体业务 -> 业务处理handler对象
handler_dict = {
    busi1 : Business1Handler(name=busi1, port_list=port_dict[busi1], deal_batch_num=batch_num_dict[busi1], wait_second=wait_dict[busi1], watcher=wait_dict[busi1], mq_dealer=mq_dealer_impl),
    busi2 : Business2Handler(name=busi2, port_list=port_dict[busi2], deal_batch_num=batch_num_dict[busi2], wait_second=wait_dict[busi2], watcher=wait_dict[busi2], mq_dealer=mq_dealer_impl),
}

###################### register table end ######################






