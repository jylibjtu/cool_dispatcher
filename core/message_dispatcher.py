#!/usr/bin/env python    parser.add_argument(u'-c', u'--method', required=True, choices=[u'start', u'halt', u'restart'])
#-*- coding:utf-8 -*-
# author:junyili
# datetime:19-8-23 下午8:06
import sys
work_dir = '/home/q/cool_dispatcher/'
if work_dir not in sys.path:
    sys.path.insert(0, work_dir)
from config.config_and_register import business_register_table, get_logger, zk, zk_app_name, zk_module_name_web, zk_module_name_dispatch
from kazoo.client import NodeExistsError
from functools import reduce
from core.message_work import MessageProducer, MessageConsumer, MessageTransferer
from traceback import format_exc as error_info
import time
import socket

logger = get_logger("root")
dispatch_logger = get_logger("dispatch")

def bind(target):
    """
    绑定zk，监视调度模块，竞争注册，实现互为主备
    :param target:指定业务名称
    :return:
    """

    while True:
        try:
            zk.start()
            zk.ensure_path("/{}/".format(zk_app_name))
            zk.ensure_path("/{}/{}/".format(zk_app_name, zk_module_name_dispatch))
            zk.create('/{}/{}/{}/'.format(zk_app_name, zk_module_name_dispatch, target),'{}'.format(socket.gethostbyname(socket.gethostname())).encode(),
                      ephemeral=True, sequence=False, makepath=True)
            dispatch_logger.info("Dispatcher register success, start work.")
            start(target)
        except NodeExistsError:
            dispatch_logger.info("Dispatcher [{}] has another living, this is back, sleep for 10s.".format(target))
            time.sleep(10)
            continue
        except:
            dispatch_logger.info("Dispatcher error, {}".format(error_info()))
            try:
                zk.stop()
            except:
                pass

            break

def start(target):
    """
    绑定zk，增加zk监视，启动生产者
    :param target:指定业务名称
    :return:
    """
    global using_list, producer, consumer_thread_dict

    zk.ensure_path("/{}/".format(zk_app_name))
    zk.ensure_path("/{}/{}/".format(zk_app_name, zk_module_name_web))
    zk.ensure_path("/{}/{}/{}/".format(zk_app_name, zk_module_name_web, target))

    @zk.ChildrenWatch('/{}/{}/{}'.format(zk_app_name, zk_module_name_web, target))
    def my_func(data):
        global real_list
        real_list = [zk.get('/{}/{}/{}/{}'.format(zk_app_name, zk_module_name_web, target, id))[0].decode() for id in data] if data else []

    producer = MessageProducer(target=target, server=None, living_list=using_list)
    producer.start()

    while True:
        dispatch(target)

def broadcast():
    """
    广播最新状态到所有的工作者，包括唯一生产者，各服务的指定消费者，以及死亡服务正在处理的转移者
    :return:
    """
    global producer, consumer_thread_dict, transferer_thread_dict, using_list
    producer.update_living_list(using_list.copy())
    for server, consumer in consumer_thread_dict.items():
        consumer.update_living_list(using_list.copy())
    for server, transferer in transferer_thread_dict.items():
        transferer.update_living_list(using_list.copy())


def dispatch(target):
    """
    周期性的调度方法，用来适配最新的服务存活状态
    :param target:
    :return:
    """
    global using_list, real_list, producer, consumer_thread_dict, transferer_thread_dict
    gone_list = set(using_list) - set(real_list)
    new_list = set(real_list) - set(using_list)
    if gone_list or new_list:
        dispatch_logger.info("Dispatch [{}] receive nodes change, add [{}], gone [{}] start dispatch.".format(target, new_list, gone_list))
        using_list = real_list
        broadcast()

        # 为新节点增加消费者
        for new in new_list:
            consumer_thread_dict[new] = MessageConsumer(target=target, server=new, living_list=using_list.copy())
            consumer_thread_dict[new].start()
            if new in transferer_thread_dict and transferer_thread_dict[new].is_alive():
                transferer_thread_dict[new].update_living_list(using_list)

        # 为挂掉的点增加转移者
        for gone in gone_list:
            if gone in transferer_thread_dict and transferer_thread_dict[gone].is_alive():
                transferer_thread_dict[gone].update_living_list(using_list)
            else:
                transferer_thread_dict[gone] = MessageTransferer(target=target, server=gone, living_list=using_list.copy(), general_producer=producer, consumer_dict=consumer_thread_dict)
                transferer_thread_dict[gone].start()

        # 有转移者的场景，需要等待所有转移者停止转移，调度结束
        if gone_list:
            counter = 0
            while True:
                if reduce(lambda a,b: a and b, [not transferer_thread_dict[gone].is_alive() for gone in gone_list]):
                    dispatch_logger.info("Transferer [{}] gone [{}] finished".format(target, gone_list))
                    break
                else:
                    counter += 1
                    time.sleep(10)
                if counter >= 5:
                    dispatch_logger.warn("Transferer [{}] has wait {} * 10s.".format(target, counter))

    else:
        dispatch_logger.info("Dispatch [{}] no nodes change, sleep 10s.".format(target))
        time.sleep(10)


if __name__ == "__main__":
    global using_list, real_list, producer, consumer_thread_dict, transferer_thread_dict
    global living_dispatchers
    living_dispatchers = []
    using_list = []
    real_list = []
    consumer_thread_dict = {}
    transferer_thread_dict = {}
    from argparse import ArgumentParser
    parser = ArgumentParser(description=u'hello.')
    parser.add_argument(u'-t', u'--target', required=True, choices=business_register_table)
    parser.add_argument(u'-e', u'--environment', required=True, choices=[u'prod', u'beta'])
    args = parser.parse_args()
    if args.target in business_register_table:
        bind(args.target)
