#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:junyili
# datetime:19-12-16 下午3:37

import click
import json
import time
import socket
import sys

work_dir = '/home/q/cool_dispatcher/'
if work_dir not in sys.path:
    sys.path.insert(0, work_dir)

from flask import app, Flask, request
from config.config_and_register import get_logger, business_register_table, watcher_dict, acl_ips as ACL, port_dict, zk, \
    zk_app_name, zk_module_name_web, handler_dict
from traceback import format_exc as error_info

app = Flask(__name__)
app.config["app_name"] = 'your_project'
logger = get_logger('web')


@click.group()
def cli():
    pass


def param_check(target, port, env):
    if target not in business_register_table:
        click.echo("wrong target")
        return False
    elif int(port) not in port_dict[target]:
        click.echo("wrong port")
        return False
    elif env not in ['prod', 'beta']:
        click.echo("wrong environment")
        return False
    return True


@cli.command()
@click.option('-t', '--target', prompt='Target model name from [{}]'.format(business_register_table),
              help='Target name')
@click.option('-p', '--port', prompt='The service port', help='port')
@click.option('-e', '--env', prompt='prod or beta', help='environment')
def start(target, port, env):
    if not param_check(target, port, env):
        return

    handler = handler_dict[target]
    watcher = handler.watcher

    @app.route('/deal_{}_message'.format(target), methods=['POST'])
    def deal():
        try:
            start = time.time()
            request_ip = request.remote_addr
            if request_ip in ACL:
                message_list = json.loads(request.get_data())
                mess_len = len(message_list)
                if message_list:
                    watcher.timer("{}_message_once_ready_count".format(target), mess_len)
                    handler.deal_messages(message_list, watcher, start)
                    logger.info(
                        "module [{}] port [{}] deal ok , message [{}]".format(target, port, json.dumps(message_list)))
                    return json.dumps({"success": True, "code": 200, "message": ""})
                else:
                    logger.info(
                        "module [{}] port [{}] service failed, empty request".format(target, port))
                    return json.dumps({"success": False, "code": 400, "message": "empty request"})
            else:
                logger.info("module [{}] port [{}] service failed, ip {} not allowd".format(target, port, request_ip))
                return json.dumps({"success": False, "code": 403, "message": "bad request, ip not allowed"})
        except Exception:
            logger.error("module [{}] port [{}] service error, info {}".format(target, port, error_info()))
            return json.dumps({"success": False, "code": 500, "message": error_info()})

    @app.route('/healthcheck', methods=['GET', 'POST'])
    def check():
        logger.info("healthcheck pass")
        return 'ok'

    @app.before_first_request
    def init():
        zk.start()
        zk.create('/{}/{}/{}/'.format(zk_app_name, zk_module_name_web, target),
                  '{}:{}'.format(socket.gethostbyname(socket.gethostname()), port).encode(),
                  ephemeral=True, sequence=True, makepath=True)
        logger.info("module [{}] port [{}] register on zookeeper".format(target, port))

    try:
        app.run(port=int(port), debug=False, host='0.0.0.0')
    except:
        logger.error("[{}] [{}] start error".format(target, port), error_info)
        try:
            zk.stop()
        except:
            logger.error(error_info)
            pass


if __name__ == '__main__':
    cli()
