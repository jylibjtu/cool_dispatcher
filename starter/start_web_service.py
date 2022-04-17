import os
import sys
import time
import requests

work_dir = '/home/q/cool_dispatcher/'
if work_dir not in sys.path:
    sys.path.insert(0, work_dir)

from utils.common_util import get_status_output
from config.config_and_register import env, business_register_table, port_dict
from traceback import format_exc as error_info


def healthcheck(target, port_list):
    result = True
    for port in port_list:
        count = 0
        succ = False
        while True:
            try:
                x = requests.get("http://127.0.0.1:{}/healthcheck".format(port)).text
                if x == "ok":
                    sys.stdout.write("Touch {} port {} ok \n".format(target, port))
                    succ = True
                    break
                else:
                    sys.stdout.write("Touch {} port {} not ok : {} \n".format(target, port, x))
                    count += 1
            except:
                sys.stdout.write("Touch {} port {} Exception : {} \n".format(target, port, error_info()))
                count += 1
                time.sleep(1)
            if count >= 50:
                sys.stdout.write("Touch {} port {} failed, retry more than 50 times. \n".format(target, port))
                break

        result = result and succ
    return result


def start(target):
    port_list = port_dict[target]
    while True:
        status, output = get_status_output(
            'ps axu | grep business_broker.py | grep {} | \
            grep -v grep | grep -v start_web_service | wc -l'.format(target))
        if int(output) == 0:
            time.sleep(1)
            break
        else:
            sys.stdout.write(output + "\n")
            time.sleep(1)

    sys.stdout.write('start {} worker....'.format(target) + "\n")
    for port in port_list:
        command_str = u'nohup python {work_dir}bin/business_broker.py start -t {target} \
        -e {env} -p {port} &>{work_dir}logs/start_{target}_web.log &\
        '.format(work_dir=work_dir, port=port, env=env, target=target)
        sys.stdout.write(command_str + "\n")
        os.system(command_str)
        time.sleep(1)
    if healthcheck(target, port_list):
        sys.stdout.write('start {} worker Done'.format(target) + "\n")
    else:
        sys.stdout.write('start {} worker failed'.format(target) + "\n")


def halt(target):
    command_str = u"ps axu | grep business_broker.py | grep {}".format(target) + "\
     | grep -v grep | grep -v start_web_service | awk '{print $2}' | xargs kill -9"
    print(command_str)
    os.system(command_str)
    while True:
        status, output = get_status_output(
            u'ps axu | grep business_broker.py | grep {} | \
            grep -v grep | grep -v start_web_service | wc -l'.format(target))
        if int(output) == 0:
            sys.stdout.write('{} stop done'.format(target) + "\n")
            break

        else:
            sys.stdout.write(output + "\n")
            time.sleep(1)


def restart(target):
    halt(target)
    start(target)


if __name__ == u'__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description=u'hello.')
    parser.add_argument(u'-c', u'--method', required=True, choices=[u'start', u'halt', u'restart'])
    parser.add_argument(u'-t', u'--target', required=True, choices=business_register_table)
    parser.add_argument(u'-e', u'--environment', required=True, choices=[u'prod', u'beta'])
    args = parser.parse_args()
    if args.target in business_register_table:
        if args.method == u'start':
            start(args.target)
        elif args.method == u'halt':
            halt(args.target)
        elif args.method == u'restart':
            restart(args.target)
