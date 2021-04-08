#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:21-3-30 上午11:57

import sys
import subprocess
import socket
import logging.config
import os
from functools import wraps
from traceback import format_exc as error_info

base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

logging.config.fileConfig(os.path.join(base_dir, "config", "logging.conf"))

def get_logger(name):
    return logging.getLogger(name)

def get_status_output(*args, **kwargs):
    return subprocess.getstatusoutput(*args, **kwargs)

def do_mail_post(to, content):
    try:
        """do mail post"""
        pass
    except:
        pass

def list_slice(seq, step=2000):
    for start in range(0, len(seq), step):
        yield seq[start:start + step]

def health_check(file_dir_name):
    if os.path.exists(file_dir_name):
        pass
    else:
        sys.exit()

def error_notice(func):
    @wraps(func)
    def wrapper(*arg, **kwargs):
        ret = None
        try:
            ret = func(*arg, **kwargs)
        except:
            do_mail_post('junyi.li', 'host {} , info {}'.format(socket.gethostname(), error_info()), '', '')
        finally:
            return ret
    return wrapper

def list_join_by_comma(str_lst):
    ret = ""
    if type(str_lst) == type([]):
        ret = ",".join([str(l) for l in str_lst])
    return ret