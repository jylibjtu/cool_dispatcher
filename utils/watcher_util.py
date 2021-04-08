#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:junyili
# datetime:19-4-8 下午7:28
import statsd
from utils.common_util import error_notice


class StatsdWatcher:
    c = statsd.StatsClient('myhost.cooldispatcher.com', 8125)
    prefix = 'your_company.test'
    def __init__(self, app_code, env='beta'):
        self.app_code = app_code
        self.env = env


    @error_notice
    def timer(self, target, value):
        self.c.timing('.'.join([self.prefix, self.app_code, self.env, target]), value)

    @error_notice
    def counter(self, target):
        self.c.gauge('.'.join([self.prefix, self.app_code, self.env, target]), 1, delta=True)

    @error_notice
    def counter_many(self, target, n):
        self.c.gauge('.'.join([self.prefix, self.app_code, self.env, target]), n, delta=True)


    @error_notice
    def gauge(self, target, value):
        self.c.gauge('.'.join([self.prefix, self.app_code, self.env, target]), value)


if __name__ == "__main__":
    # test
    watcher = StatsdWatcher('cool_dispatcher', 'beta')
    watcher.timer('deal_business1_all_cost', 100)
