#!/usr/bin/env python
# encoding: utf-8
import logging
from time import time
from crew.master.threaded_client import Client

logging.basicConfig(
    format=u'[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-6s %(message)s',
    level=logging.DEBUG
)

if __name__ == '__main__':
    c = Client()
    tasks = list()
    for _ in range(30):
        tasks.append(c.call('sleep'))

    start_time = time()
    for task in tasks:
        task.wait(timeout=50)

    print ("Execution time: {}".format(time() - start_time))
