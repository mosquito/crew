#!/usr/bin/env python
# encoding: utf-8
import logging
from crew.master.threaded_client import Client

logging.basicConfig(
    format=u'[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-6s %(message)s',
    level=logging.DEBUG
)

if __name__ == '__main__':
    c = Client()
    print c.call('stat').wait(timeout=1)