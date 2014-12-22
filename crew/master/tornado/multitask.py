#!/usr/bin/env python
# encoding: utf-8
from shortuuid import uuid
from tornado.concurrent import Future


class MultitaskCall(object):
    def __init__(self, client):
        self.__calls = list()
        self.__results = {}
        self.__result_future = Future()
        self.client = client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def call(self, channel, **kwargs):
        assert not 'callback' in kwargs
        assert not 'set_cid' in kwargs

        cid = "{0}.{1}".format(channel, uuid())
        def set_result(result, headers):
            self.__result_cb(result, cid)

        self.client.call(channel, callback=set_result, set_cid=cid, **kwargs)
        self.__calls.append(cid)

    def result(self):
        self.__queue = list(self.__calls)
        return self.__result_future

    def __result_cb(self, result, cid):
        self.__results[cid] = result
        self.__queue.remove(cid)

        if not self.__queue:
            self.__result_future.set_result([self.__results[i] for i in self.__calls])
            self.__clean()

    def __clean(self):
        self.__calls = []
        self.__queue = []
        self.__results = {}
        self.__result_future = Future()
