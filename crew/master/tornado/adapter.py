#!/usr/bin/env python
# encoding: utf-8
from functools import wraps, partial
from heapq import heappop, heappush
from copy import copy
import pika
from pika.adapters.tornado_connection import TornadoConnection
from tornado.concurrent import Future
import tornado.ioloop
import tornado.gen
from tornado.log import app_log as log


class ChannelException(Exception):
    pass


def queued(order=1000):
    def deco(func):
        @wraps(func)
        def wrap(self, *args, **kwargs):
            f = Future()
            def run():
                try:
                    f.set_result(func(self, *args, **kwargs))
                except Exception as e:
                    f.set_exception(e)

            if self.channel and self.channel.is_open:
                log.debug("Running %r", func)
                tornado.ioloop.IOLoop.instance().add_callback(run)
            else:
                log.debug("Queued %r", func)
                heappush(self._queue, (order, run))

            return f
        return wrap
    return deco


def memory(order=1000):
    def deco(func):
        @wraps(func)
        def wrap(self, *args, **kwargs):
            heappush(self._memory, (order, partial(func, self, *args, **kwargs)))
            return func(self, *args, **kwargs)

        return wrap
    return deco


class TornadoPikaAdapter(object):
    RECONNECT_TIMEOUT = 5
    IOLOOP = tornado.ioloop.IOLoop.instance()

    def _on_close(self, connection, *args):
        log.info('PikaClient: Try to reconnect')

        if self.connected:
            for func in list(self._on_close_listeners):
                try:
                    func(self)
                except Exception as e:
                    log.exception(e)

        self.connecting = False
        self.connected = False
        self.IOLOOP.add_callback(self.connect)

    def __init__(self, cp):
        assert isinstance(cp, pika.ConnectionParameters)
        self._cp = cp
        self._on_close_listeners = set()
        self._on_open_listeners = set()
        self.channel = None
        self.connection = None
        self.connecting = None
        self.connected = None
        self._queue = list()
        self._memory = list()

    @tornado.gen.coroutine
    def connect(self):
        if self.connecting:
            return

        log.info("Connecting to Rabbitmq...")
        self.connecting = True

        self.connection = yield self._connect()
        self.connected = True
        self.connecting = False
        log.debug("Connection establishment")

        yield self._open_channel()

    def _on_channel_open(self):
        for func in list(self._on_open_listeners):
            try:
                func(self)
            except Exception as e:
                log.exception(e)

        self.IOLOOP.add_callback(self._bethink)

    @tornado.gen.coroutine
    def _bethink(self):
        mem = copy(self._memory)
        while mem:
            o, f = heappop(mem)
            f()

        while self._queue:
            o, f = heappop(self._queue)
            f()

    def add_close_listener(self, func):
        self._on_close_listeners.add(func)

    def add_open_listener(self, func):
        self._on_open_listeners.add(func)

    @queued(10)
    @memory(10)
    def exchange_declare(self, exchange, exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False, arguments=None, type=None):
        f = Future()
        self.channel.exchange_declare(lambda *a: f.set_result(a), exchange=exchange, exchange_type=exchange_type,
                                      passive=passive, durable=durable, auto_delete=auto_delete,
                                      internal=internal, nowait=nowait, arguments=arguments, type=type)
        return f

    @queued(20)
    @memory(20)
    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        f = Future()
        self.channel.queue_declare(
            lambda *a: f.set_result(a), queue=queue, passive=passive, durable=durable,
            exclusive=exclusive, auto_delete=auto_delete, nowait=nowait,
            arguments=arguments
        )
        return f

    @tornado.gen.coroutine
    def _open_channel(self):
        self.channel = yield self._channel()
        self.channel.add_on_close_callback(self._on_channel_close)
        self.IOLOOP.add_callback(self._on_channel_open)
        log.info('Channel "{0}" was opened.'.format(self.channel))

    def _channel(self):
        f = Future()
        log.debug("Creating channel")
        self.connection.channel(on_open_callback=f.set_result)
        return f

    def _connect(self):
        f = Future()
        self._make_connection(f)
        return f

    def _make_connection(self, future):
        log.info('PikaClient: Trying to connect to rabbitmq://%s:%s/%s, Object: %r',
                 self._cp.host, self._cp.port, self._cp.virtual_host, self)

        def reconnect(*args):
            self.IOLOOP.add_timeout(
                self.IOLOOP.time() + self.RECONNECT_TIMEOUT,
                partial(self._make_connection, future)
            )

        TornadoConnection(
            self._cp,
            on_open_callback=future.set_result,
            on_open_error_callback=reconnect,
            on_close_callback=self._on_close
        )

    def _reconnect(self, *args, **kwargs):
        try:
            self.channel.close()
        except:
            pass

        try:
            self.connection.close()
        except:
            pass

        self.IOLOOP.add_timeout(self.IOLOOP.time() + self.RECONNECT_TIMEOUT, self._connect)

    @queued(500)
    @memory(500)
    def queue_bind(self, queue, exchange, routing_key=None, nowait=False, arguments=None):
        f = Future()
        self.channel.queue_bind(
            lambda *a: f.set_result(a), queue, exchange, routing_key=routing_key, nowait=nowait, arguments=arguments
        )
        return f

    @queued(600)
    @memory(600)
    def consume(self, queue, callback):
        assert callable(callback)
        return self.channel.basic_consume(consumer_callback=callback, queue=queue, no_ack=False)

    @queued(900)
    @memory(900)
    def cancel(self, consumer_tag='', nowait=False):
        f = Future()
        self.channel.basic_cancel(callback=lambda *a: f.set_result(a), consumer_tag=consumer_tag, nowait=nowait)
        return f

    @queued(500)
    @memory(500)
    def queue_unbind(self, queue='', exchange=None, routing_key=None, arguments=None):
        f = Future()
        self.channel.queue_unbind(
            callback=lambda *a: f.set_result(a), queue=queue,
            exchange=exchange, routing_key=routing_key, arguments=arguments
        )
        return f

    @queued()
    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False, immediate=False):
        return self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                                   properties=properties, mandatory=mandatory, immediate=immediate)

    def _on_channel_close(self, channel, code, reason, **kwargs):
        self.IOLOOP.add_timeout(self.IOLOOP.time()+1, self._reconnect)