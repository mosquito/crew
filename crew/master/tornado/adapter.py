#!/usr/bin/env python
# encoding: utf-8
import pika
from pika.adapters.tornado_connection import TornadoConnection
from tornado.concurrent import Future
import tornado.ioloop
import tornado.gen
from tornado.log import app_log as log


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
        self.IOLOOP.add_timeout(self.IOLOOP.time() + self.RECONNECT_TIMEOUT, self.connect)

    def __init__(self, cp):
        assert isinstance(cp, pika.ConnectionParameters)
        self._cp = cp
        self._on_close_listeners = set()
        self._on_open_listeners = set()
        self.channel = None
        self.connection = None

    @tornado.gen.coroutine
    def connect(self):
        log.info("Connecting to Rabbitmq...")
        if self.connecting:
            return

        log.info('PikaClient: Trying to connect to RabbitMQ on {1}:{2}{3}, Object: {0}'.format(
            repr(self),
            self._cp.host,
            self._cp.port,
            self._cp.virtual_host)
        )

        self.connecting = True

        self.connection = yield self._connect()
        self.channel = yield self._channel()
        log.info('Channel "{0}" was opened.'.format(self.channel))

        for func in list(self._on_open_listeners):
            try:
                func(self)
            except Exception as e:
                log.exception(e)


    def add_close_listener(self, func):
        self._on_close_listeners.add(func)

    def add_open_listener(self, func):
        self._on_open_listeners.add(func)

    def exchange_declare(self, exchange, exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False, arguments=None, type=None):
        if not self.connected:
            raise IOError("Connection not opened")

        f = Future()
        self.channel.exchange_declare(f.set_result, exchange=exchange, exchange_type=exchange_type,
                                      passive=passive, durable=durable, auto_delete=auto_delete,
                                      internal=internal, nowait=nowait, arguments=arguments, type=type)
        return f

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        f = Future()
        self.channel.queue_declare(
            f.set_result, queue=queue, passive=passive, durable=durable,
            exclusive=exclusive, auto_delete=auto_delete, nowait=nowait,
            arguments=arguments
        )
        return f

    def _channel(self):
        f = Future()
        self.connection.channel(on_open_callback=f.set_result)
        return f

    def _connect(self):
        f = Future()
        TornadoConnection(self._cp, on_open_callback=f.set_result, on_open_error_callback=f.set_exception)
        return f

    def queue_bind(self, queue, exchange, routing_key=None, nowait=False, arguments=None):
        f = Future()
        self.channel.queue_bind(
            f.set_result, queue, exchange, routing_key=routing_key, nowait=nowait, arguments=arguments
        )
        return f

    def consume(self, queue, callback):
        assert callable(callback)
        return self.channel.basic_consume(consumer_callback=callback, queue=queue, no_ack=False)

