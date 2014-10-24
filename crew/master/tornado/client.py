# encoding: utf-8
from collections import defaultdict
import json
import traceback
import zlib
import time

import sys
from crew.exceptions import DuplicateTaskId

if sys.version_info >= (3,):
    import pickle
else:
    import cPickle as pickle

import pika
from pika.credentials import ExternalCredentials, PlainCredentials
from shortuuid import uuid
import pika.adapters.tornado_connection
from tornado.concurrent import Future
import tornado.ioloop
from tornado.log import app_log as log
from crew import ExpirationError, DuplicateTaskId


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


class Client(object):

    SERIALIZERS = {
        'json': 'application/json',
        'pickle': 'application/python-pickle',
        'text': 'text/plain',
    }

    def __init__(self, host='localhost', port=5672, virtualhost='/', credentials=None, io_loop=None):

        if credentials is not None:
            assert isinstance(credentials, (PlainCredentials, ExternalCredentials))

        self._cp = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            virtual_host=virtualhost,
        )

        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.callbacks_queue = dict()
        self.client_uid = uuid()
        self.callbacks_hash = {}
        self.pubsub = defaultdict(set)

        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.instance()

        self.io_loop = io_loop

    def _on_close(self, connection, *args):
        log.info('PikaClient: Try to reconnect')
        self.connecting = False
        self.connected = False
        self.io_loop.add_timeout(self.io_loop.time() + 5, self.connect)

    def _on_connected(self, connection):
        log.debug('PikaClient: connected')
        self.connected = True
        self.connection = connection
        self.connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):

        def on_dlx_bound(f):
            self.channel.basic_consume(consumer_callback=self._on_pika_message,
                                       queue='DLX',
                                       no_ack=False)

        def on_bind(f):
            self.channel.basic_consume(consumer_callback=self._on_pika_message,
                                       queue=self.client_uid,
                                       no_ack=False)
            self.connected = True

        def on_bound(f):
            self.channel.queue_bind(exchange='pubsub',
                                    queue=self.client_uid,
                                    callback=on_bind)
            self.channel.queue_declare(callback=on_dlx_bound, queue='DLX',)

        log.info('Channel "{0}" was opened.'.format(channel))
        self.channel = channel

        self.channel.queue_declare(
            callback=on_bound,
            queue=self.client_uid,
            exclusive=True,
            auto_delete=True,
            arguments={
                "x-message-ttl": 60000,
            }
        )

    def _on_pika_message(self, channel, method, props, body):
        log.debug('PikaCient: Message received, delivery tag #%i : %r' % (
            method.delivery_tag, len(body)
        ))

        correlation_id = getattr(props, 'correlation_id', None)
        if correlation_id not in self.callbacks_hash and method.exchange != 'pubsub':
            if method.exchange != 'DLX':
                log.info('Got result for task "{0}", but no has callback'.format(correlation_id))
            return

        if method.exchange == 'pubsub':
            cb = None
        else:
            cb = self.callbacks_hash.pop(correlation_id)

        content_type = getattr(props, 'content_type', 'text/plain')

        if method.exchange == 'DLX':
            dl = props.headers['x-death'][0]
            body = ExpirationError(
                "Dead letter received. Reason: {0}".format(dl.get('reason'))
            )
            body.reason = dl.get('reason')
            body.time = dl.get('time')
            body.expiration = int(dl.get('original-expiration')) / 1000
        else:
            if props.content_encoding == 'gzip':
                body = zlib.decompress(body)
            if 'application/json' in content_type:
                body = json.loads(body)
            elif 'application/python-pickle' in content_type:
                body = pickle.loads(body)

        channel.basic_ack(delivery_tag=method.delivery_tag)

        if method.exchange == 'pubsub':
            ch = props.headers.get('x-pubsub-channel-name', None)
            if ch in self.pubsub:
                for cb in self.pubsub[ch]:
                    try:
                        cb(body)
                    except Exception as e:
                        log.debug(traceback.format_exc())
                        log.error("Error in subscribed callback: {0}".format(str(e)))
                return
        else:
            if isinstance(cb, Future):
                if isinstance(body, Exception):
                    cb.set_exception(body)
                else:
                    cb.set_result(body)
            else:
                out = cb(body, headers=props.headers)
                return out

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

        try:
            self.connection = pika.adapters.tornado_connection.TornadoConnection(
                self._cp, on_open_callback=self._on_connected
            )
            self.connection.add_on_close_callback(self._on_close)

        except Exception as e:
            self.connecting = False
            log.exception(
                'PikaClient: connection failed because: '
                '"{0}", trying again in 5 seconds'.format(str(e))
            )
            self._on_close(None)

    def call(self, channel, data=None, callback=None, serializer='pickle',
             headers={}, persistent=True, priority=0, expiration=86400,
             timestamp=None, gzip=None, gzip_level=6, set_cid=None):

        assert priority <= 255
        assert isinstance(expiration, int) and expiration > 0

        serializer, content_type = self.get_serializer(serializer)

        if set_cid:
            cid = str(set_cid)
            if cid in self.callbacks_hash:
                raise DuplicateTaskId('Task ID: {0} already exists'.format(cid))

        else:
            cid = "{0}.{1}".format(channel, uuid())

        data = serializer(data)

        if gzip is None and data is not None and len(data) > 1024 * 32:
            gzip = True

        data = zlib.compress(data, gzip_level) if gzip else data


        props = pika.BasicProperties(
            content_encoding='gzip' if gzip else 'plain',
            content_type=content_type,
            reply_to=self.client_uid,
            correlation_id=cid,
            headers=headers,
            timestamp=int(time.time()),
            delivery_mode=2 if persistent else None,
            priority=priority,
            expiration="%d" % (expiration * 1000),
            )

        if callback is None:
            callback = Future()

        self.callbacks_hash[props.correlation_id] = callback

        self.channel.basic_publish(
            exchange='',
            routing_key=channel,
            properties=props,
            body=data
        )

        if isinstance(callback, Future):
            return callback
        else:
            return props.correlation_id

    def subscribe(self, channel, callback):
        self.pubsub[channel].add(callback)

    def get_serializer(self, name):
        assert name in self.SERIALIZERS
        if name == 'pickle':
            return (lambda x: pickle.dumps(x, protocol=2), self.SERIALIZERS[name])
        elif name == 'json':
            return (json.dumps, self.SERIALIZERS[name])
        elif name == 'text':
            return lambda x: str(x).encode('utf-8')

    def publish(self, channel, message, serializer='pickle'):
        assert serializer in self.SERIALIZERS

        serializer, t = self.get_serializer(serializer)

        self.channel.basic_publish(
            exchange='pubsub',
            routing_key='',
            body=serializer(message),
            properties=pika.BasicProperties(
                content_type=t, delivery_mode=1,
                headers={'x-pubsub-channel-name': channel}
            )
        )

    def parallel(self):
        return MultitaskCall(self)