# encoding: utf-8
import json
import zlib
import time

import sys
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
from crew import ExpirationError


class serializers:
    json = 'json'
    pickle = 'pickle'
    text = 'text'
    __all__ = set([json, pickle, text])


class Client(object):
    SERIALIZERS = serializers.__all__

    def __init__(self, host='localhost', port=5672, virtualhost='/', credentials=None, io_loop=None):
        assert credentials is None or isinstance(credentials, PlainCredentials) or isinstance(credentials,
                                                                                              ExternalCredentials)

        self._cp = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            virtual_host=virtualhost
        )

        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.callbacks_queue = dict()
        self.client_uid = uuid()
        self.callbacks_hash = {}

        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.instance()
        self.io_loop = io_loop


    def _on_close(self, connection):
        log.info('PikaClient: Try to reconnect')
        self.io_loop.add_timeout(time.time() + 5, self.connect)


    def _on_connected(self, connection):
        log.debug('PikaClient: connected')
        self.connected = True
        self.connection = connection
        self.connection.channel(self._on_channel_open)


    def _on_channel_open(self, channel):
        log.info('Channel "{0}" was opened.'.format(channel))
        self.channel = channel

        self.channel.queue_declare(
            callback=self._on_results_queue_bound,
            queue=self.client_uid,
            exclusive=True,
            auto_delete=True,
            arguments={
                "x-message-ttl": 60000,
            }
        )
        self.channel.queue_declare(callback=self._on_dlx_queue_bound, queue='DLX',)

    def _on_dlx_queue_bound(self, frame):
        self.channel.basic_consume(consumer_callback=self._on_pika_message, queue='DLX', no_ack=True)

    def _on_results_queue_bound(self, frame):
        self.channel.basic_consume(consumer_callback=self._on_pika_message, queue=frame.method.queue, no_ack=False)
        self.connected = True

    def _on_pika_message(self, channel, method, props, body):
        log.debug('PikaCient: Message received, delivery tag #%i : %r' % (method.delivery_tag, len(body)))

        correlation_id = getattr(props, 'correlation_id', None)
        if not correlation_id in self.callbacks_hash:
            if method.exchange != 'DLX':
                log.info('Got result for task "{0}", but no has callback'.format(correlation_id))
            return

        cb = self.callbacks_hash.pop(correlation_id)
        content_type = getattr(props, 'content_type', 'text/plain')

        if method.exchange == 'DLX':
            dl = props.headers['x-death'][0]
            body = ExpirationError("Dead letter received. Reason: {0}".format(dl.get('reason')))
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

        if isinstance(cb, Future):
            if isinstance(body, Exception):
                cb.set_exception(body)
            else:
                cb.set_result(body)
        else:
            out = cb(body, headers=props.headers)
            return out


    def connect(self):
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
            log.exception('PikaClient: connection failed because: "{0}", trying again in 5 seconds'.format(str(e)))
            self._on_close(None)


    def call(self, channel, data=None, callback=None, serializer='pickle',
             headers={}, persistent=True, priority=None, expiration=86400, timestamp=None, gzip=None, gzip_level=6):
        assert priority <= 255
        assert isinstance(expiration, int) and expiration > 0
        assert serializer in self.SERIALIZERS

        if gzip is None and len(data) > 1024 * 32:
            gzip = True

        if serializer == 'pickle':
            data = pickle.dumps(data, protocol=2)
            content_type = 'application/python-pickle'
        elif serializer == 'json':
            data = json.dumps(data, sort_keys=False, encoding='utf-8', check_circular=False)
            content_type = 'application/json'
        else:
            data = str(data).encode('utf-8')
            content_type = 'text/plain'

        assert data

        data = zlib.compress(data, gzip_level) if gzip else data

        props = pika.BasicProperties(
            content_encoding='gzip' if gzip else 'plain',
            content_type=content_type,
            reply_to=self.client_uid,
            correlation_id="{0}.{1}".format(channel, uuid()),
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
