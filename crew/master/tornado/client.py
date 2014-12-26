# encoding: utf-8
from functools import partial
import json
import zlib
import time
import sys
import tornado.ioloop
import tornado.gen
import pika
from tornado.gen import Future
from pika.credentials import ExternalCredentials, PlainCredentials
from shortuuid import uuid
from tornado.concurrent import Future
from tornado.log import app_log as log
from crew import ExpirationError, DuplicateTaskId
from multitask import MultitaskCall
from adapter import TornadoPikaAdapter


if sys.version_info >= (3,):
    import pickle
else:
    import cPickle as pickle

class Client(object):

    SERIALIZERS = {
        'json': 'application/json',
        'pickle': 'application/python-pickle',
        'text': 'text/plain',
    }

    def __init__(self, host='localhost', port=5672, virtualhost='/', credentials=None):

        if credentials is not None:
            assert isinstance(credentials, (PlainCredentials, ExternalCredentials))

        self.channel = TornadoPikaAdapter(pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            virtual_host=virtualhost,
        ))

        client_uid = uuid()
        self._res_queue = "crew.master.%s" % client_uid
        self._pubsub_queue = "crew.subscribe.%s" % client_uid
        self.callbacks_hash = {}
        self._subscribe_cache = {}

        tornado.ioloop.IOLoop.instance().add_callback(self.connect)

    def parse_body(self, body, props):
        content_type = getattr(props, 'content_type', 'text/plain')

        if props.content_encoding == 'gzip':
            body = zlib.decompress(body)

        if 'application/json' in content_type:
            return json.loads(body)
        elif 'application/python-pickle' in content_type:
            return pickle.loads(body)

    def _on_result(self, channel, method, props, body):
        log.debug('PikaCient: Result message received, tag #%i len %d', method.delivery_tag, len(body))

        correlation_id = getattr(props, 'correlation_id', None)
        if correlation_id not in self.callbacks_hash:
            log.info('Got result for task "%d", but no has callback', correlation_id)

        try:
            cb = self.callbacks_hash.pop(correlation_id)
            body = self.parse_body(body, props)

            if isinstance(cb, Future):
                if isinstance(body, Exception):
                    cb.set_exception(body)
                else:
                    cb.set_result(body)
            else:
                out = cb(body, headers=props.headers)
                return out
        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def _on_dlx_received(self, channel, method, props, body):
        correlation_id = getattr(props, 'correlation_id', None)
        if correlation_id in self.callbacks_hash:
            cb = self.callbacks_hash.pop(correlation_id)
        else:
            log.error("Method callback %s is not found", correlation_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            dl = props.headers['x-death'][0]
            body = ExpirationError(
                "Dead letter received. Reason: {0}".format(dl.get('reason'))
            )
            body.reason = dl.get('reason')
            body.time = dl.get('time')
            body.expiration = int(dl.get('original-expiration')) / 1000

            if isinstance(cb, Future):
                tornado.ioloop.IOLoop.instance().add_callback(partial(cb.set_result, body))
            elif callable(cb):
                tornado.ioloop.IOLoop.instance().add_callback(partial(cb, body))
            else:
                log.error("Callback is not callable")
        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    @tornado.gen.coroutine
    def connect(self):
        self.channel.exchange_declare("crew.PUBSUB", auto_delete=True, exchange_type="headers")
        self.channel.exchange_declare("crew.DLX", auto_delete=True, exchange_type="headers")

        self.channel.queue_declare(queue="crew.DLX", auto_delete=False)
        self.channel.queue_declare(
            queue=self._res_queue, exclusive=True,
            auto_delete=True, arguments={"x-message-ttl": 60000}
        )
        self.channel.queue_declare(
            queue=self._pubsub_queue, exclusive=True,
            auto_delete=True, arguments={"x-message-ttl": 60000}
        )


        self.channel.queue_bind("crew.DLX", "crew.DLX", arguments={"x-original-sender": self._res_queue})

        self.channel.consume(queue="crew.DLX", callback=self._on_dlx_received)
        self.channel.consume(queue=self._pubsub_queue, callback=self._on_subscribed_message)
        self.channel.consume(queue=self._res_queue, callback=self._on_result)

        yield self.channel.connect()

    def call(self, channel, data=None, callback=None, serializer='pickle',
             headers={}, persistent=True, priority=0, expiration=86400,
             timestamp=None, gzip=None, gzip_level=6, set_cid=None, routing_key=None):

        assert priority <= 255
        assert isinstance(expiration, int) and expiration > 0

        qname = "crew.tasks.%s" % channel

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

        headers.update({"x-original-sender": self._res_queue})

        props = pika.BasicProperties(
            content_encoding='gzip' if gzip else 'plain',
            content_type=content_type,
            reply_to=self._res_queue if not routing_key else routing_key,
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
            routing_key=qname,
            properties=props,
            body=data
        )

        if isinstance(callback, Future):
            return callback
        else:
            return props.correlation_id

    def _on_subscribed_message(self, channel, method, props, body):
        key = props.headers['x-channel-name']
        cb = self._subscribe_cache.get(key, None)
        if not cb:
            log.error("[PubSub] Method callback %s is not found", key)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        body = self.parse_body(body, props)

        channel.basic_ack(delivery_tag=method.delivery_tag)

        if isinstance(cb, Future):
            tornado.ioloop.IOLoop.instance().add_callback(partial(cb.set_result, body))
        elif callable(cb):
            tornado.ioloop.IOLoop.instance().add_callback(partial(cb, body))
        else:
            log.error("Callback is not callable")

    def subscribe(self, channel, callback):
        self.channel.queue_bind(self._pubsub_queue, exchange="crew.PUBSUB", arguments={"x-channel-name": channel})
        self._subscribe_cache[channel] = callback

    @tornado.gen.coroutine
    def unsubscribe(self, qname, callback):
        log.debug('Cancelling subscription for channel: "%s"', qname)
        yield self.channel.cancel(qname)

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
            exchange='crew.PUBSUB',
            routing_key='',
            body=serializer(message),
            properties=pika.BasicProperties(
                content_type=t, delivery_mode=1,
                headers={"x-channel-name": channel}
            )
        )

    def _on_custom_consume(self, callback, channel, method, props, body):
        log.debug('PikaCient: Result message received, tag #%i len %d', method.delivery_tag, len(body))

        try:
            body = self.parse_body(body, props)
            return callback(body, headers=props.headers)
        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def consume(self, queue, callback):
        self.channel.consume(queue=queue, callback=lambda *a: self._on_custom_consume(callback, *a))

    def parallel(self):
        return MultitaskCall(self)
