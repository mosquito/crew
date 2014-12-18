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

        self.client_uid = uuid()
        self.callbacks_queue = dict()
        self.callbacks_hash = {}

    def parse_body(self, body, props):
        content_type = getattr(props, 'content_type', 'text/plain')

        if props.content_encoding == 'gzip':
            body = zlib.decompress(body)

        if 'application/json' in content_type:
            return json.loads(body)
        elif 'application/python-pickle' in content_type:
            return pickle.loads(body)

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

    def _on_result(self, channel, method, props, body):
        log.debug('PikaCient: Message received, delivery tag #%i : %r' % (
            method.delivery_tag, len(body)
        ))

        correlation_id = getattr(props, 'correlation_id', None)
        if correlation_id not in self.callbacks_hash:
            log.info('Got result for task "{0}", but no has callback'.format(correlation_id))

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

        channel.basic_ack(delivery_tag=method.delivery_tag)

    @tornado.gen.coroutine
    def connect(self):
        try:
            self.channel.connect()

            yield self.channel.queue_declare(
                queue=self.client_uid, exclusive=True, auto_delete=True, arguments={ "x-message-ttl": 60000, }
            )

            yield self.channel.exchange_declare('crew.DLX', auto_delete=True, internal=True)
            yield self.channel.queue_bind(self.client_uid, 'crew.DLX', routing_key=self.client_uid)

            self.channel.consume(queue=self.client_uid, callback=self._on_result)
            self.connected = True

        except Exception as e:
            self.connecting = False
            log.exception(
                'PikaClient: connection failed because: '
                '"{0}", trying again in 5 seconds'.format(str(e))
            )

    def call(self, channel, data=None, callback=None, serializer='pickle',
             headers={}, persistent=True, priority=0, expiration=86400,
             timestamp=None, gzip=None, gzip_level=6, set_cid=None, routing_key=None):

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
            reply_to=self.client_uid if not routing_key else routing_key,
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
        qname = "pubsub_%s" % channel
        # self.channel.basic_consume(qname)
        self.pubsub[qname].add(callback)

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
