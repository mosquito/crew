#!/usr/bin/env python
# encoding: utf-8
import logging
import json
import zlib
import time
import sys
import pika
from pika.adapters.blocking_connection import BlockingConnection
from pika import ConnectionParameters, PlainCredentials
from functools import wraps
from shortuuid import uuid
from crew import ExpirationError, DuplicateTaskId, TimeoutError
from threading import Thread

if sys.version_info >= (3,):
    import pickle
else:
    import cPickle as pickle


log = logging.getLogger("crew.client.blocking")


def close_result(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if not self._closed:
            res = func(self, *args, **kwargs)
            self._closed = True
            return res
        else:
            raise AssertionError("Result already set")
    return wrap


class Result(object):
    def __init__(self):
        self.__oid = object()
        self._closed = False
        self.result = self.__oid
        self.headers = {}

    @close_result
    def set_result(self, result, headers={}):
        self.result = result
        self.headers = headers

    @close_result
    def set_exception(self, exc):
        self.result = exc
        self.raise_exc()

    def raise_exc(self):
        raise self.result

    def wait(self, timeout=60):
        start = time.time()
        while not self._closed:
            if (start + timeout) < time.time():
                raise TimeoutError("Waiting timeout")
            else:
                time.sleep(0.0001)

        return self.result

class Client(object):
    SERIALIZERS = {
        'json': 'application/json',
        'pickle': 'application/python-pickle',
        'text': 'text/plain',
    }

    _CHNUM = 1

    def __init__(self, host='127.0.0.1', port=5672, user=None, password=None, vhost='/'):
        if user:
            credentials = PlainCredentials(username=user, password=password)
        else:
            credentials = None

        self.__conn_params = ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=credentials
        )

        self.callbacks_hash = {}
        self._res_queue = "crew.master.%s" % uuid()
        self.__active = True
        self._connect()

    def parse_body(self, body, props):
        content_type = getattr(props, 'content_type', 'text/plain')

        if props.content_encoding == 'gzip':
            body = zlib.decompress(body)

        if 'application/json' in content_type:
            return json.loads(body)
        elif 'application/python-pickle' in content_type:
            return pickle.loads(body)

    def _connect(self):
        log.debug(
            "Starting new connection to amqp://%s:%d/%s",
            self.__conn_params.host,
            self.__conn_params.port,
            self.__conn_params.virtual_host
        )

        self.connection = BlockingConnection(self.__conn_params)
        self._CHNUM += 1

        log.debug("Opening channel %d", self._CHNUM)
        self.channel = self.connection.channel(self._CHNUM)

        self.channel.exchange_declare("crew.DLX", auto_delete=True, exchange_type="headers")

        self.channel.queue_declare(queue="crew.DLX", auto_delete=False)
        self.channel.queue_declare(
            queue=self._res_queue, exclusive=True, durable=False,
            auto_delete=True, arguments={"x-message-ttl": 60000}
        )

        self.channel.basic_qos(prefetch_count=1)

        self.channel.queue_bind("crew.DLX", "crew.DLX", arguments={"x-original-sender": self._res_queue})

        self.channel.basic_consume(self._on_dlx_received, queue="crew.DLX")
        self.channel.basic_consume(self._on_result, queue=self._res_queue)

        self.__connected = True
        t = Thread(target=self._consumer)
        t.daemon = True
        t.start()

        while not self.__connected:
            time.sleep(0.0001)

    def _on_result(self, channel, method, props, body):
        log.debug('PikaCient: Result message received, tag #%i len %d', method.delivery_tag, len(body))
        correlation_id = getattr(props, 'correlation_id', None)

        try:
            if correlation_id not in self.callbacks_hash:
                log.info('Got result for task "%d", but no has callback', correlation_id)
            else:
                cb = self.callbacks_hash.pop(correlation_id)
                body = self.parse_body(body, props)
                if isinstance(body, Exception):
                    cb.set_exception(body)
                else:
                    cb.set_result(body, headers=props.headers)
                return
        except Exception as e:
            log.exception(e)
        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def _on_dlx_received(self, channel, method, props, body):
        correlation_id = getattr(props, 'correlation_id', None)

        if correlation_id in self.callbacks_hash:
            cb = self.callbacks_hash.pop(correlation_id)

            try:
                dl = props.headers['x-death'][0]
                body = ExpirationError(
                    "Dead letter received. Reason: {0}".format(dl.get('reason'))
                )
                body.reason = dl.get('reason')
                body.time = dl.get('time')
                body.expiration = int(dl.get('original-expiration')) / 1000

                cb.set_exception(body)
            finally:
                channel.basic_ack(delivery_tag=method.delivery_tag)

        else:
            log.error("Method callback %s is not found", correlation_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

    def _consumer(self):
        while self.__active:
            try:
                self.channel.start_consuming()
            except:
                self.__connected = False
                while not self.__connected:
                    try:
                        self._connect()
                    except:
                        time.sleep(5)

    def close(self):
        self.__active = False
        self.channel.close()
        self.connection.close()

    def call(self, channel, data=None, serializer='pickle',
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

        callback = Result()

        self.callbacks_hash[props.correlation_id] = callback

        self.channel.basic_publish(
            exchange='',
            routing_key=qname,
            properties=props,
            body=data
        )

        return callback

    def get_serializer(self, name):
        assert name in self.SERIALIZERS
        if name == 'pickle':
            return (lambda x: pickle.dumps(x, protocol=2), self.SERIALIZERS[name])
        elif name == 'json':
            return (json.dumps, self.SERIALIZERS[name])
        elif name == 'text':
            return lambda x: str(x).encode('utf-8')
