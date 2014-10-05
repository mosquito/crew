# encoding: utf-8
import json
import logging
import traceback
import time
import zlib
import pika
import sys

from .thread import KillableThread

if sys.version_info >= (3,):
    import pickle
else:
    import cPickle as pickle

from .context import context
from ..exceptions import TimeoutError, ExpirationError

log = logging.getLogger(__name__)


def thread_inner(func, results, *args):
    try:
        results.append(func(*args))
    except Exception as e:
        e._tb = traceback.format_exc()
        results.append(e)


class Listener(object):
    def __init__(self, handlers, host='localhost', port=5672, context=None, **kwargs):
        assert isinstance(port, int)
        self._handlers = handlers
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, **kwargs))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.context = context

        self.channel.exchange_declare(exchange='DLX', type='fanout', auto_delete=True)
        self.channel.queue_declare(queue='DLX')
        self.channel.queue_bind(queue='DLX', exchange='DLX')
        for queue, handler in self._handlers.items():
            if isinstance(handler, tuple):
                handler, args = handler
            else:
                args = {}

            self.channel.queue_declare(
                queue=queue,
                arguments={
                    "x-dead-letter-exchange": "DLX",
                    "x-message-ttl": 600000, # 10 minutes
                }
            )
            self.channel.basic_consume(self.on_request, queue=queue, **args)


    def get_worker(self, key):
        worker = self._handlers[key]
        self.w_name = worker.__name__
        if hasattr(worker, 'im_self'):
            self.w_name = worker.im_self.__name__
        context.settings = self.context
        return worker


    def set_env(self, props, method):
        self.content_type = getattr(props, 'content_type', 'text/plain')
        self.content_encoding = getattr(props, 'content_encoding', 'plain')
        self.gzip = self.content_encoding == 'gzip'
        self.cid = props.correlation_id
        self.dst = props.reply_to
        self.timestamp = int(getattr(props, 'timestamp', time.time()))
        self.expiration = int(getattr(props, 'expiration', 86400000)) / 1000
        self.start = time.time()
        self.delivery_tag = method.delivery_tag
        self.routing_key = method.routing_key
        context.headers = getattr(props, 'headers', {})


    def reset_env(self):
        self.content_type = 'text/plain'
        self.content_encoding = 'plain'
        self.gzip = False
        self.cid = None
        self.dst = None
        self.timestamp = 0
        self.expiration = 0
        self.delivery_tag = None
        self.routing_key = None
        context.headers = {}

    def handle(self, body):
        results = list()
        thread = KillableThread(target=thread_inner, args=(self.get_worker(self.routing_key), results, body))
        timeout = (int((self.timestamp + self.expiration) - time.time()))
        time_edge = time.time() + timeout

        thread.start()

        while time.time() < time_edge and not len(results):
            time.sleep(0.001)

        if not results:
            thread.kill()
            res = TimeoutError('Function lasted longer than {0} seconds'.format(timeout))
            log.debug('Task finished.')
        else:
            res = results.pop(0)
            if isinstance(res, Exception):
                log.debug(res._tb)
                log.error('Task error: {0}'.format(str(res)))

        return res

    def on_request(self, channel, method, props, body):
        try:
            self.set_env(props, method)

            if self.timestamp + self.expiration < self.start:
                log.error('Rejecting task because this expired of %.3f sec' % (self.start - (self.timestamp + self.expiration)))
                return self.reply(ExpirationError("Task now expired"))

            log.debug('Got message with content type "{0}" and length {1} bytes.'.format(self.content_type, len(body) if body else 0))

            body = self.deserializer(body)

            try:
                self.reply(self.handle(body))
            except Exception as e:
                log.info(traceback.format_exc())
                log.error(e)
                try:
                    self.reply(e)
                except:
                    self.reply(Exception(repr(e)))
        except Exception as e:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            print(traceback.format_exc())
            log.critical(repr(e))


    def reply(self, data):
        body = self.serializer(data)
        self.channel.basic_publish(
            exchange='',
            routing_key=self.dst,
            properties=pika.BasicProperties(
                correlation_id=self.cid,
                content_type=self.content_type,
                headers=context.headers,
                content_encoding=self.content_encoding,
            ),
            body=body
        )
        self.channel.basic_ack(delivery_tag=self.delivery_tag)
        log.info('Handle "%s" for %06f sec. Length of response: %s' % (self.w_name, time.time() - self.start, len(body)))
        self.reset_env()


    @property
    def serializer(self):
        def pickler(obj):
            return pickle.dumps(obj, protocol=2)

        def jsonifer(obj):
            return json.dumps(obj)

        def zliber(func):
            def wrap(obj):
                return zlib.compress(func(obj))
            return wrap

        def texter(obj):
            return str(obj).encode('utf-8')

        if 'application/python-pickle' in self.content_type:
            dumper = pickler
        elif 'application/json' in self.content_type:
            dumper = jsonifer
        else:
            dumper = texter

        if self.gzip:
            dumper = zliber(dumper)

        return dumper

    @property
    def deserializer(self):
        def pickler(obj):
            return pickle.loads(obj)

        def jsonifer(obj):
            return json.loads(obj)

        def zliber(func):
            def wrap(obj):
                return func(zlib.decompress(obj))
            return wrap

        def texter(obj):
            return str(obj).decode('utf-8')

        if 'application/python-pickle' in self.content_type:
            dumper = pickler
        elif 'application/json' in self.content_type:
            dumper = jsonifer
        else:
            dumper = texter

        if self.gzip:
            dumper = zliber(dumper)

        return dumper


    def loop(self):
        return self.channel.start_consuming()


__all__ = (Listener)
