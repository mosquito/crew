# encoding: utf-8
from crew.master.tornado import Client
import pika
from multiprocessing import Process
from socket import getfqdn
from shortuuid import uuid
from tornado import testing, httpserver
from tornado.gen import coroutine
from crew.worker import Listener, Context, context
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from .app.application import application
from .app import worker


class Worker(object):
    HOST = 'localhost'
    PORT = 5672
    VHOST = '/'
    USER = None
    PASSWORD = None
    NODE_UUID = uuid(getfqdn())
    UUID = uuid()

    def __init__(self, **kwargs):
        ctx = {'uuid': self.UUID, 'node_uuid': self.NODE_UUID}
        ctx.update(kwargs)

        self.worker = Process(
            target=Worker._listener,
            kwargs=dict(
                port=self.PORT,
                host=self.HOST,
                credentials={'user': self.USER, 'password': self.PASSWORD} if self.USER else None,
                virtual_host=self.VHOST,
                handlers=context.handlers,
                set_context=ctx,
            )
        )
        self.worker.start()

    def stop(self):
        self.worker.terminate()

    @classmethod
    def _listener(cls, port, host, credentials, virtual_host, handlers, set_context):
        return Listener(
            port=port,
            host=host,
            credentials=pika.PlainCredentials(**credentials) if credentials else None,
            virtual_host=virtual_host,
            handlers=handlers,
            set_context=Context(**set_context)
        ).loop()


class TestBaseHandler(testing.AsyncTestCase):
    ROUTER = None
    WORKERS = 1
    WORKER_CONTEXT = {
        'counter': 0,
    }

    def _start_server(self):
        self.app = application
        self.app.crew = Client()
        self.io_loop.add_callback(self.app.crew.connect)

        self.server = httpserver.HTTPServer(self.app)
        socket, self.port = testing.bind_unused_port()
        self.server.add_socket(socket)
        self.server.start()

    def setUp(self):
        testing.AsyncTestCase.setUp(self)
        self._start_server()
        self._client = AsyncHTTPClient()
        self.workers = set()
        for _ in range(self.WORKERS):
            self.workers.add(Worker(**self.WORKER_CONTEXT))

        print len(self.workers)

    def client(self, uri, **kwargs):
        req = HTTPRequest(
            "http://127.0.0.1:{0.port}{1}".format(self, uri if uri.startswith('/') else "/{0}".format(uri)),
            **kwargs
        )

        return self._client.fetch(req)

    @coroutine
    def tearDown(self):
        for worker in self.workers:
            worker.stop()

        yield self.server.close_all_connections()
        self.app.crew.close()
        self.server.stop()
