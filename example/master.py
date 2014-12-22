# encoding: utf-8
import tornado.ioloop
import tornado.gen
import tornado.web
import tornado.log
import tornado.options

from crew import TimeoutError, ExpirationError
from crew.master.tornado import Client


class MainHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.application.crew.call('test', "*" * 1000000, priority=100)
        self.write("{0}: {1}".format(type(resp).__name__, str(resp)))


class StatHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self):
        resp = yield self.application.crew.call('stat', persistent=False, priority=0)
        self.write("{0}: {1}".format(type(resp).__name__, str(resp)))


class FastHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self):
        try:
            resp = yield self.application.crew.call(
                'dead', persistent=False, priority=255, expiration=3,
            )
            self.write("{0}: {1}".format(type(resp).__name__, str(resp)))
        except TimeoutError:
            self.write('Timeout')
        except ExpirationError:
            self.write('All workers are gone')


class LongPoolingHandler(tornado.web.RequestHandler):
    LISTENERS = []

    @tornado.web.asynchronous
    def get(self):
        self.LISTENERS.append(self.response)

    def response(self, data):
        self.finish(str(data))

    @classmethod
    def responder(cls, data):
        for cb in cls.LISTENERS:
            cb(data)

        cls.LISTENERS = []


class AsyncStyle(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def get(self):
        self.application.crew.call('stat', callback=self.on_response, persistent=False, priority=0)

    def on_response(self, resp):
        self.write("{0}: {1}".format(type(resp).__name__, str(resp)))


class PublishHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        resp = yield self.application.crew.call('publish', self.request.body)
        self.finish(str(resp))


class PublishHandler2(tornado.web.RequestHandler):

    def post(self, *args, **kwargs):
        self.application.crew.publish('test', self.request.body)


class Multitaskhandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        with self.application.crew.parallel() as mc:
            mc.call('test')
            mc.call('stat')
            test_result, stat_result = yield mc.result()
            self.set_header('Content-Type', 'text/plain')
            self.write("Test result: {0}\nStat result: {1}".format(test_result, stat_result))


application = tornado.web.Application(
    [
        (r"/", MainHandler),
        (r"/stat", StatHandler),
        (r"/stat2", StatHandler),
        (r"/fast", FastHandler),
        (r'/subscribe', LongPoolingHandler),
        (r'/publish', PublishHandler),
        (r'/publish2', PublishHandler2),
        (r'/parallel', Multitaskhandler),
    ],
    autoreload=True,
    debug=True,
)

application.crew = Client()
application.crew.subscribe('test', LongPoolingHandler.responder)

if __name__ == "__main__":
    tornado.options.define('port', default=8888)
    tornado.options.parse_command_line()
    application.listen(tornado.options.options.port)
    tornado.ioloop.IOLoop.instance().start()
