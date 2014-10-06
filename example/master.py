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
        resp = yield self.settings['crew'].call('test', "*" * 1000000, priority=100)
        self.write("{0}: {1}".format(type(resp).__name__, str(resp)))


class StatHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.settings['crew'].call('stat', persistent=False, priority=0)
        self.write("{0}: {1}".format(type(resp).__name__, str(resp)))


class FastHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            resp = yield self.settings['crew'].call('dead', persistent=False, priority=255, expiration=3)
            self.write("{0}: {1}".format(type(resp).__name__, str(resp)))
        except TimeoutError:
            self.write('Timeout')
        except ExpirationError:
            self.write('All workers are gone')


class LongPoolingHandler(tornado.web.RequestHandler):
    LISTENERS = list()
    CL = None

    @tornado.web.asynchronous
    def get(self):
        self.LISTENERS.append(self.response)

    def response(self, data):
        self.LISTENERS.remove(self.response)
        self.finish(str(data))

    @classmethod
    def responder(cls, data):
        for cb in cls.LISTENERS:
            cb(data)

    @classmethod
    def subscribe(cls):
        cls.CL.subscribe('test', cls.responder)


class AsyncStyle(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.settings['crew'].call('stat', callback=self.on_response, persistent=False, priority=0)

    def on_response(self, resp):
        self.write("{0}: {1}".format(type(resp).__name__, str(resp)))


class PublishHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        resp = yield self.settings['crew'].call('publish', self.request.body)
        self.finish(str(resp))


cl = Client()

LongPoolingHandler.CL = cl
LongPoolingHandler.subscribe()

application = tornado.web.Application(
    [
        (r"/", MainHandler),
        (r"/stat", StatHandler),
        (r"/stat2", StatHandler),
        (r"/fast", FastHandler),
        (r'/subscribe', LongPoolingHandler),
        (r'/publish', PublishHandler)
    ],
    crew = cl,
    autoreload=True,
    debug=True,
)

if __name__ == "__main__":
    cl.connect()
    tornado.options.parse_command_line()
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
