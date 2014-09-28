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
        resp = yield self.settings['crew'].call('test', "*" * 1000000, gzip=True, priority=100)
        self.write("{0}: {1}".format(type(resp).__name__, unicode(resp)))


class StatHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.settings['crew'].call('stat', persistent=False, priority=0)
        self.write("{0}: {1}".format(type(resp).__name__, unicode(resp)))


class FastHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            resp = yield self.settings['crew'].call('dead', persistent=False, priority=255, expiration=3)
            self.write("{0}: {1}".format(type(resp).__name__, unicode(resp)))
        except TimeoutError:
            self.write('Timeout')
        except ExpirationError:
            self.write('All workers are gone')


class AsyncStyle(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.settings['crew'].call('stat', callback=self.on_response, persistent=False, priority=0)

    def on_response(self, resp):
        self.write("{0}: {1}".format(type(resp).__name__, unicode(resp)))


cl = Client()
application = tornado.web.Application(
    [
        (r"/", MainHandler),
        (r"/stat", StatHandler),
        (r"/stat2", StatHandler),
        (r"/fast", FastHandler),
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
