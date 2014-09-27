# encoding: utf-8
import tornado.ioloop
import tornado.web
from crew.master.tornado import Client


class HandlerBase(tornado.web.RequestHandler):
    def on_response(self, data, headers=None):
        self.set_header('Content-Type', 'text/plain')
        self.finish("{0}: {1}".format(type(data).__name__, unicode(data)))


class MainHandler(HandlerBase):
    @tornado.web.asynchronous
    def get(self):
        self.settings['crew'].call('test', "*" * 1000000, callback=self.on_response, gzip=True, priority=100)


class StatHandler(HandlerBase):
    @tornado.web.asynchronous
    def get(self):
        self.settings['crew'].call('stat', callback=self.on_response, persistent=False, priority=0)


class FastHandler(HandlerBase):
    @tornado.web.asynchronous
    def get(self):
        self.settings['crew'].call('dead', callback=self.on_response, persistent=False, priority=255, expiration=3)


cl = Client()
application = tornado.web.Application(
    [
        (r"/", MainHandler),
        (r"/stat", StatHandler),
        (r"/fast", FastHandler),
    ],
    crew = cl
)

if __name__ == "__main__":
    cl.connect()
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
