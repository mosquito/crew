CREW
====

AMQP based RPC library for Tornado

Use cases
---------

* Task queue
* Load balancing for different CPU-bound HTTP handlers
* ... other systems which involve RPC

Installation
------------

::

    pip install crew


Example
-------

See the full example_.

Usage
+++++

For example create your first app, and save as master.py::

    import tornado.ioloop
    import tornado.gen
    import tornado.web
    import tornado.log
    import json
    import tornado.options
    from crew import TimeoutError, ExpirationError
    from crew.master.tornado import Client

    class MainHandler(tornado.web.RequestHandler):

        @tornado.gen.coroutine
        def get(self):
            resp = yield self.settings['crew'].call(None, priority=100)
            self.write(json.dumps(resp)

    cl = Client()
    application = tornado.web.Application(
        [
            (r"/", MainHandler),
        ],
        crew=cl,
        autoreload=True,
        debug=True,
    )

    if __name__ == "__main__":
        cl.connect()
        tornado.options.parse_command_line()
        application.listen(8888)
        tornado.ioloop.IOLoop.instance().start()

And create your first task, and save as worker.py::

    # encoding: utf-8
    from crew.worker import run, context, Task

    @Task('test')
    def long_task(req):
        context.settings.counter += 1
        return {"counter": context.settings.counter}

    run(
        counter=0
    )

After that run it::

    $ python master.py &
    $ python worker.py &
    $ wait

Try to test it::

    $ curl http://localhost:8888/


.. _example: https://github.com/mosquito/crew/tree/master/example
