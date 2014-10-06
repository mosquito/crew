CREW Example
============

This example demonstrates the usage of the CREW module.
You need to install and run Rabbitmq Server on the localhost before running this example.


Master
------

Master is http server based on [tornado](http://www.tornadoweb.org/en/stable/). For running, please install tornado:

	pip install tornado
	
After that run:

	python master.py
	
Master is non blocking http server, therefore it may serve thousands of requests per minute.

Each request will be added in queue after accepting it by master and will be handled as soon as possible.

	
Worker
------

Worker is a small application for handling requests from queue and responding the handling results.

For running call:

	python worker.py

Now run 2 or more workers for balancing requests between them. Thanks to AMQP you can run workers for different hosts.


Working
-------

Web server listening port 8888. After running master you may make any responses:
 
    $ curl -s localhost:8888 | wc
       1       4   10018

    $ curl http://localhost:8888/fast
	TimeoutError: 
    
    $ curl http://localhost:8888/stat
	str: I serve 3 tasks

You should read worker.py and master.py for full understanding.

Usage
-----

For example create your first app, and save as master.py:

	import tornado.ioloop
	import tornado.gen
	import tornado.web
	import tornado.log
	import jaon
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


And create your first task, and save as worker.py:

	# encoding: utf-8
    from crew.worker import run, context, Task

    
    @Task('test')
    def long_task(req):
    	context.settings.counter += 1
        return {"counter": context.settings.counter}
    
    run(
        counter=0
    )


After that run it:

	$ python master.py &
	$ python worker.py &
	$ wait

Try to test it:

	$ curl http://localhost:8888/
	
