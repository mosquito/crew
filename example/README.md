CREW Example
============

This example demonstrates the usage of the CREW package.

You have to install and run the RabbitMQ Server on the localhost before running this example.


Master
------

Master is HTTP server based on [tornado](http://www.tornadoweb.org/en/stable/).

It depends on crew and tornado. To install it, run:

    pip install crew tornado
	
And now you can run master:

    python master.py
	
Master is a non blocking http server, therefore it may serve thousands of requests per second.

Each request will be added to the RabbitMQ queue and will be handled by one of the workers, as soon as worker could process it.

	
Worker
------

Worker is a small application for processing requests in the queue and responding with the results.

It depends only on crew, so you can simply run it:

    python worker.py

You can run 2 or more workers to see how the requests balancing works between them. If your RabbitMQ server is listening on the external network interface, then you can run worker on different hosts by:

    python worker.py -H <rabbitmq_host> -P <rabbitmq_port>


Try it
------

The web server listens on 8888 port. After running master you can make some requests:

    $ curl -s localhost:8888
    str: Wake up Neo.

    $ curl http://localhost:8888/fast
    TimeoutError:

    $ curl http://localhost:8888/stat
    str: I serve 3 tasks
    
    $ curl http://localhost:8888/subscribe &
	[1] 27175
	$ curl -X POST http://localhost:8888/publish -d "Hello there"
	Hello there [1]  + done       curl http://localhost:8888/subscribe
	None
	
	$ curl http://localhost:8888/parallel
	Test result: Wake up Neo.
	Stat result: I'm worker "34vvqinRRhAyTPTJ9zsPoK". And I serve 6 tasks

You can find further details in worker.py and master.py files.
