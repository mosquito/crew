CREW Example
============

This example is a demonstration of the module CREW.
You need install and run Rabbitmq before running this example.


Master
------

Master is http server based on tornado. For running, please install tornado:

	pip install tornado
	
After that run:

	python master.py
	
Master is non blocking http server, therefore it may serve thousands of requests per minute.

Each request will be added in queue after accepting it by master and will be handled as soon as possible.

	
Worker
------

Worker is a small application for handling requests from queue and responses the handling results.

For running call:

	python worker.py

Now run 2 or more workers for balancing requests between them. Thanks to AMQP you can run workers for different servers.

Working
-------

Web server listening port 8888. After running master you may make any responses:
 
    $ curl -s localhost:8888 | wc
       1       4   10018

    $ curl http://localhost:8888/fast
	TimeoutError: 
    
    $ curl http://localhost:8888/stat
	str: I serve 3 tasks

You should be read worker.py and master.py for full understanding.