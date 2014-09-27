CREW Example
============

This example is a demonstration of the module CREW.
You need install and run Rabbitmq before rubbung this example.

Master
------

Master it's http server based on tornado for running please install tornado

	pip install tornado
	
After that run:

	python master.py
	
Master is non blocking http server therefore this may serving thousands of requests.

After start of the single request this staying in queue on the RabbitMQ and will be handled as soon as possible.

	
Worker
------

Worker it's a small application for handling requests from master server and responses the handling results.

For running call:

	python worker.py

Now run 2 or more workers for balancing requests between them.

Thanks to AMQP you can run workers for different servers.

Wirking
-------

Web server listening port 8888. After running master you may make any responses:
 
    $ curl -s localhost:8888 | wc
       1       4   10018

    $ curl http://localhost:8888/fast
	TimeoutError: 
    
    $ curl http://localhost:8888/stat
	str: I serve 3 tasks

You should be read worker.py and master.py for full understanding.