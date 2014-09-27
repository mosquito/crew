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
	
Worker
------

Worker it's a small application for handling requests from master server and responses the handling results.

For running call:

	python worker.py
