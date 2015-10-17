#!/usr/bin/env python
import logging
import pika
import signal
from time import sleep
from multiprocessing import Process
from shortuuid import uuid
from optparse import OptionParser
from socket import getfqdn
from .listener import Listener
from .context import Context, context


NODE_UUID = uuid(getfqdn())
UUID = uuid()


def listener_process(port, host, credentials, virtual_host, handlers, set_context):
    exit(Listener(
        port=port,
        host=host,
        credentials=pika.PlainCredentials(**credentials) if credentials else None,
        virtual_host=virtual_host,
        handlers=handlers,
        set_context=Context(**set_context)
    ).loop())


def run(**kwargs):
    parser = OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-v", "--verbose", action="store_true",
                      dest="verbose", default=False, help="make lots of noise")
    parser.add_option(
        "--logging", dest="logging", default='info', help="Logging level")
    parser.add_option(
        "-H", "--host", dest="host", default='localhost', help="RabbitMQ host")
    parser.add_option(
        "-P", "--port", dest="port", default=5672, type=int, help="RabbitMQ port")

    parser.add_option(
        "--username", dest="username", default=None, help="RabbitMQ username")

    parser.add_option(
        "--password", dest="password", default=None, help="RabbitMQ password")

    parser.add_option(
        "--vhost", dest="vhost", default='/', help="RabbitMQ virtual host")

    parser.add_option(
        "--forks", dest="forks", default=1, help="Create multiple process", type=int)

    (options, args) = parser.parse_args()

    log_level = getattr(logging, options.logging.upper(), logging.INFO)

    logging.basicConfig(
        format=u'[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-6s %(message)s',
        level=log_level,
    )

    arguments = dict(
        port=options.port,
        host=options.host,
        credentials={'username': options.username, 'password': options.password} if options.username else None,
        virtual_host=options.vhost,
        handlers=context.handlers,
        set_context=dict(
            options=options,
            node_uuid=NODE_UUID,
            uuid=UUID,
            **kwargs
        )
    )

    def create_proc():
        process = Process(target=listener_process, kwargs=arguments)
        process.start()
        return process

    if options.forks > 1:
        logging.info("Running %d processes", options.forks)
        processes = set()

        def careful_stop(*args):
            for process in list(processes):
                try:
                    process.terminate()
                except:
                    pass
            exit(0)

        signal.signal(signal.SIGINT, careful_stop)

        for count in range(options.forks):
            processes.add(create_proc())

        while len(processes):
            for process in list(processes):
                if not process.is_alive():
                    logging.warning("Restatring worker process")
                    processes.remove(process)
                    processes.add(create_proc())
            sleep(1)

    else:
        logging.info("Running single process")
        Listener(**arguments).loop()
