#!/usr/bin/env python
import logging
from shortuuid import uuid
from optparse import OptionParser
from socket import getfqdn


def run(handlers, **kwargs):
    from .listener import Listener, Context

    parser = OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="make lots of noise")
    parser.add_option("--logging", dest="logging", default='info', help="Logging level")
    parser.add_option("-H", "--host", dest="host", default='localhost', help="RabbitMQ host")
    parser.add_option("-P", "--port", dest="port", default=5672, type=int, help="RabbitMQ port")

    (options, args) = parser.parse_args()

    assert options.logging.lower() in set(['error', 'critical', 'debug', 'trace', 'info'])

    logging.basicConfig(
        format=u'[%(asctime)s] %(process)d:%(lineno)d %(levelname)-6s %(message)s',
        level=getattr(logging, options.logging.upper(), logging.INFO)
    )

    Listener(
        port=options.port,
        host=options.host,
        handlers=handlers,
        context=Context(
            options=options,
            node_uuid=uuid(getfqdn()),
            uuid=uuid(),
            **kwargs
        )
    ).loop()
