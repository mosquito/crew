#!/usr/bin/env python
# encoding: utf-8
import json
import sys
import pika

if sys.version_info >= (3,):
    import pickle
else:
    import cPickle as pickle


class PubSub(object):
    SERIALIZERS = {
        'json': 'application/json',
        'pickle': 'application/python-pickle',
        'text': 'text/plain'
    }

    def __init__(self, connection):
        self.channel = connection.channel()
        self.channel.exchange_declare(
            exchange='pubsub', exchange_type='fanout', durable=True)

    def get_serializer(self, name):
        assert name in self.SERIALIZERS
        if name == 'pickle':
            return (lambda x: pickle.dumps(x, protocol=2), self.SERIALIZERS[name])
        elif name == 'json':
            return (json.dumps, self.SERIALIZERS[name])
        elif name == 'text':
            return lambda x: str(x).encode('utf-8')

    def publish(self, channel, message, serializer='pickle'):
        assert serializer in self.SERIALIZERS

        serializer, t = self.get_serializer(serializer)

        self.channel.basic_publish(
            exchange='crew.PUBSUB',
            routing_key='',
            body=serializer(message),
            properties=pika.BasicProperties(
                content_type=t, delivery_mode=1, headers={'x-channel-name': channel})
        )
