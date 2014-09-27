# encoding: utf-8


class HandlerClass(object):

    def __init__(self, data):
        self.data = data

    @classmethod
    def as_handler(cls, data):
        obj = cls(data)
        return obj.process()

    def process(self):
        return None
