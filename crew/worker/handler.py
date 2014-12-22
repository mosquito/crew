# encoding: utf-8
from .context import context


class HandlerClass(object):

    def __init__(self, data):
        self.data = data

    @classmethod
    def as_handler(cls, data):
        obj = cls(data)
        return obj.process()

    def process(self):
        return None

    @classmethod
    def bind(cls, task_id):
        tid = "crew.tasks.%s" % task_id
        context.handlers[tid] = cls.as_handler
