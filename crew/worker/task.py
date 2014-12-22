# encoding: utf-8
from functools import wraps
from .context import context


class Task(object):

    def __init__(self, task_id, force_gzip=False):
        self.task_id = "crew.tasks.%s" % task_id

    def __call__(self, func):
        context.handlers[self.task_id] = func

        @wraps(func)
        def wrap(*args, **kwargs):
            return func(*args, **kwargs)

        return wrap
