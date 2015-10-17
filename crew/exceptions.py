# encoding: utf-8


class TaskError(Exception):
    pass


class ConnectionError(Exception):
    pass


class TimeoutError(TaskError):
    pass


class ExpirationError(TaskError):
    pass


class DuplicateTaskId(TaskError):
    pass
