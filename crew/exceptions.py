# encoding: utf-8


class TaskError(Exception):
    pass


class TimeoutError(TaskError):
    pass


class ExpirationError(TaskError):
    pass

class DuplicateTaskId(TaskError):
    pass