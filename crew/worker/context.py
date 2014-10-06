import sys
import types


class Context(object):

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class UniqueDict(dict):

    def __setitem__(self, key, value):
        if key in self:
            raise IndexError('Item already exists')

        super(UniqueDict, self).__setitem__(key, value)


class ContextModule(types.ModuleType):
    _STORAGE = {
        's': None,
        'h': None,
        'f': UniqueDict(),
        'p': None,
    }

    @property
    def settings(self):
        return self.__class__._STORAGE['s']

    @settings.setter
    def settings(self, value):
        self.__class__._STORAGE['s'] = value

    @property
    def headers(self):
        return self.__class__._STORAGE['h']

    @headers.setter
    def headers(self, value):
        self.__class__._STORAGE['h'] = value

    @property
    def handlers(self):
        return self.__class__._STORAGE['f']

    @property
    def pubsub(self):
        return self.__class__._STORAGE['p']

    @pubsub.setter
    def pubsub(self, value):
        self.__class__._STORAGE['p'] = value


context = ContextModule('context')
sys.modules[__name__].context = ContextModule('context')
__all__ = [context, Context]
