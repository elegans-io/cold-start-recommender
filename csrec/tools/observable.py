__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"

import abc
from functools import wraps


def observable(function):
    """
    observable decorator
    :param function:
    :return:
    """
    @wraps(function)
    def newf(*args, **kwargs):
        return_value = function(*args, **kwargs)
        called_class = args[0]

        try:
            observers = called_class.observers[function]
        except KeyError:
            pass
        else:
            for o in observers:
                kwargs['return_value'] = return_value
                o(**kwargs)
        return return_value
    return newf


class Observable(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.observers = {}

    def register(self, function, observer):
        """
        register an observer to a specific event

        :param function: the function to observe
        :param observer: the observer function
        :return:
        """
        event = function.__name__
        if event not in self.observers:
            self.observers[event] = {}

        try:
            self.observers[event][observer] = 1
        except KeyError:
            op = False
        else:
            op = True
        return op

    def unregister(self, function, observer):
        event = function.__name__
        try:
            del self.observers[event][observer]
        except KeyError:
            return False
        else:
            return True

    def unregister_all(self):
        self.observers = {}