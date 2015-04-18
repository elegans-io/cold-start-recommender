__author__ = "Angelo Leto"
__email__ = "angelo.leto@elegans.io"

import abc

def observable(function):
    """
    observable decorator
    :param function:
    :return:
    """
    wrapped_function_name = function.__name__
    def newf(*args, **kwargs):
        return_value = function(*args, **kwargs)
        called_class = args[0]

        try:
            observers = called_class.observers[wrapped_function_name]
        except KeyError:
            return False
        else:
            for o in observers:
                kwargs['return_value'] = return_value
                o(**kwargs)
        return return_value
    newf.__name__ = wrapped_function_name
    return newf

class Observable(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.observers = {}

    def register(self, function, observer):
        """
        register an observer to a specific event
        :param observer:
        :param event:
        :return:
        """
        event = function.__name__
        if not self.observers.has_key(event):
            self.observers[event] = {}

        try:
            self.observers[event][observer] = 1
        except KeyError :
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
            return op

    def unregister_all(self):
        self.observers = {}