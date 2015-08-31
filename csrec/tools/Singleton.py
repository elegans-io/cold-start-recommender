# Singleton

class  Singleton (object):
    __instance = {}
    def __new__(cls, *args, **kwargs):
        if Singleton.__instance.get(cls) is None:
            cls.__original_init__ = cls.__init__
            Singleton.__instance[cls] = object.__new__(cls, *args, **kwargs)
        elif cls.__init__ == cls.__original_init__:
            def nothing(*args, **kwargs):
                pass
            cls.__init__ = nothing
        return Singleton.__instance[cls]
