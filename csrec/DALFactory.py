__author__ = "Angelo Leto"
__email__ = "angleto@gmail.com"

import memDAL

def DatabaseFactory(name='mem'):
    if name == "mem":
        return memDAL.Database()