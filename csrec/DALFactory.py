__author__ = "Angelo Leto"
__email__ = "angleto@gmail.com"

import MemDAL

# database abstraction layer factory
def DALFactory(name='mem', params = {}):
    if name == "mem":
        dal = MemDAL.Database()
        try:
            dal.init(params = params)
        except:
            return None
        return dal