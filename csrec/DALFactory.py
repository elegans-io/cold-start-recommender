__author__ = "Angelo Leto"
__email__ = "angleto@gmail.com"

import MemDAL
import MongoDAL

# database abstraction layer factory
def DALFactory(name='mem', params = {}):
    if name == 'mem':
        dal = MemDAL.Database()
    elif name == 'mongo':
        dal = MongoDAL.Database()
    else:
        dal = None

    try:
        dal.init(params = params)
    except:
        return None
    return dal