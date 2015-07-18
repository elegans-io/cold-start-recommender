__author__ = "Angelo Leto"
__email__ = "angleto@gmail.com"

# database abstraction layer factory
def DALFactory(name='mem', params = {}):
    if name == 'mem':
        import MemDAL
        dal = MemDAL.Database()
    elif name == 'mongo':
        import MongoDAL
        dal = MongoDAL.Database()
    else:
        dal = None

    try:
        dal.init(params=params)
    except:
        return None
    return dal