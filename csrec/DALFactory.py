__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"

# database abstraction layer factory


def DALFactory(name='mem', params=None):
    if name == 'mem':
        import MemDAL
        dal = MemDAL.Database()
    # elif name == 'mongo':
    #     import MongoDAL
    #     dal = MongoDAL.Database()
    else:
        dal = None

    try:
        dal.init(params=params)
    except:
        return None
    return dal