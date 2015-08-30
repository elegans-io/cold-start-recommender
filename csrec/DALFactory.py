__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"

# database abstraction layer factory


def DALFactory(name='mem', **params):
    if name == 'mem':
        import MemDAL
        dal = MemDAL.Database()
    # elif name == 'mongo':
    #     import MongoDAL
    #     dal = MongoDAL.Database()
    else:
        dal = None

    dal.init(**params)
    return dal