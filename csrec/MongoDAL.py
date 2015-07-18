__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"

__base_error_code__ = 110

import unittest

import sys
import DAL
from Observable import observable
#from tools.Singleton import Singleton

from pymongo import MongoClient

#class Database(DAL.DALBase, Singleton):
class Database(DAL.DALBase):
    def __init__(self):
        DAL.DALBase.__init__(self)

        self.__params_dictionary = {}  # abstraction layer initialization parameters

        self._mongo_client = None
        self._db = None

        self.mongo_host = "localhost"
        self.mongo_db_name = "csrec"
        self.mongo_replica_set = None

        self.parameters_variables = {
            'host' : self.mongo_host,
            'dbname' : self.mongo_db_name,
            'replicaset' : self.mongo_replica_set
        }

    def init(self, params={}):
        """
        initialization method

        :param params: dictionary of parameters
        :return: True if the class was successfully initialized, otherwise return False
        """
        self.__params_dictionary.update(params)

        for k in params:
            try:
                self.parameters_variables[k] = params[k]
            except KeyError:
                print >> sys.stderr, ("Error: unsupported parameter: %s" % (k))

        try:
            self._mongo_client = MongoClient(self.mongo_host)
            self._db = self._mongo_client[self.mongo_db_name]
        except:
            print >> sys.stderr, ("Error: unable to initialize mongo connection: %d" % (__base_error_code__))
            return False
        return True

    def insert_or_update_recomms(self, user_id, recommendations):
        """
        insert a new recommendation for a user

        :param user_id: user id
        :param recommendations: the recommendations for the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            self._db['users_recomm_tbl'].update({"_id": user_id}, {"$set": recommendations}, upsert=True)
        except:
            print >> sys.stderr, ("Error: failed to insert recommendations")
            return False
        return True

    def remove_recomms(self, user_id):
        """
        remove an item from datastore

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            self._db['users_recomm_tbl'].remove({"_id": user_id})
        except:
            print >> sys.stderr, ("Error: failed removing recommendations")
            return False
        return True

    def reset_recomms(self):
        """
        remove all recommendations from datastore

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            self._db['users_recomm_tbl'].remove({})
        except:
            print >> sys.stderr, ("Error: failed to reset recommentation table")
            return False
        return True

    def get_user_recomms(self, user_id):
        """
        retrieve the list of recommendations for the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the recommendations for a user, if the user does not exists returns an empty dictionary
        """
        try:
            recomm = self._db['users_recomm_tbl'].find_one({'_id': user_id})
            del recomm['_id']
        except:
            print >> sys.stderr, ("Error: failed to get recommentations")
            recomm = {}
        return recomm

    @observable
    def insert_or_update_item(self, item_id, attributes):
        """
        insert a new item on datastore

        :param item_id: item id
        :param attributes: a dictionary with item attributes e.g.
            {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            self._db['items_tbl'].update({"_id": item_id}, {"$set": attributes}, upsert=True)
        except:
            print >> sys.stderr, ("Error: failed to insert items")
            return False
        return True

    @observable
    def remove_item(self, item_id):
        """
        remove an item from datastore, remove also all references from ratings

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            self._db['items_tbl'].remove({"_id": item_id})
        except:
            print >> sys.stderr, ("Error: failed removing items")
            return False
        return True

    def get_all_items(self):
        """
        return a dictionary with all items:
            item_id0 : {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }
            ...
            item_idN : {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }

        :return: a dictionary with ratings
        """
        items_tbl = {}
        try:
            iter = self._db['items_tbl'].find({})
        except:
            print >> sys.stderr, ("Error: failed to retrieve items")
            return items_tbl

        for item in iter:
            key = item['_id']
            del item['_id']
            items_tbl[key] = item

        return items_tbl

    def get_item(self, item_id):
        """
        return an item by ID
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the item record
        """
        try:
            item = self._db['items_tbl'].find_one({'_id': item_id})
            del item['_id']
        except:
            print >> sys.stderr, ("Error: failed to get item")
            item = {}
        return item

    @observable
    def insert_or_update_item_rating(self, user_id, item_id, rating=3.0):
        """
        insert a new item rating on datastore, for each user a list of ratings will be stored:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :param item_id: item id
        :param rating: the rating, default value is 3.0
        :return: True if the operation was successfully executed, otherwise return False
        """
        item = { str(item_id) : rating }
        try:
            self._db['users_ratings_tbl'].update({"_id": str(user_id)}, {"$set": item}, upsert=True)
        except:
            print >> sys.stderr, ("Error: failed to insert item rating")
            print item
            return False
        return True

    @observable
    def remove_item_rating(self, user_id, item_id):
        """
        remove an item rating from datastore

        :param user_id: user id
        :param item_id: item id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        try:
            self._db['users_ratings_tbl'].update({"_id": user_id}, {"$unset": {item_id:""}})
        except:
            print >> sys.stderr, ("Error: failed removing item rating")
            return False
        return True

    def get_all_item_ratings(self):
        """
        return a dictionary with all ratings:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :return: a dictionary with ratings
        """
        users_ratings_tbl = {}
        try:
            iter = self._db['users_ratings_tbl'].find({})
        except:
            print >> sys.stderr, ("Error: failed to retrieve item ratings")
            return users_ratings_tbl

        for item in iter:
            key = item['_id']
            del item['_id']
            users_ratings_tbl[key] = item

        return users_ratings_tbl

    def get_item_ratings(self, user_id):
        """
        retrieve the list of ratings made by the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the ratings of a user, if the user does not exists returns an empty dictionary
        """
        try:
            ratings = self._db['users_ratings_tbl'].find_one({'_id': str(user_id)})
            del ratings['_id']
        except:
            print >> sys.stderr, "Error: failed to get item rating: ", user_id
            ratings = {}
        return ratings

    @observable
    def reconcile_user(self, old_user_id, new_user_id):
        """
        merge two users under the new user id, old user id will be removed
        for each item rated more than once, those rated by new_user_id will be kept

        :param old_user_id: old user id
        :param new_user_id: new user id
        :return: all the ratings of the new user
        """

        # load dictionaries
        old_usr_dictionary = {}
        try:
            old_usr_dictionary = self.get_item_ratings(old_user_id)
            self._db['users_ratings_tbl'].remove({"_id": str(old_user_id)})
        except:
            pass

        new_usr_dictionary = {}
        try:
            new_usr_dictionary = self.get_item_ratings(new_user_id)
            self._db['users_ratings_tbl'].remove({"_id": str(new_user_id)})
        except:
            pass

        if not old_usr_dictionary:
            old_usr_dictionary = {}

        if not new_usr_dictionary:
            new_usr_dictionary = {}

        # updating new entry
        old_usr_dictionary.update(new_usr_dictionary)

        for k in old_usr_dictionary:
            self.insert_or_update_item_rating(new_user_id, k, old_usr_dictionary[k])
        try:
            del old_usr_dictionary['_id']
        except:
            pass

        return old_usr_dictionary

    def get_user_count(self):
        """
        count the number of users present in ratings table

        :return: the number of users
        """
        try:
            count = self._db['users_ratings_tbl'].count()
        except:
            print >> sys.stderr, ("Error: unable to count users")
            return 0
        return count

    def get_items_count(self):
        """
        count items

        :return: the number of items
        """
        try:
            count = self._db['items_tbl'].count()
        except:
            print >> sys.stderr, ("Error: unable to count items")
            return 0
        return count

    def get_item_ratings_iterator(self):
        """
        return an iterator on item ratings for each user:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        :return: an iterator on users ratings
        """
        try:
            iter = self._db['users_ratings_tbl'].find({})
        except:
            print >> sys.stderr, ("Error: unable to get a valid iterator")
            yield None
            return

        for i in iter:
            key = i['_id']
            del i['_id']
            print (key, i)

    def get_items_iterator(self):
        """
        return an iterator on items

        :return: an iterator on items
        """
        try:
            iter = self._db['items_tbl'].find({})
        except:
            print >> sys.stderr, ("Error: unable to get a valid iterator")
            yield None
            return

        for i in iter:
            key = i['_id']
            del i['_id']
            print (key, i)

    @observable
    def reset(self):
        """
        reset the datastore

        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            self._db['items_tbl'].drop()
            self._db['users_ratings_tbl'].drop()
            self._db['users_recomm_tbl'].drop()
        except:
            return False
        return True

    @observable
    def serialize(self, filepath):
        """
        dump the datastore on file

        :return: True if the operation was successfully executed, otherwise return False
        """
        r_value = False
        return r_value

    @observable
    def restore(self, filepath):
        """
        restore the datastore from file

        :return: True if the operation was successfully executed, otherwise return False
        """
        r_value = False
        return r_value

class MongoDALTest(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.init({})
        self.rating_counter = 0
        self.item_count = 100
        self.user_count = 50

    def on_insert_or_update_item_rating(self, *args, **kwargs):
        self.rating_counter += 1

    def on_remove_item_rating(self, *args, **kwargs):
        self.rating_counter -= 1

    def test_init_and_insert(self):

        self.db.register(self.db.insert_or_update_item_rating, self.on_insert_or_update_item_rating)

        self.db.register(self.db.remove_item_rating, self.on_remove_item_rating)

        for i in range(0, self.item_count):
            for j in range(0, self.item_count * 1):
                r = self.db.insert_or_update_item(i, {'author': j%3, 'category': j%7, 'subcategory': [j%11, j%13]})
                self.assertEquals(r, True)

        for u in range(0, self.user_count):
            r = self.db.insert_or_update_item_rating(user_id=u, item_id=self.item_count % max(u,1), rating=u % 5)
            self.assertEquals(r, True)

    def test_user_reconcile(self):
        self.rating_counter = 0
        self.db.register(self.db.insert_or_update_item_rating, self.on_insert_or_update_item_rating)

        for u in range(0, self.user_count * 100):
            self.db.insert_or_update_item_rating(user_id=u % self.user_count, item_id=u % self.item_count, rating=u % 5)

        merged_ratings = 0
        for u in range(0, self.user_count, 2):
            self.db.reconcile_user(u, u+1)

if __name__ == '__main__':
    unittest.main()