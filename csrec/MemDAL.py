__author__ = "Angelo Leto"
__email__ = "angelo.leto@elegans.io"

__base_error_code__ = 110

import unittest

import sys
import pickle # serialization library
import DAL
from Observable import observable
from tools.Singleton import Singleton


class Database(DAL.DALBase, Singleton):
    def __init__(self):
        DAL.DALBase.__init__(self)

        self.__params_dictionary = {}  # abstraction layer initialization parameters
        self.items_tbl = None  # table with items
        self.users_ratings_tbl = None  # table with users rating
        self.users_recomm_tbl = None  # table with recommendations

    def init(self, params={}):
        self.__params_dictionary.update(params)
        rValue = True

        if rValue:
            try:
                self.items_tbl = {}
                self.users_recomm_tbl = {}
                self.users_ratings_tbl = {}
            except:
                print >> sys.stderr, ("Error: unable to initialize tables: %d" % (__base_error_code__))
                rValue = False

        return rValue

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
        self.users_recomm_tbl[user_id] = recommendations
        return True

    def remove_recomms(self, user_id):
        """
        remove an item from datastore

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            del self.users_recomm_tbl[user_id]
        except KeyError:
            return False
        return True

    def reset_recomms(self):
        """
        remove all recommendations from datastore

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        self.users_recomm_tbl = {}
        return True

    def get_user_recomms(self, user_id):
        """
        retrieve the list of recommendations for the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the recommendations for a user, if the user does not exists returns an empty dictionary
        """
        try:
            recomm = self.users_recomm_tbl[user_id]
        except KeyError:
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
        self.items_tbl[item_id] = attributes
        return True

    @observable
    def remove_item(self, item_id):
        """
        remove an item from datastore, remove also all references from ratings

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            del self.items_tbl[item_id]
        except KeyError:
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
        return self.items_tbl

    def get_item(self, item_id):
        """
        return an item by ID
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the item record
        """
        try:
            item = self.items_tbl[item_id]
        except KeyError:
            return None
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
        if user_id in self.users_ratings_tbl:
            self.users_ratings_tbl[user_id][item_id] = rating
        else:
            self.users_ratings_tbl[user_id] = { item_id: rating }
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
            del self.users_ratings_tbl[user_id][item_id]
        except KeyError:
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
        return self.users_ratings_tbl

    def get_item_ratings(self, user_id):
        """
        retrieve the list of ratings made by the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the ratings of a user, if the user does not exists returns an empty dictionary
        """
        try:
            recomm = self.users_ratings_tbl[user_id]
        except KeyError:
            recomm = {}
        return recomm

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
            old_usr_dictionary = self.users_ratings_tbl[old_user_id]
            del self.users_ratings_tbl[old_user_id]
        except KeyError:
            pass

        new_usr_dictionary = {}
        try:
            new_usr_dictionary = self.users_ratings_tbl[new_user_id]
        except KeyError:
            pass

        # updating new entry
        old_usr_dictionary.update(new_usr_dictionary)
        self.users_ratings_tbl[new_user_id] = old_usr_dictionary

        return self.users_ratings_tbl[new_user_id]

    def get_user_count(self):
        """
        count the number of users present in ratings table

        :return: the number of users
        """
        try:
            tot_users = len(self.users_ratings_tbl)
        except:
            tot_users = 0
        return tot_users

    def get_items_count(self):
        """
        count items

        :return: the number of items
        """
        try:
            tot_items = len(self.items_tbl)
        except:
            tot_items = 0
        return tot_items

    def get_item_ratings_iterator(self):
        """
        return an iterator on item ratings for each user:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        :return: an iterator on users ratings
        """
        return self.users_ratings_tbl.iteritems()

    def get_items_iterator(self):
        """
        return an iterator on items

        :return: an iterator on items
        """
        return self.items_tbl.iteritems()


    @observable
    def serialize(self, filepath):
        """
        dump the datastore on file

        :return: True if the operation was successfully executed, otherwise return False
        """
        r_value = True
        # Write chunks of text data
        try:
            with open(filepath, 'wb') as f:
                data_to_serialize = {'items': self.items_tbl,
                                     'item_ratings': self.users_ratings_tbl,
                                     'user_recomms': self.users_recomm_tbl}
                pickle.dump(data_to_serialize, f)
        except:
            r_value = False
            print >> sys.stderr, ("Error: unable to serialize data to file: %d" % (__base_error_code__ + 1))

        return r_value

    @observable
    def restore(self, filepath):
        """
        restore the datastore from file

        :return: True if the operation was successfully executed, otherwise return False
        """
        r_value = True
        # Write chunks of text data

        #reset existing data
        self.items_tbl = {}
        self.users_recomm_tbl = {}
        self.users_ratings_tbl = {}

        try:
            with open(filepath, 'rb') as f:
                data_from_file = pickle.load(f)
                self.items_tbl = data_from_file['items']
                self.users_ratings_tbl = data_from_file['item_ratings']
                self.users_recomm_tbl =  data_from_file['user_recomms']
        except:
            r_value = False
            print >> sys.stderr, ("Error: unable to load data from file: %d" % (__base_error_code__ + 2))

        return r_value


class MemDALTest(unittest.TestCase):

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
            for j in range(0, self.item_count * 10):
                self.db.insert_or_update_item(i, {'author': j%3, 'category': j%7, 'subcategory': [j%11, j%13]})

        self.assertEquals(self.db.get_items_count(), self.item_count)

        for u in range(0, self.user_count):
            self.db.insert_or_update_item_rating(user_id=u, item_id=self.item_count % max(u,1), rating=u % 5)

        self.assertEquals(self.db.get_user_count(), self.user_count)

        self.assertEquals(self.item_count, self.db.get_items_count())

        self.assertEquals(self.user_count, self.db.get_user_count())

        for u in range(0, self.user_count):
            if u % 2 == 0:
                self.db.remove_item_rating(u, u % 2)

        self.assertEquals(self.rating_counter, self.user_count/2)

    def test_user_reconcile(self):
        self.rating_counter = 0
        self.db.register(self.db.insert_or_update_item_rating, self.on_insert_or_update_item_rating)

        for u in range(0, self.user_count * 100):
            self.db.insert_or_update_item_rating(user_id=u % self.user_count, item_id=u % self.item_count, rating=u % 5)

        merged_ratings = 0
        for u in range(0, self.user_count, 2):
            self.db.reconcile_user(u, u+1)
        iter = self.db.get_item_ratings_iterator()
        for k,v in iter:
            for r in v:
                merged_ratings += 1
        self.assertEquals( merged_ratings, self.item_count )

if __name__ == '__main__':
    unittest.main()