__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"


__base_error_code__ = 110

import unittest

import sys
import pickle  # serialization library
from dal import DALBase
from tools.Singleton import Singleton


class Database(DALBase, Singleton):
    def __init__(self):
        DALBase.__init__(self)

        self.__params_dictionary = {}  # abstraction layer initialization parameters
        self.items_tbl = None  # table with items
        self.users_ratings_tbl = None  # table with users rating
        self.users_recomm_tbl = None  # table with recommendations
        self.users_social_tbl = None  # table with action user-user

    def init(self, **params):
        """
        initialization method

        :param params: dictionary of parameters
        :return: True if the class was successfully initialized, otherwise return False
        """
        if not params:
            params = {}

        self.__params_dictionary.update(params)
        return_value = True

        try:
            self.items_tbl = {}
            self.users_recomm_tbl = {}
            self.users_ratings_tbl = {}
            self.users_social_tbl = {}
            print >> sys.stderr, ("MemDAL initialized")
        except:
            print >> sys.stderr, ("Error: unable to initialize tables: %d" % __base_error_code__)
            return_value = False

        return return_value

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

    def insert_item(self, item_id, attributes=None):
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
        if attributes:
            self.items_tbl[item_id] = attributes
        else:
            self.items_tbl[item_id] = {}
        return True

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
            item_id: {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }

        :param item_id: item id
        :return: the item record
        """
        return self.items_tbl.get(item_id)

    def get_item_value(self, item_id, key):
        """
        return the value of an information of the item.
        e.g. If we have

        item_id: {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }

        item_id, category return "horror"

        :param item_id: user id
        :param key: the name or the info
        :return: value of the info in the item
        """
        return self.items_tbl.get(item_id, {}).get(key)

    def insert_social_action(self, user_id, user_id_to, code=3.0):
        """

        :param user_id_from:
        :param user_id_to:
        :param code:
        :return:
        """
        if user_id in self.users_social_tbl:
            self.users_social_tbl[user_id][user_id_to] = code
        else:
            self.users_social_tbl[user_id] = {user_id_to: code}
        return True

    def insert_item_action(self, user_id, item_id, code=3.0):
        """
        insert a new item code on datastore, for each user a list of ratings will be stored:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :param item_id: item id
        :param code: the code, default value is 3.0
        :return: True if the operation was successfully executed, otherwise return False
        """
        if user_id in self.users_ratings_tbl:
            self.users_ratings_tbl[user_id][item_id] = code
        else:
            self.users_ratings_tbl[user_id] = {item_id: code}
        return True

    def remove_item_action(self, user_id, item_id):
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

    def remove_social_action(self, user_id, user_id_to):
        """
        remove an user social action from datastore

        :param user_id: user id
        :param user_id_to: user id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        try:
            del self.users_social_tbl[user_id][user_id_to]
        except KeyError:
            return False
        return True

    def get_all_users_item_actions(self):
        """
        return a dictionary with all ratings:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :return: a dictionary with ratings
        """
        return self.users_ratings_tbl

    def get_user_item_actions(self, user_id):
        """

        :param user_id:
        :return: {user_id: {item0: 1, ....}}
        """
        return self.users_ratings_tbl.get(user_id)

    def get_all_social_actions(self):
        """

        :return: a dict with all actions user to user
        """
        return self.users_social_tbl

    def get_user_social_actions(self, user_id):
        """

        :param user_id:
        :return: {user_id: {u1: 1, u2: 1 ...}}
        """
        return self.users_social_tbl.get(user_id)

    def reconcile_user(self, old_user_id, new_user_id):
        """
        merge two users under the new user id, old user id will be removed
        for each item rated more than once, those rated by new_user_id will be kept

        Also, update the social table (user A -> user B)

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

        # updating the social stuff
        old_social_dict = {}
        try:
            old_social_dict = self.users_social_tbl[old_user_id]
            del self.users_social_tbl[old_user_id]
        except KeyError:
            pass

        new_social_dict = {}
        try:
            new_social_dict = self.users_social_tbl[new_user_id]
        except KeyError:
            pass

        old_social_dict.update(new_social_dict)
        self.users_social_tbl[new_user_id] = old_social_dict

        return True

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

    def get_social_count(self):
        """
        count social
        :return: number of social actions
        """
        tot_social = 0
        try:
            for user in self.users_social_tbl:
                tot_social += len(self.users_social_tbl.get(user, {}).keys())
        except:
            tot_social = 0
        return tot_social

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

    def get_social_iterator(self):
        """
        {user: {user: rating, user:rating, ...}}
        :return: iterator on user's social activity
        """
        for user in self.users_social_tbl:
            yield {user: self.users_social_tbl[user]}

    def reset(self):
        """
        reset the datastore

        :return: True if the operation was successfully executed, otherwise return False
        """
        self.items_tbl = {}
        self.users_ratings_tbl = {}
        self.users_recomm_tbl = {}
        self.users_social_tbl = {}
        return True

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
                                     'user_recomms': self.users_recomm_tbl,
                                     'user_social': self.users_social_tbl}
                pickle.dump(data_to_serialize, f)
        except:
            r_value = False
            print >> sys.stderr, ("Error: unable to serialize data to file: %d" % (__base_error_code__ + 1))

        return r_value

    def restore(self, filepath):
        """
        restore the datastore from file

        :return: True if the operation was successfully executed, otherwise return False
        """
        r_value = True
        # Write chunks of text data

        # reset existing data
        self.items_tbl = {}
        self.users_recomm_tbl = {}
        self.users_ratings_tbl = {}
        self.users_social_tbl = {}

        try:
            with open(filepath, 'rb') as f:
                data_from_file = pickle.load(f)
                self.items_tbl = data_from_file['items']
                self.users_ratings_tbl = data_from_file['item_ratings']
                self.users_recomm_tbl = data_from_file['user_recomms']
                self.users_social_tbl = data_from_file['user_social']
        except:
            r_value = False
            print >> sys.stderr, ("Error: unable to load data from file: %d" % (__base_error_code__ + 2))

        return r_value


class MemDALTest(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.init({})
        self.rating_counter = 0
        self.social_counter = 0
        self.item_count = 100
        self.user_count = 50

    def on_insert_or_update_item_action(self, *args, **kwargs):
        self.rating_counter += 1

    def on_remove_item_action(self, *args, **kwargs):
        self.rating_counter -= 1

    def on_insert_or_update_social_action(self, *args, **kwargs):
        self.social_counter += 1

    def on_remove_social_action(self, *args, **kwargs):
        self.social_counter -= 1

    def test_init_and_insert(self):

        self.db.register(self.db.insert_item_action, self.on_insert_or_update_item_action)

        self.db.register(self.db.remove_item_action, self.on_remove_item_action)

        for i in range(0, self.item_count):
            for j in range(0, self.item_count * 10):
                self.db.insert_item(i, {'author': j % 3, 'category': j % 7, 'subcategory': [j % 11, j % 13]})

        self.assertEquals(self.db.get_items_count(), self.item_count)

        for u in range(0, self.user_count):
            self.db.insert_item_action(user_id=u, item_id=self.item_count % max(u, 1), code=u % 5)

        self.assertEquals(self.db.get_user_count(), self.user_count)

        self.assertEquals(self.item_count, self.db.get_items_count())

        self.assertEquals(self.user_count, self.db.get_user_count())

        for u in range(0, self.user_count):
            if u % 2 == 0:
                self.db.remove_item_action(u, u % 2)

        self.assertEquals(self.rating_counter, self.user_count/2)

    def test_init_and_social(self):

        self.db.register(self.db.insert_social_action, self.on_insert_or_update_social_action)

        self.db.register(self.db.remove_social_action, self.on_remove_social_action)

        for u in range(0, self.user_count):
            v = (u + 1) % self.user_count  # each one follows the next one
            self.db.insert_social_action(user_id=u, user_id_to=v, code=u % 5)

        self.assertEquals(self.db.get_social_count(), self.user_count)

        for u in range(0, self.user_count):
            v = (u + 1) % self.user_count  # each one follows the next one
            self.db.remove_social_action(u, v)

        self.assertEquals(self.db.get_social_count(), 0)

    def test_user_reconcile(self):
        self.rating_counter = 0
        self.db.register(self.db.insert_item_action, self.on_insert_or_update_item_action)

        for u in range(0, self.user_count * 100):
            self.db.insert_item_action(user_id=u % self.user_count, item_id=u % self.item_count, code=u % 5)

        merged_ratings = 0
        for u in range(0, self.user_count, 2):
            self.db.reconcile_user(u, u+1)
        iter = self.db.get_item_ratings_iterator()
        for k, v in iter:
            for r in v:
                merged_ratings += 1
        self.assertEquals(merged_ratings, self.item_count)

if __name__ == '__main__':
    unittest.main()