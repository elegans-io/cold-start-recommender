__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"


__base_error_code__ = 110

import unittest
from collections import defaultdict

import sys
import pickle  # serialization library
from csrec.dal import DALBase
from csrec.tools.singleton import Singleton
from csrec.tools.observable import observable
from csrec.tools.observable import Observable


class Database(DALBase, Singleton):
    def __init__(self):
        DALBase.__init__(self)

        self.__params_dictionary = {}  # abstraction layer initialization parameters

        self.items_tbl = {}  # table with items
        self.users_ratings_tbl = {}  # table with users rating
        self.users_recomm_tbl = {}  # table with recommendations
        self.users_social_tbl = {}  # table with action user-user

        self.tot_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.n_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # number of ratings  (inmemory testing)

    @staticmethod
    def get_init_parameters_description():
        param_description = {}
        return param_description

    def init(self, **params):
        """
        initialization method

        :param params: dictionary of parameters
        :return: True if the class was successfully initialized, otherwise return False
        """
        if not params:
            params = {}

        try:
            self.__params_dictionary.update(params)
        except:
            return False
        return True

    @observable
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

    @observable
    def remove_recomms(self, user_id):
        """
        remove an item from datastore

        :param user_id: user_id
        :return: True if the operation was successfully executed, otherwise return False
        """
        try:
            del self.users_recomm_tbl[user_id]
        except KeyError:
            return False
        return True

    @observable
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
            item_id: {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }

        :param item_id: item id
        :return: the item record
        """
        return self.items_tbl.get(item_id)

    @observable
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

    @observable
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
        #TODO see the recommender one
        if user_id in self.users_ratings_tbl:
            self.users_ratings_tbl[user_id][item_id] = code
        else:
            self.users_ratings_tbl[user_id] = {item_id: code}
        return True

    def insert_item_action_recommender(self, user_id, item_id, code=3.0, item_meaningful_info=None,
                                       only_info=False, info_used=set()):
        """

        self.item_meaningful_info can be any further information given with the dict item.
        e.g. author, category etc

        NB NO DOTS IN user_id, or they will be taken away. Fields in mongodb cannot have dots..

        If only_info==True, only the self.item_meaningful_info's are put in the co-occurrence, not item_id.
         This is necessary when we have for instance a "segmentation page" where we propose
         well known items to get to know the user. If s/he select "Harry Potter" we only want
         to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        :param user_id: id of user. NO DOTS, or they will taken away. Fields in mongodb cannot have dots.
        :param item_id: is either id or a dict with item_id_key
        :param code: float parsable
        :return: None
        """
        if not item_meaningful_info:
            item_meaningful_info = []

        # If self.only_info==True, only the self.item_meaningful_info's are put in the co-occurrence, not item_id.
        # This is necessary when we have for instance a "segmentation page" where we propose
        # well known items to get to know the user. If s/he select "Harry Potter" we only want
        # to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        # Now fill the dicts or the db collections if available
        user_id = str(user_id).replace('.', '')

        item = self.get_item(item_id)
        if item:
            # Do categories only if the item is stored
            if len(item_meaningful_info) > 0:
                for k, v in item.items():
                    if k in item_meaningful_info:
                        # Some items' attributes are lists (e.g. tags: [])
                        # or, worse, string which can represent lists...
                        try:
                            v = json.loads(v.replace("'", '"'))
                        except:
                            pass

                        if not hasattr(v, '__iter__'):
                            values = [v]
                        else:
                            values = v
                        info_used.add(k)
                        # we cannot set the rating, because we want to keep the info
                        # that a user has read N books of, say, the same author,
                        # category etc.
                        # We could, but won't, sum all the ratings and count the a result as "big rating".
                        # We won't because reading N books of author A and rating them 5 would be the same
                        # as reading 5*N books of author B and rating them 1.
                        # Therefore we take the average because --
                        # 1) we don't want ratings for category to skyrocket
                        # 2) if a user changes their idea on rating a book, it should not add up.
                        # Average is not perfect, but close enough.
                        #
                        # Take total number of ratings and total rating:
                        for value in values:
                            if len(str(value)) > 0:
                                self.tot_categories_user_ratings[k][user_id][value] += int(code)
                                self.n_categories_user_ratings[k][user_id][value] += 1
                                # for the co-occurrence matrix is not necessary to do the same for item, but better do it
                                # in case we want to compute similarities etc using categories
                                self.tot_categories_item_ratings[k][value][user_id] += int(code)
        else:
            self.insert_item(item_id=item_id)
        if not only_info:
            self.insert_item_action(user_id=user_id, item_id=item_id, code=code)

    def get_tot_categories_user_ratings(self):
        return self.tot_categories_user_ratings

    def get_tot_categories_item_ratings(self):
        return self.tot_categories_item_ratings

    def get_n_categories_user_ratings(self):
        return self.n_categories_user_ratings

    @observable
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

    @observable
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
        return self.users_ratings_tbl

    def get_user_item_actions(self, user_id):
        return self.users_ratings_tbl.get(user_id)

    def get_users_actions_on_item(self, item_id):
        users_actions = {}
        for user in self.users_ratings_tbl:
            try:
                code = self.users_ratings_tbl[user][item_id]
            except KeyError:
                continue
            else:
                users_actions[user] = code
        return users_actions

    def get_all_social_actions(self):
        return self.users_social_tbl

    def get_user_social_actions(self, user_id):
        return self.users_social_tbl.get(user_id)

    @observable
    def reconcile_user(self, old_user_id, new_user_id):
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

    @observable
    def reset(self):
        """
        reset the datastore

        :return: True if the operation was successfully executed, otherwise return False
        """
        self.items_tbl = {}
        self.users_ratings_tbl = {}
        self.users_recomm_tbl = {}
        self.users_social_tbl = {}
        self.tot_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.n_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # number of ratings  (inmemory testing)
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
                                     'user_social': self.users_social_tbl,
                                     'tot_categories_user_ratings': self.tot_categories_user_ratings,
                                     'tot_categories_item_ratings': self.tot_categories_item_ratings,
                                     'n_categories_user_ratings': self.n_categories_user_ratings
                }
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

        # reset existing data
        self.items_tbl = {}  # table with items
        self.users_ratings_tbl = {}  # table with users rating
        self.users_recomm_tbl = {}  # table with recommendations
        self.users_social_tbl = {}  # table with action user-user

        self.tot_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.n_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # number of ratings  (inmemory testing)

        try:
            with open(filepath, 'rb') as f:
                data_from_file = pickle.load(f)
                self.items_tbl = data_from_file['items']
                self.users_ratings_tbl = data_from_file['item_ratings']
                self.users_recomm_tbl = data_from_file['user_recomms']
                self.users_social_tbl = data_from_file['user_social']
                self.tot_categories_user_ratings = data_from_file['tot_categories_user_ratings']
                self.tot_categories_item_ratings = data_from_file['tot_categories_item_ratings']
                self.n_categories_user_ratings = data_from_file['n_categories_user_ratings']
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