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
import json

from csrec.exceptions import *


def _dd_float():
    return defaultdict(float)


def _dd_int():
    return defaultdict(int)


def _dd_dd_int():
    return defaultdict(_dd_int)


class Database(DALBase, Singleton):
    def __init__(self):
        DALBase.__init__(self)

        self.__params_dictionary = {}  # abstraction layer initialization parameters

        self.items_tbl = defaultdict(_dd_float)  # table with items

        self.users_ratings_tbl = defaultdict(_dd_float)  # table with users rating
        self.items_ratings_tbl = defaultdict(_dd_float)  # table with items rating

        self.users_social_tbl = defaultdict(_dd_float)  # table with action user-user
        self.info_used = set()

        self.tot_categories_user_ratings = defaultdict(_dd_dd_int)  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(_dd_dd_int)  # ditto
        self.n_categories_user_ratings = defaultdict(_dd_dd_int)  # number of ratings  (inmemory testing)

    def init(self, **params):
        if not params:
            params = {}
        try:
            self.__params_dictionary.update(params)
        except Exception as e:
            e_message = "error during initialization"
            raise InitializationException(e_message + " : " + e.message)

    @staticmethod
    def get_init_parameters_description():
        param_description = {}
        return param_description

    @observable
    def insert_item(self, item_id, attributes=None):
        if attributes:
            self.items_tbl[item_id] = attributes
        else:
            self.items_tbl[item_id] = {}
        return True

    @observable
    def remove_item(self, item_id=None):
        if item_id is not None:
            try:
                del self.items_tbl[item_id]
            except KeyError:
                pass
        else:
            self.items_tbl.clear()

    def get_items(self, item_id=None):
        if item_id is not None:
            items = self.items_tbl[item_id]
            if not items:
                return {}
            else:
                return {item_id: items}
        else:
            return self.items_tbl

    def get_items_iterator(self):
        return self.items_tbl.iteritems()

    @observable
    def insert_social_action(self, user_id, user_id_to, code=3.0):
        if user_id in self.users_social_tbl:
            self.users_social_tbl[user_id][user_id_to] = code
        else:
            self.users_social_tbl[user_id] = {user_id_to: code}

    @observable
    def remove_social_action(self, user_id, user_id_to):
        try:
            del self.users_social_tbl[user_id][user_id_to]
        except KeyError:
            pass

    def get_social_actions(self, user_id=None):
        if user_id is not None:
            social_actions = self.users_social_tbl[user_id]
            if not social_actions:
                return {}
            else:
                return {user_id: social_actions}
        else:
            return self.users_social_tbl

    @observable
    def insert_item_action(self, user_id, item_id, code=3.0, item_meaningful_info=None, only_info=False):
        if not item_meaningful_info:
            item_meaningful_info = []

        # If self.only_info==True, only the self.item_meaningful_info's are put in the co-occurrence, not item_id.
        # This is necessary when we have for instance a "segmentation page" where we propose
        # well known items to get to know the user. If s/he select "Harry Potter" we only want
        # to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        # Now fill the dicts or the db collections if available
        user_id = str(user_id).replace('.', '')

        item = self.get_items(item_id=item_id)
        if item:
            # Do categories only if the item is stored
            if len(item_meaningful_info) > 0:
                for k, v in item[item_id].items():
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

                        self.set_info_used(k)

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
                                # for the co-occurrence matrix is not necessary to do the same for item,
                                #       but better do it
                                # in case we want to compute similarities etc using categories
                                self.tot_categories_item_ratings[k][value][user_id] += int(code)
        else:
            self.insert_item(item_id=item_id)
        if not only_info:
                self.users_ratings_tbl[user_id][item_id] = code
                self.items_ratings_tbl[item_id][user_id] = code

    @observable
    def remove_item_action(self, user_id, item_id):
        try:
            del self.users_ratings_tbl[user_id][item_id]
        except KeyError:
            pass

        try:
            del self.items_ratings_tbl[item_id][user_id]
        except KeyError:
            pass

    def get_item_actions(self, user_id=None):
        if user_id is not None:
            item_actions = self.users_ratings_tbl[user_id]
            if not item_actions:
                return {}
            else:
                return {user_id: item_actions}
        else:
            return self.users_ratings_tbl

    def get_item_actions_iterator(self):
        return self.users_ratings_tbl.iteritems()

    def get_item_ratings(self, item_id=None):
        if item_id is not None:
            users_actions = self.items_ratings_tbl[item_id]
            if users_actions:
                return {}
            else:
                return {item_id: users_actions}
        else:
            return self.items_ratings_tbl

    def get_info_used(self):
        return self.info_used

    def set_info_used(self, info_used):
        self.info_used.add(info_used)

    def remove_info_used(self, info_used=None):
        if info_used:
            try:
                self.info_used.remove(info_used)
            except KeyError:
                pass
        else:
            self.info_used.clear()

    @observable
    def reconcile_user(self, old_user_id, new_user_id):
        #  verifying that both users exists
        if old_user_id not in self.users_ratings_tbl:
            e_message = "unable to reconcile old user id does not exists: %s" % str(old_user_id)
            raise MergeEntitiesException(e_message)

        if new_user_id not in self.users_ratings_tbl:
            e_message = "unable to reconcile new user id does not exists: %s" % str(new_user_id)
            raise MergeEntitiesException(e_message)

        if old_user_id == new_user_id:
            e_message = "users to be reconcile are the same: %s" % str(new_user_id)
            raise MergeEntitiesException(e_message)

        # updating ratings
        old_user_actions = self.users_ratings_tbl[old_user_id]
        self.users_ratings_tbl[new_user_id].update(old_user_actions)
        del self.users_ratings_tbl[old_user_id]

        # replacing all ratings of the user
        for i, r in old_user_actions.items():
            try:
                del self.items_ratings_tbl[i][old_user_id]
            except KeyError:
                pass
            self.items_ratings_tbl[i][new_user_id] = r

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

        for category in self.tot_categories_user_ratings:
            if old_user_id in self.tot_categories_user_ratings[category]:
                tot_curr_cat_values = self.tot_categories_user_ratings[category][old_user_id]
                del self.tot_categories_user_ratings[category][old_user_id]
                if new_user_id not in self.tot_categories_user_ratings[category]:
                    self.tot_categories_user_ratings[category][new_user_id] = defaultdict(int)
                self.tot_categories_user_ratings[category][new_user_id].update(tot_curr_cat_values)

        for category in self.n_categories_user_ratings:
            if old_user_id in self.n_categories_user_ratings[category]:
                n_curr_cat_values = self.n_categories_user_ratings[category][old_user_id]
                del self.n_categories_user_ratings[category][old_user_id]
                if new_user_id not in self.n_categories_user_ratings[category]:
                    self.n_categories_user_ratings[category][new_user_id] = defaultdict(int)
                self.n_categories_user_ratings[category][new_user_id].update(n_curr_cat_values)

        for category in self.tot_categories_item_ratings:
            for v in self.tot_categories_item_ratings[category]:
                if old_user_id in self.tot_categories_item_ratings[category][v]:
                    tot_curr_cat_item_values = self.tot_categories_item_ratings[category][v][old_user_id]
                    del self.tot_categories_item_ratings[category][v][old_user_id]
                    self.tot_categories_item_ratings[category][v][new_user_id] = tot_curr_cat_item_values

    def get_user_count(self):
        return len(self.users_ratings_tbl)

    def get_items_count(self):
        return len(self.items_tbl)

    def get_social_count(self):
        tot_social = 0
        try:
            for user in self.users_social_tbl:
                tot_social += len(self.users_social_tbl.get(user, {}).keys())
        except:
            tot_social = 0
        return tot_social

    def get_social_iterator(self):
        for user in self.users_social_tbl:
            yield {user: self.users_social_tbl[user]}

    @observable
    def reset(self):
        self.items_tbl.clear()
        self.users_ratings_tbl.clear()
        self.items_ratings_tbl.clear()
        self.users_social_tbl.clear()
        self.tot_categories_user_ratings.clear()
        self.tot_categories_item_ratings.clear()
        self.n_categories_user_ratings.clear()
        self.info_used.clear()

    @observable
    def serialize(self, filepath):
        # Write chunks of text data
        try:
            with open(filepath, 'wb') as f:
                data_to_serialize = {'items': self.items_tbl,
                                     'users_ratings': self.users_ratings_tbl,
                                     'items_ratings': self.items_ratings_tbl,
                                     'user_social': self.users_social_tbl,
                                     'tot_categories_user_ratings': self.tot_categories_user_ratings,
                                     'tot_categories_item_ratings': self.tot_categories_item_ratings,
                                     'n_categories_user_ratings': self.n_categories_user_ratings,
                                     'info_used': self.info_used
                                     }
                pickle.dump(data_to_serialize, f)
        except Exception as e:
            e_message = "unable to serialize data to file: %d" % (__base_error_code__ + 1)
            raise SerializeException(e_message + " : " + e.message)

    @observable
    def restore(self, filepath):
        # Write chunks of text data
        try:
            with open(filepath, 'rb') as f:
                data_from_file = pickle.load(f)
                self.items_tbl = data_from_file['items']
                self.users_ratings_tbl = data_from_file['users_ratings']
                self.items_ratings_tbl = data_from_file['items_ratings']
                self.users_social_tbl = data_from_file['user_social']
                self.tot_categories_user_ratings = data_from_file['tot_categories_user_ratings']
                self.tot_categories_item_ratings = data_from_file['tot_categories_item_ratings']
                self.n_categories_user_ratings = data_from_file['n_categories_user_ratings']
                self.info_used = data_from_file['info_used']
        except Exception as e:
            e_message = "unable to load data from file: %d" % (__base_error_code__ + 2)
            raise RestoreException(e_message + " : " + e.message)

    def get_tot_categories_user_ratings(self):
        return self.tot_categories_user_ratings

    def get_tot_categories_item_ratings(self):
        return self.tot_categories_item_ratings

    def get_n_categories_user_ratings(self):
        return self.n_categories_user_ratings
