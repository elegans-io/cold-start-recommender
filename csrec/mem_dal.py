__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"


__base_error_code__ = 110

from collections import defaultdict

import pickle  # serialization library
from csrec.dal import DALBase
from csrec.tools.singleton import Singleton
from csrec.tools.observable import observable
import json

from csrec.exceptions import *


class Database(DALBase, Singleton):
    def __init__(self):
        DALBase.__init__(self)

        self.__params_dictionary = {}  # abstraction layer initialization parameters

        self.items_tbl = {}  # table with items

        self.users_ratings_tbl = {}  # table with users rating
        self.items_ratings_tbl = {}  # table with items rating

        self.users_social_tbl = {}  # table with action user-user
        self.info_used = set()

        self.tot_categories_user_ratings = {}  # sum of all ratings
        self.tot_categories_item_ratings = {}  # ditto
        self.n_categories_user_ratings = {}  # number of ratings
        self.n_categories_item_ratings = {}

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
        """
        insert a new item on datastore, if the item already exists replace it

        exception: raise an InsertException if any error occur

        :param item_id: item id
        :param attributes: a dictionary with item attributes e.g.
            {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }
        """
        if attributes is not None:
            for k, v in attributes.items():
                try:
                    v = json.loads(v)
                except ValueError:
                    pass

                if not isinstance(v, list):
                    values = [v]
                else:
                    values = v

                self.items_tbl.setdefault(item_id, {})[k] = values
        else:
            self.items_tbl[item_id] = {}
        return True

    @observable
    def remove_item(self, item_id=None):
        """
        remove an item from datastore

        exception: raise a DeleteException if any error occur

        :param item_id: the item id to delete, if None remove all items
        """
        if item_id is not None:
            try:
                del self.items_tbl[item_id]
            except KeyError:
                pass
        else:
            self.items_tbl.clear()

    def get_items(self, item_id=None):
        """
        get a dictionary of items

        exception: raise a GetException if any error occur

        :param item_id: the item id to get, if None get all items
        :return: a dictionary with one or more items:
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

            an empty dictionary will be returned if the item was not found
        """

        if item_id is not None:
            return {item_id: self.items_tbl.get(item_id)}
        else:
            return self.items_tbl

    def get_items_iterator(self):
        """
        an iterator on items

        :return: an iterator on items
        """
        return self.items_tbl.items()

    @observable
    def insert_social_action(self, user_id, user_id_to, code=3.0):
        """
        insert a new user id on datastore, for each user a list of actions will be maintained:
            user0: { 'user_0':3.0, ..., 'user_N':5.0}
            ...
            userN: { 'user_0':3.0, ..., 'user_N':5.0}

        exception: raise an InsertException if any error occur

        :param user_id: user id who make the action
        :param user_id_to: the user id destination of the action
        :param code: the code, default value is 3.0
        """
        self.users_social_tbl.setdefault(user_id, {})[user_id_to] = code

    @observable
    def remove_social_action(self, user_id, user_id_to):
        """
        remove a social action from datastore

        exception: raise a DeleteException if any error occur

        :param user_id: user id who make the action
        :param user_id_to: the user id destination of the action
        """
        try:
            del self.users_social_tbl[user_id][user_id_to]
        except KeyError:
            pass

    def get_social_actions(self, user_id=None):
        """
        get the social actions

        exception: raise a GetException if any error occur

        :param user_id: user id, if None, return all social actions
        :return: a dictionary social actions performed BY users:
            user0: { 'user_1':3.0, ..., 'user_M':5.0}
            ...
            userN: { 'user_0':3.0, ..., 'user_M':5.0}
        """
        if user_id is not None:
            social_actions = self.users_social_tbl.get(user_id)
            if social_actions is None:
                return {}
            else:
                return {user_id: social_actions}
        else:
            return self.users_social_tbl

    @observable
    def insert_item_action(self, user_id, item_id, code=3.0, item_meaningful_info=None, only_info=False):
        """
        insert a new item code on datastore, for each user a list of ratings will be maintained:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        exception: raise an InsertException if any error occur

        :param user_id: user id
        :param item_id: item id
        :param code: the code, default value is 3.0
        :param item_meaningful_info: list of info to be considered, e.g. ['Author', 'tags']
        :param only_info: should only the info, and not the item, be considered
        """
        if item_meaningful_info is None:
            item_meaningful_info = []

        # If only_info==True, only the self.item_meaningful_info's are put in the co-occurrence, not item_id.
        # This is necessary when we have for instance a "segmentation page" where we propose
        # well known items to get to know the user. If s/he select "Harry Potter" we only want
        # to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        # Now fill the dicts or the db collections if available
        user_id = str(user_id).replace('.', '')

        item = self.get_items(item_id=item_id)[item_id]
        if item is not None:
            for info in item_meaningful_info:
                values = item.get(info)
                if values is not None:
                    self.set_info_used(info)

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
                        self.tot_categories_user_ratings.setdefault(info, {}).setdefault(user_id, {}).setdefault(value, 0)
                        self.tot_categories_user_ratings[info][user_id][value] += int(code)
                        self.n_categories_user_ratings.setdefault(info, {}).setdefault(user_id, {}).setdefault(value, 0)
                        self.n_categories_user_ratings[info][user_id][value] += 1
                        # same for items
                        self.tot_categories_item_ratings.setdefault(info, {}).setdefault(value, {}).setdefault(user_id, 0)
                        self.tot_categories_item_ratings[info][value][user_id] += int(code)
                        self.n_categories_item_ratings.setdefault(info, {}).setdefault(value, {}).setdefault(user_id, 0)
                        self.n_categories_item_ratings [info][value][user_id] += 1

        else:
            self.insert_item(item_id=item_id)
        if not only_info:
                self.users_ratings_tbl.setdefault(user_id, {})[item_id] = code
                self.items_ratings_tbl.setdefault(item_id, {})[user_id] = code

    @observable
    def remove_item_action(self, user_id, item_id):
        """
        remove a rating made by a user from the datastore

        exception: raise a DeleteException if any error occur

        :param user_id: user id
        :param item_id: item id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        try:
            del self.users_ratings_tbl[user_id][item_id]
        except KeyError:
            pass

        try:
            del self.items_ratings_tbl[item_id][user_id]
        except KeyError:
            pass

    def get_item_actions(self, user_id=None):
        """
        get a dictionary with user's actions

        exception: raise a GetException if any error occur

        :param user_id: user id, if None returns actions for all users
        :return: a dictionary with all ratings:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        """
        if user_id is not None:
            item_actions = self.users_ratings_tbl.get(user_id)
            if item_actions is None:
                return {}
            else:
                return {user_id: item_actions}
        else:
            return self.users_ratings_tbl

    def get_item_actions_iterator(self):
        """
        get an iterator on item actions

        exception: raise a GetException if any error occur

        :return: an iterator on item ratings for each user:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        """
        return self.users_ratings_tbl.items()

    def get_item_ratings(self, item_id=None):
        """
        get ratings on items made by users

        exception: raise a GetException if any error occur

        :param item_id: an item id, if None returns the ratings for all items
        :return: a dictionary with ratings for each item
            item0: { 'user_0':3.0, ..., 'user_N':5.0}
            ...
            itemN: { 'user_0':3.0, ..., 'user_N':5.0}
        """
        if item_id is not None:
            users_actions = self.items_ratings_tbl.get(item_id)
            if users_actions is None:
                return {}
            else:
                return {item_id: users_actions}
        else:
            return self.items_ratings_tbl

    def get_info_used(self):
        """
        get the categories used

        exception: raise a GetException if any error occur

        :return: a set with the name of categories used
        """
        return self.info_used

    def set_info_used(self, info_used):
        """
        insert a new category

        exception: raise an InsertException if any error occur

        :param info_used: the new category to use
        """
        self.info_used.add(info_used)

    def remove_info_used(self, info_used=None):
        """
        remove a category from the datastore

        exception: raise a DeleteException if any error occur

        :param info_used: the category to be deleted, if None, reset all categories
        """
        if info_used:
            try:
                self.info_used.remove(info_used)
            except KeyError:
                pass
        else:
            self.info_used.clear()

    @observable
    def reconcile_user(self, old_user_id, new_user_id):
        """
        merge two users under the new user id, old user id will be removed
        for each item rated more than once, those rated by new_user_id will be kept

        exception: raise a MergeEntitiesException if any error occur

        :param old_user_id: old user id, raise an error if does not exists
        :param new_user_id: new user id, raise an error if does not exists
        """
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
                del self.items_ratings_tbl.setdefault(i, {})[old_user_id]
            except KeyError:
                pass
            self.items_ratings_tbl.setdefault(i, {})[new_user_id] = r

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

        for category in self.info_used:
            if old_user_id in self.tot_categories_user_ratings[category]:
                tot_curr_cat_values = self.tot_categories_user_ratings[category][old_user_id]
                del self.tot_categories_user_ratings[category][old_user_id]
                if new_user_id not in self.tot_categories_user_ratings[category]:
                    self.tot_categories_user_ratings[category][new_user_id] = tot_curr_cat_values
                else:
                    for value, tot_code in tot_curr_cat_values.items():
                        self.tot_categories_user_ratings[category][new_user_id].setdefault(value, 0)
                        self.tot_categories_user_ratings[category][new_user_id][value] += tot_code

                n_curr_cat_values = self.n_categories_user_ratings[category][old_user_id]
                del self.n_categories_user_ratings[category][old_user_id]
                if new_user_id not in self.n_categories_user_ratings[category]:
                    self.n_categories_user_ratings[category][new_user_id] = n_curr_cat_values
                else:
                    for value, n_code in n_curr_cat_values.items():
                        self.n_categories_user_ratings[category][new_user_id].setdefault(value, 0)
                        self.n_categories_user_ratings[category][new_user_id][value] += n_code

            for v in self.tot_categories_item_ratings[category]:
                if old_user_id in self.tot_categories_item_ratings[category][v]:
                    tot_curr_cat_item_values = self.tot_categories_item_ratings[category][v][old_user_id]
                    del self.tot_categories_item_ratings[category][v][old_user_id]
                    self.tot_categories_item_ratings[category][v].setdefault(new_user_id, 0)
                    self.tot_categories_item_ratings[category][v][new_user_id] += tot_curr_cat_item_values

                    tot_curr_cat_item_values = self.n_categories_item_ratings[category][v][old_user_id]
                    del self.n_categories_item_ratings[category][v][old_user_id]
                    self.n_categories_item_ratings[category][v].setdefault(new_user_id, 0)
                    self.n_categories_item_ratings[category][v][new_user_id] += 1


    def get_user_count(self):
        """
        get the number of users who rated items

        exception: raise a GetException if any error occur

        :return: the number of users
        """
        return len(self.users_ratings_tbl)

    def get_items_count(self):
        """
        get the number of items

        exception: raise a GetException if any error occur

        :return: the number of items
        """
        return len(self.items_tbl)

    def get_social_count(self):
        """
        Get the number of social actions
        :return: number of social actions
        """
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
        """
        reset all data into the datastore

        exception: raise a DeleteException if any error occur
        """
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
        """
        dump the datastore on file

        exception: raise a SerializeException if any error occur
        """
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
        """
        restore the datastore from file

        exception: raise a RestoreException if any error occur
        """
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

    def get_n_categories_item_ratings(self):
        return self.n_categories_item_ratings

    def get_n_categories_user_ratings(self):
        return self.n_categories_user_ratings
