__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"

import abc

from csrec.tools.observable import Observable
from csrec.tools.observable import observable
from csrec.exceptions import *

class DALBase(Observable):  # interface of the data abstraction layer
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        Observable.__init__(self)

    @abc.abstractmethod
    def init(self, **params):
        """
        initialization method

        exception: raise an InitializationException if any error occur

        :param params: dictionary of parameters
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def get_init_parameters_description():
        """
        return a description of the supported options like the following:

        { "db_path", "the path of a database file previously initialized" }


        :return: a dictionary with the supported options
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def insert_item(self, item_id, attributes):
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
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def remove_item(self, item_id=None):
        """
        remove an item from datastore

        exception: raise a DeleteException if any error occur

        :param item_id: the item id to delete, if None remove all items
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
    def get_items_iterator(self):
        """
        an iterator on items

        :return: an iterator on items
        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert_social_action(self, user_id, user_id_to, code=3.0):
        """
        insert a new user id on datastore, for each user a list of actions will be mantained:
            user0: { 'user_0':3.0, ..., 'user_N':5.0}
            ...
            userN: { 'user_0':3.0, ..., 'user_N':5.0}

        exception: raise an InsertException if any error occur

        :param user_id: user id who make the action
        :param user_id_to: the user id destination of the action
        :param code: the code, default value is 3.0
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def remove_social_action(self, user_id, user_id_to):
        """
        remove a social action from datastore

        exception: raise a DeleteException if any error occur

        :param user_id: user id who make the action
        :param user_id_to: the user id destination of the action
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def insert_item_action(self, user_id, item_id, code=3.0, item_meaningful_info=None, only_info=False):
        """
        insert a new item code on datastore, for each user a list of ratings will be mantained:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        exception: raise an InsertException if any error occur

        :param user_id: user id
        :param item_id: item id
        :param code: the code, default value is 3.0
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def remove_item_action(self, user_id, item_id):
        """
        remove a rating made by a user from the datastore

        exception: raise a DeleteException if any error occur

        :param user_id: user id
        :param item_id: item id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
    def get_item_actions_iterator(self):
        """
        get an iterator on item actions

        exception: raise a GetException if any error occur

        :return: an iterator on item ratings for each user:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def get_info_used(self):
        """
        get the categories used

        exception: raise a GetException if any error occur

        :return: a set with the name of categories used
        """
        raise NotImplementedError

    def set_info_used(self, info_used):
        """
        insert a new category

        exception: raise an InsertException if any error occur

        :param info_used: the new category to use
        """
        raise NotImplementedError

    def remove_info_used(self, info_used=None):
        """
        remove a category from the datastore

        exception: raise a DeleteException if any error occur

        :param info_used: the category to be deleted, if None, reset all categories
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def reconcile_user(self, old_user_id, new_user_id):
        """
        merge two users under the new user id, old user id will be removed
        for each item rated more than once, those rated by new_user_id will be kept

        exception: raise a MergeEntitiesException if any error occur

        :param old_user_id: old user id, raise an error if does not exists
        :param new_user_id: new user id, raise an error if does not exists
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user_count(self):
        """
        get the number of users who rated items

        exception: raise a GetException if any error occur

        :return: the number of users
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_items_count(self):
        """
        get the number of items

        exception: raise a GetException if any error occur

        :return: the number of items
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_social_count(self):
        """
        Get the number of social actions
        :return: number of social actions
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_social_iterator(self):
        """
        iterator over social actions
        :return: iterator over social actions
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def reset(self):
        """
        reset all data into the datastore

        exception: raise a DeleteException if any error occur
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def serialize(self, filepath):
        """
        dump the datastore on file

        exception: raise a SerializeException if any error occur
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def restore(self, filepath):
        """
        restore the datastore from file

        exception: raise a RestoreException if any error occur
        """
        raise NotImplementedError
