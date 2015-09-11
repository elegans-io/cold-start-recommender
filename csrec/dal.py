__author__ = "elegans.io Ltd"
__email__ = "info@elegans.io"

import abc

from csrec.tools.observable import Observable
from csrec.tools.observable import observable


class DALBase(Observable):  # interface of the data abstraction layer
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        Observable.__init__(self)

    @abc.abstractmethod
    def init(self, **params):
        """
        initialization method
        :param params: dictionary of parameters
        :return: True if the class was successfully initialized, otherwise return False
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
        raise NotImplementedError

    @abc.abstractmethod
    def remove_recomms(self, item_id):
        """
        remove an item from datastore
        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    def reset_recomms(self):
        """
        remove all recommendations from datastore
        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user_recomms(self, user_id):
        """
        retrieve the list of recommendations for the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
        :param user_id: user id
        :return: the recommendations for a user, if the user does not exists returns an empty dictionary
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def insert_item(self, item_id, attributes):
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
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def remove_item(self, item_id):
        """
        remove an item from datastore
        :param item_id: item id
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
    def get_item(self, item_id):
        """
        return an item by ID
            {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }
        :param item_id: user id
        :return: the item record
        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert_social_action(self, user_id, user_id_to, code=3.0):
        """
        insert a new user id on datastore, for each user a list of actions will be mantained:
            user0: { 'user_0':3.0, ..., 'user_N':5.0}
            ...
            userN: { 'user_0':3.0, ..., 'user_N':5.0}
        :param user_id: user id
        :param user_id_to: user id
        :param code: the code, default value is 3.0
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def insert_item_action(self, user_id, item_id, code=3.0):
        """
        insert a new item code on datastore, for each user a list of ratings will be mantained:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :param user_id: user id
        :param item_id: item id
        :param code: the code, default value is 3.0
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def remove_item_action(self, user_id, item_id):
        """
        remove an item rating from datastore
        :param user_id: user id
        :param item_id: item id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def remove_social_action(self, user_id, iser_id_to):
        """
        remove an item rating from datastore
        :param user_id: user id
        :param item_id: item id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_users_item_actions(self):
        """
        return a dictionary with all ratings:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :return: a dictionary with ratings
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user_item_actions(self, user_id):
        """
        retrieve the list of ratings made by the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
        :param user_id: user id
        :return: the ratings of a user, if the user does not exists returns an empty dictionary
        """
        raise NotImplementedError

    def get_users_actions_on_item(self, item_id):
        """
        get actions on item made by users

        :param item_id
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_social_actions(self):
        """
        return a dictionary with all social actions performed BY each user:
            user0: { 'user_1':3.0, ..., 'user_M':5.0}
            ...
            userN: { 'user_0':3.0, ..., 'user_M':5.0}
        :return: a dictionary with action codes
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user_social_actions(self, user_id):
        """
        retrieve the list of ratings made by the user
            user0: { 'user_1': 3.0, ..., 'user_M': 5.0}
        :param user_id: user id
        :return: the social action performed by a user, if the user does not exists returns an empty dictionary
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def reconcile_user(self, old_user_id, new_user_id):
        """
        merge two users under the new user id, old user id will be removed
        for each item rated more than once, those rated by new_user_id will be kept
        :param old_user_id: old user id
        :param new_user_id: new user id
        :return: all the ratings of the user
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user_count(self):
        """
        count the number of users present in ratings table
        :return: the number of users
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_items_count(self):
        """
        count items
        :return: the number of items
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_item_ratings_iterator(self):
        """
        return an iterator on item ratings for each user:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :return: an iterator on users ratings
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_items_iterator(self):
        """
        return an iterator on items
        :return: an iterator on items
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def reset(self):
        """
        reset the datastore
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def serialize(self, filepath):
        """
        dump the datastore on file
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError

    @abc.abstractmethod
    @observable
    def restore(self, filepath):
        """
        restore the datastore from file
        :return: True if the operation was successfully executed, otherwise return False
        """
        raise NotImplementedError
