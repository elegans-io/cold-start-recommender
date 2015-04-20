__author__ = "Angelo Leto"
__email__ = "angelo.leto@elegans.io"

import abc
from Observable import Observable
from Observable import observable

#datastore abstraction layer: interface
class DALBase(Observable):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        Observable.__init__(self)

    @abc.abstractmethod
    def init(self, params = {}):
        """
        initialization method

        :param params: dictionary of parameters
        :return: True if the class was successfully initialized, otherwise return False
        """
        #TODO: implement me
        return

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
        #TODO: implement me
        return

    @abc.abstractmethod
    def remove_recomms(self, item_id):
        """
        remove an item from datastore

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def reset_recomms(self):
        """
        remove all recommendations from datastore

        :param item_id: item id
        :return: True if the operation was successfully executed, otherwise return False
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_user_recomms(self, user_id):
        """
        retrieve the list of recommendations for the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the recommendations for a user, if the user does not exists returns an empty dictionary
        """
        #TODO: implement me
        return

    @abc.abstractmethod
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
        #TODO: implement me
        return

    @abc.abstractmethod
    @observable
    def remove_item(self, item_id):
        """
        remove an item from datastore

        :param item_id: item id
        :return:
        """
        #TODO: implement me
        return

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
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_item(self, item_id):
        """
        return an item by ID
            {"author": "AA. VV.",
                "category":"horror",
                "subcategory":["splatter", "zombies"],
                ...
            }
        :param user_id: user id
        :return: the item record
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    @observable
    def insert_or_update_item_rating(self, user_id, item_id, rating=3.0):
        """
        insert a new item rating on datastore, for each user a list of ratings will be mantained:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :param item_id: item id
        :param rating: the rating, default value is 3.0
        :return: True if the operation was successfully executed, otherwise return False
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    @observable
    def remove_item_rating(self, user_id, item_id):
        """
        remove an item rating from datastore

        :param user_id: user id
        :param item_id: item id
        :return: True if the operation was successfully executed or it does not exists, otherwise return False
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_all_item_ratings(self):
        """
        return a dictionary with all ratings:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}
        :return: a dictionary with ratings
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_item_ratings(self, user_id):
        """
        retrieve the list of ratings made by the user
            user0: { 'item_0':3.0, ..., 'item_N':5.0}

        :param user_id: user id
        :return: the ratings of a user, if the user does not exists returns an empty dictionary
        """
        #TODO: implement me
        return

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
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_user_count(self):
        """
        count the number of users present in ratings table

        :return: the number of users
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_items_count(self):
        """
        count items

        :return: the number of items
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_item_ratings_iterator(self):
        """
        return an iterator on item ratings for each user:
            user0: { 'item_0':3.0, ..., 'item_N':5.0}
            ...
            userN: { 'item_0':3.0, ..., 'item_N':5.0}

        :return: an iterator on users ratings
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    def get_items_iterator(self):
        """
        return an iterator on items

        :return: an iterator on items
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    @observable
    def serialize(self, filepath):
        """
        dump the datastore on file

        :return: True if the operation was successfully executed, otherwise return False
        """
        #TODO: implement me
        return

    @abc.abstractmethod
    @observable
    def restore(self, filepath):
        """
        restore the datastore from file

        :return: True if the operation was successfully executed, otherwise return False
        """
        #TODO: implement me
        return

