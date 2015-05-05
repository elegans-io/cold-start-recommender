from collections import defaultdict
import pandas as pd
import numpy as np
from time import time
import logging
import json
from tools.Singleton import Singleton


class Recommender(Singleton):
    """
    Cold Start Recommender
    """
    def __init__(self, db, max_rating=5, log_level=logging.DEBUG):
        # Logger initialization
        self.logger = logging.getLogger("csrc")
        self.logger.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        self.logger.addHandler(ch)
        self.logger.debug("============ Logger initialized ================")

        # initialization of datastore attribute
        self.db = db

        # registering callback functions for datastore events
        self.db.register(self.db.insert_or_update_item, self.on_insert_or_update_item)
        self.db.register(self.db.remove_item, self.on_remove_item)
        self.db.register(self.db.insert_or_update_item_rating, self.on_insert_or_update_item_rating)
        self.db.register(self.db.remove_item_rating, self.on_remove_item_rating)
        self.db.register(self.db.reconcile_user, self.on_reconcile_user)
        self.db.register(self.db.serialize, self.on_serialize)
        self.db.register(self.db.restore, self.on_restore)

        # Algorithm's specific attributes
        self._items_cooccurrence = pd.DataFrame  # cooccurrence of items
        self.cooccurrence_updated = 0.0
        self.info_used = set() # Info used in addition to item_id. Only for in-memory testing, otherwise there is utils collection in the MongoDB
        self.item_info = [] #any information given with item, e.g. ['author', 'category', 'subcategory']
        self.only_info = False #not used yet
        self.max_rating = max_rating
        self._items_cooccurrence = pd.DataFrame  # cooccurrence of items
        self._categories_cooccurrence = {} # cooccurrence of categories

        # categories --same as above, but separated as they are not always available
        self.tot_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.n_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # number of ratings  (inmemory testing)
        self.items_by_popularity = []
        self.items_by_popularity_updated = 0.0  # Time of update

        self.last_serialization_time = 0.0 # Time of data backup

    def on_insert_or_update_item(self, item_id, attributes, return_value):
        #TODO: to implement
        pass

    def on_remove_item(self, item_id):
        #TODO: to implement
        pass

    def on_insert_or_update_item_rating(self, user_id, item_id, rating=3.0, return_value=None):
        """
        item is treated as item_id if it is not a dict, otherwise we look
        for a key called item_id_key if it is a dict.

        self.item_info can be any further information given with the dict item.
        e.g. author, category etc

        NB NO DOTS IN user_id, or they will be taken away. Fields in mongodb cannot have dots..

        If self.only_info==True, only the self.item_info's are put in the co-occurrence, not item_id.
         This is necessary when we have for instance a "segmentation page" where we propose
         well known items to get to know the user. If s/he select "Harry Potter" we only want
         to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        :param user_id: id of user. NO DOTS, or they will taken away. Fields in mongodb cannot have dots.
        :param item: is either id or a dict with item_id_key
        :param rating: float parseable
        :return: None
        """
        if not return_value:  # do nothing if the insert fail
            return

        if not self.item_info:
            self.item_info = []

        # If self.only_info==True, only the self.item_info's are put in the co-occurrence, not item_id.
        # This is necessary when we have for instance a "segmentation page" where we propose
        # well known items to get to know the user. If s/he select "Harry Potter" we only want
        # to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        # Now fill the dicts or the db collections if available
        user_id = str(user_id).replace('.', '')

        item = self.db.get_item(item_id)
        if item:
            # Do categories only if the item is stored
            if len(self.item_info) > 0:
                for k, v in item.items():
                    if k in self.item_info:
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
                        self.info_used.add(k)
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
                                self.tot_categories_user_ratings[k][user_id][value] += int(rating)
                                self.n_categories_user_ratings[k][user_id][value] += 1
                                # for the co-occurrence matrix is not necessary to do the same for item, but better do it
                                # in case we want to compute similarities etc using categories
                                self.tot_categories_item_ratings[k][value][user_id] += int(rating)

    def on_remove_item_rating(self, user_id, item_id, return_value):
        #TODO: to implement
        pass

    def on_reconcile_user(self, old_user_id, new_user_id, return_value):
        #TODO: to implement
        pass

    def on_serialize(self, filepath, return_value):
        if return_value:
            self.last_serialization_time = time()
        else:
            self.logger.error("[on_serialize] data backup failed on file %s, last successful backup at: %f" %
                              (filepath,
                               self.last_serialization_time))

    def on_restore(self, filepath, return_value):
        if not return_value:
            self.logger.error("[on_restore] restore from serialized data fail: ", filepath)

        self._create_cooccurrence()
        r_it = self.db.get_item_ratings_iterator()
        for item in r_it:
            user_id = item[0]
            ratings = item[1]
            for item_id, rating in ratings.items():
                self.on_insert_or_update_item_rating(user_id=user_id, item_id=item_id, rating=rating, return_value=True)

    def _create_cooccurrence(self):
        """
        Create or update the co-occurrence matrix
        :return:
        """
        all_ratings = self.db.get_all_item_ratings()
        df = pd.DataFrame(all_ratings).fillna(0).astype(int)  # convert dictionary to pandas dataframe

        #calculate co-occurrence matrix
        # sometime will print the warning: "RuntimeWarning: invalid value encountered in true_divide"
        # use np.seterr(divide='ignore', invalid='ignore') to suppress this warning
        df_items = (df / df).replace(np.inf, 0).replace(np.nan,0) #calculate co-occurrence matrix and normalize to 1
        co_occurrence = df_items.fillna(0).dot(df_items.T)
        self._items_cooccurrence = co_occurrence

        #update co-occurrence matrix for items categories
        df_tot_cat_item = {}

        if len(self.info_used) > 0:

            for i in self.info_used:
                df_tot_cat_item[i] = pd.DataFrame(self.tot_categories_item_ratings[i]).fillna(0).astype(int)

            for i in self.info_used:
                if type(df_tot_cat_item.get(i)) == pd.DataFrame:
                    df_tot_cat_item[i] = (df_tot_cat_item[i] / df_tot_cat_item[i]).replace(np.inf, 0)
                    self._categories_cooccurrence[i] = df_tot_cat_item[i].T.dot(df_tot_cat_item[i])

        self.cooccurrence_updated = time()

    def compute_items_by_popularity(self, max_items=10, fast=False):
        """
        As per name, get self.
        :return: list of popular items, 0=most popular
        """
        if fast and (time() - self.items_by_popularity_updated) < 1800:
            return self.items_by_popularity
        else:
            self.items_by_popularity_updated = time()

        df_item = pd.DataFrame(self.db.get_all_item_ratings()).T.fillna(0).astype(int).sum()
        df_item.sort(ascending=False)
        pop_items = list(df_item.index)
        if len(pop_items) >= max_items:
            self.items_by_popularity = pop_items
        else:
            all_items = set(self.db.get_all_items().keys())
            self.items_by_popularity = pop_items + list( all_items - set(pop_items) )

    def set_item_info(self, item_info):
        self.item_info = self.info_used

    def get_item_info(self):
        return self.item_info

    def set_only_info(self, only_info):
        self.only_info=only_info

    def get_only_info(self, only_info):
        return self.only_info

    def get_recommendations(self, user_id, max_recs=50, fast=False, algorithm='item_based'):
        """
        algorithm item_based:
            - Compute recommendation to user using item co-occurrence matrix (if the user
            rated any item...)
            - If there are less than max_recs recommendations, the remaining
            items are given according to popularity. Scores for the popular ones
            are given as score[last recommended]*index[last recommended]/n
            where n is the position in the list.
            - Recommended items above receive a further score according to categories
        :param user_id: the user id as in the mongo collection 'users'
        :param max_recs: number of recommended items to be returned
        :param fast: Compute the co-occurrence matrix only if it is one hour old or
                     if matrix and user vector have different dimension
        :return: list of recommended items
        """
        user_id = str(user_id).replace('.', '')
        df_tot_cat_user = {}
        df_n_cat_user = {}
        rec = pd.Series()
        item_based = False  # has user rated some items?
        info_based = []  # user has rated the category (e.g. the category "author" etc)
        df_user = None
        if self.db.get_item_ratings(user_id):  # compute item-based rec only if user has rated smt
            item_based = True
            #Just take user_id for the user vector
            df_user = pd.DataFrame(self.db.get_all_item_ratings()).fillna(0).astype(int)[[user_id]]
        info_used = self.info_used
        if len(info_used) > 0:
            for i in info_used:
                if self.tot_categories_user_ratings[i].get(user_id):
                    info_based.append(i)
                    df_tot_cat_user[i] = pd.DataFrame(self.tot_categories_user_ratings[i]).fillna(0).astype(int)[[user_id]]
                    df_n_cat_user[i] = pd.DataFrame(self.n_categories_user_ratings[i]).fillna(0).astype(int)[[user_id]]

        if item_based:
            try:
                # this might fail for fast in case a user has rated an item
                # but the co-occurrence matrix has not been updated
                # therefore the matrix and the user-vector have different
                # dimension
                if not fast or (time() - self.cooccurrence_updated > 1800):
                    self._create_cooccurrence()
#                self.logger.debug("[get_recommendations] Trying cooccurrence dot df_user")
#                self.logger.debug("[get_recommendations] _items_cooccurrence: %s", self._items_cooccurrence)
#                self.logger.debug("[get_recommendations] df_user: %s", df_user)
                rec = self._items_cooccurrence.T.dot(df_user[user_id])
#                self.logger.debug("[get_recommendations] Rec: %s", rec)
            except:
                self.logger.debug("[get_recommendations] 1st rec production failed, calling _create_cooccurrence.")
                try:
                    self._create_cooccurrence()
                    rec = self._items_cooccurrence.T.dot(df_user[user_id])
#                    self.logger.debug("[get_recommendations] Rec: %s", rec)
                except:
                    self.logger.warning("[get_recommendations] user_ and item_ratings seem not synced")
                    self._create_cooccurrence()
                    rec = self._items_cooccurrence.T.dot(df_user[user_id])
                    self.logger.debug("[get_recommendations] Rec: %s", rec)

            # Add to rec items according to popularity
            rec.sort(ascending=False)

            if len(rec) < max_recs:
                self.compute_items_by_popularity(fast=fast)
                for v in self.items_by_popularity:
                    if len(rec) == max_recs:
                        break
                    elif v not in rec.index:
                        n = len(rec)
                        rec.set_value(v, rec.values[n - 1]*n/(n+1.))  # supposing score goes down according to Zipf distribution
        else:
            self.compute_items_by_popularity(fast=fast)
            for i, v in enumerate(self.items_by_popularity):
                if len(rec) == max_recs:
                    break
                rec.set_value(v, self.max_rating / (i+1.))  # As comment above, starting from max_rating
#        self.logger.debug("[get_recommendations] Rec after item_based or not: %s", rec)

        # Now, the worse case we have rec=popular with score starting from max_rating
        # and going down as 1/i (this is item_based == False)

        global_rec = rec.copy()
        if len(info_used) > 0:
            cat_rec = {}
            for cat in info_based:
                user_vec = df_tot_cat_user[cat][user_id] / df_n_cat_user[cat][user_id].replace(0, 1)
                # print "DEBUG get_recommendations. user_vec:\n", user_vec
                try:
                    cat_rec[cat] = self._categories_cooccurrence[cat].T.dot(user_vec)
                    cat_rec[cat].sort(ascending=False)
                    #self.logger.debug("[get_recommendations] cat_rec (try):\n %s", cat_rec)
                except:
                    self._create_cooccurrence()
                    cat_rec[cat] = self._categories_cooccurrence[cat].T.dot(user_vec)
                    cat_rec[cat].sort(ascending=False)
                    #self.logger.debug("[get_recommendations] cat_rec (except):\n %s", cat_rec)
                for k, v in rec.iteritems():
                    #self.logger.debug("[get_recommendations] rec_item_id: %s", k)
                    try:
                        item_info_value = self.items[k][cat]

                        #self.logger.debug("DEBUG get_recommendations. item value for %s: %s", cat, item_info_value)
                        # In case the info value is not in cat_rec (as it can obviously happen
                        # because a rec'd item coming from most popular can have the value of
                        # an info (author etc) which is not in the rec'd info
                        if item_info_value:
                            global_rec[k] = v + cat_rec.get(cat, []).get(item_info_value, 0)
                    except Exception, e:
                        self.logger.error("item %s, category %s", k, cat)
                        logging.exception(e)
        global_rec.sort(ascending=False)
#        self.logger.debug("[get_recommendations] global_rec:\n %s", global_rec)

        if item_based:
            # If the user has rated all items, return an empty list
            rated = df_user[user_id] != 0
#            self.logger.debug("Rated: %s", rated)
            return [i for i in global_rec.index if not rated.get(i, False)][:max_recs]
        else:
            try:
                recomms = list(global_rec.index)[:max_recs]
            except:
                return None
            return recomms
