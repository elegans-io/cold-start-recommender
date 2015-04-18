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
    def __init__(self, default_rating=3, max_rating=5, log_level=logging.DEBUG):
        # Loggin stuff
        self.logger = logging.getLogger("csrc")
        self.logger.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        self.logger.addHandler(ch)
        self.logger.debug("============ Creating a Recommender Instance ================")

        self.info_used = set() # Info used in addition to item_id. Only for in-memory testing, otherwise there is utils collection in the MongoDB
        self.default_rating = default_rating  # Rating inserted by default
        self.max_rating = max_rating
        self._items_cooccurrence = pd.DataFrame  # cooccurrence of items
        self._categories_cooccurrence = {} # cooccurrence of categories
        self.cooccurrence_updated = 0.0  # Time of update
        self.item_ratings = defaultdict(dict)  # matrix of ratings for a item (inmemory testing)
        self.user_ratings = defaultdict(dict)  # matrix of ratings for a user (inmemory testing)
        self.items = defaultdict(dict)  # matrix of item's information {item_id: {"Author": "AA. VV."....}
        self.item_id_key = 'id'

        # categories --same as above, but separated as they are not always available
        self.tot_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.n_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # number of ratings  (inmemory testing)
        self.n_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.items_by_popularity = []
        self.items_by_popularity_updated = 0.0  # Time of update

    def _coll_name(self, k, typ):
        """
        e.g. user_author_ratings
        """
        return str(typ) + '_' + str(k) + '_ratings'


    def _create_cooccurrence(self):
        """
        Create or update the co-occurrence matrix
        :return:
        """
        df_tot_cat_item = {}

        # Items' vectors
        df_item = pd.DataFrame(self.item_ratings).fillna(0).astype(int)
        # Categories' vectors
        info_used = self.info_used
        if len(info_used) > 0:
            for i in info_used:
                df_tot_cat_item[i] = pd.DataFrame(self.tot_categories_item_ratings[i]).fillna(0).astype(int)

        df_item = (df_item / df_item).replace(np.inf, 0)  # normalize to one to build the co-occurrence
        self._items_cooccurrence = df_item.T.dot(df_item)
        if len(info_used) > 0:
            for i in info_used:
                if type(df_tot_cat_item.get(i)) == pd.DataFrame:
                    df_tot_cat_item[i] = (df_tot_cat_item[i] / df_tot_cat_item[i]).replace(np.inf, 0)
                    self._categories_cooccurrence[i] = df_tot_cat_item[i].T.dot(df_tot_cat_item[i])
        self.cooccurrence_updated = time()


    def _sync_user_item_ratings(self):
        """
        It might happen that the user_ratings and the item_ratings
        are not aligned. It shouldn't, but with users can be profiled,
        then reconciled with session_id etc, it happened...
        :return:
        """
        #Doing that only for the mongodb case..
        self.logger.warning("[_sync_user_item_ratings] Syncronyzing item_ratings with user_ratings data")

    def insert_item(self, item, _id="_id"):
        """
        Insert the whole document either in self.items or in db.items.
        self.items is a nested dict {_id: dict(item), ....}
        :param item: {_id: item_id, cat1: ...} or {item_id_key: item_id, cat1: ....}
        :return: None
        """
        self.item_id_key = _id
        self.items[item[_id]] = item


    def reconcile_ids(self, id_old, id_new):
        """
        Create id_new if not there, add data of id_old into id_new.
        Compute the co-occurrence matrix.
        NB id_old is removed!
        :param id_new:
        :param id_old:
        :return: None
        """
        id_new = str(id_new).replace(".", "")
        id_old = str(id_old).replace(".", "")

        # user-item
        for key, value in self.user_ratings[id_old].items():
            self.user_ratings[id_new][key] = self.user_ratings[id_old][key]
        self.user_ratings.pop(id_old)

        for k, v in self.item_ratings.items():
            if v.has_key(id_old):
                v[id_new] = v.pop(id_old)

        # user-categories
        if len(self.info_used) > 0:
            for i in self.info_used:
                for key, value in self.tot_categories_user_ratings[i][id_old].items():
                    self.tot_categories_user_ratings[i][id_new][key] = self.tot_categories_user_ratings[i][id_old][key]
                self.tot_categories_user_ratings[i].pop(id_old)

                for k, v in self.tot_categories_item_ratings[i].items():
                    if v.has_key(id_old):
                        v[id_new] = v.pop(id_old)

                for key, value in self.n_categories_user_ratings[i][id_old].items():
                    self.n_categories_user_ratings[i][id_new][key] = self.n_categories_user_ratings[i][id_old][key]
                self.n_categories_user_ratings[i].pop(id_old)

                for k, v in self.n_categories_item_ratings[i].items():
                    if v.has_key(id_old):
                        v[id_new] = v.pop(id_old)

        self._create_cooccurrence()


    def compute_items_by_popularity(self, max_items=10, fast=False):
        """
        As per name, get self.
        :return: list of popular items, 0=most popular
        """
        if fast and (time() - self.items_by_popularity_updated) < 1800:
            return self.items_by_popularity
        else:
            self.items_by_popularity_updated = time()

        df_item = pd.DataFrame(self.item_ratings).fillna(0).astype(int).sum()

        df_item.sort(ascending=False)
        pop_items = list(df_item.index)
        if len(pop_items) >= max_items:
            self.items_by_popularity = pop_items
        else:
            all_items = set(self.items.keys())
            self.items_by_popularity = pop_items + list( all_items - set(pop_items) )


    def get_similar_item(self, item_id, user_id=None, algorithm='simple'):
        """
        TODO
        Simple: return the row of the co-occurrence matrix ordered by score or,
        if user_id is not None, multiplied times the user_id rating
        (not transposed!) so to weigh the similarity score with the
        rating of the user
        :param item_id: Id of the item
        :param user_id: Id of the user
        :param algorithm: keep it simple...
        :return:
        """
        user_id = str(user_id).replace('.', '')
        pass


    def remove_rating(self, user_id, item_id):
        """
        Remove ratings from item and user. This cannot be undone for categories
        (only thing we could do is subtracting the average value from sum and n-1)
        :param user_id:
        :param item_id:
        :return:
        """
        user_id = str(user_id).replace('.', '')
        self.user_ratings[user_id].pop(item_id, None)
        self.item_ratings[item_id].pop(user_id, None)
        self.items[item_id] = {}  # just insert the bare id. quite useless because it is a defaultdict, but in case .keys() we can count the # of items

    def insert_item(self):
        pass

    def insert_rating(self, user_id, item_id, rating=3, item_info=None, only_info=False):
        """
        item is treated as item_id if it is not a dict, otherwise we look
        for a key called item_id_key if it is a dict.

        item_info can be any further information given with the dict item.
        e.g. author, category etc

        NB NO DOTS IN user_id, or they will taken away. Fields in mongodb cannot have dots..

        If only_info==True, only the item_info's are put in the co-occurrence, not item_id.
         This is necessary when we have for instance a "segmentation page" where we propose
         well known items to get to know the user. If s/he select "Harry Potter" we only want
         to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        :param user_id: id of user. NO DOTS, or they will taken away. Fields in mongodb cannot have dots.
        :param item: is either id or a dict with item_id_key
        :param rating: float parseable
        :param item_info: any info given with dict(item), e.g. ['author', 'category', 'subcategory']
        :param only_info: not used yet
        :return: [recommended item_id_values]
        """
        if not item_info:
            item_info = []
        # If only_info==True, only the item_info's are put in the co-occurrence, not item_id.
        # This is necessary when we have for instance a "segmentation page" where we propose
        # well known items to get to know the user. If s/he select "Harry Potter" we only want
        # to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        # Now fill the dicts or the Mongodb collections if available
        user_id = str(user_id).replace('.', '')

        if self.items.get(item_id):
            item = self.items.get(item_id)
            # Do categories only if the item is stored
            if len(item_info) > 0:
                for k,v in item.items():
                    if k in item_info:
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
                        # We could sum all the ratings and count the a result as "big rating".
                        # Reading N books of author A and rating them 5 would be the same as reading
                        # 5*N books of author B and rating them 1.
                        # Still:
                        # 1) we don't want ratings for category to skyrocket, so we have to take the average
                        # 2) if a user changes their idea on rating a book, it should not add up. Average
                        #   is not perfect, but close enough. Take total number of ratings and total rating
                        for value in values:
                            if len(str(value)) > 0:
                                self.tot_categories_user_ratings[k][user_id][value] += int(rating)
                                self.n_categories_user_ratings[k][user_id][value] += 1
                                # for the co-occurrence matrix is not necessary to do the same for item, but better do it
                                # in case we want to compute similarities etc using categories
                                self.tot_categories_item_ratings[k][value][user_id] += int(rating)
                                self.n_categories_item_ratings[k][value][user_id] += 1

        else:
            self.insert_item({"_id": item_id})
        # Do item always, at least is for categories profiling
        if not only_info:
            self.user_ratings[user_id][item_id] = float(rating)
            self.item_ratings[item_id][user_id] = float(rating)


    def get_recommendations_item_based():
        pass

    def get_recommendations_popularity_based():
        pass

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

        if self.user_ratings.get(user_id):  # compute item-based rec only if user has rated smt
            item_based = True
            #Just take user_id for the user vector
            df_user = pd.DataFrame(self.user_ratings).fillna(0).astype(int)[[user_id]]
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
                self.logger.debug("[get_recommendations] Trying cooccurrence dot df_user")
                self.logger.debug("[get_recommendations] _items_cooccurrence: %s", self._items_cooccurrence)
                self.logger.debug("[get_recommendations] df_user: %s", df_user)
                rec = self._items_cooccurrence.T.dot(df_user[user_id])
                self.logger.debug("[get_recommendations] Rec: %s", rec)
            except:
                self.logger.debug("[get_recommendations] 1st rec production failed, calling _create_cooccurrence.")
                try:
                    self._create_cooccurrence()
                    rec = self._items_cooccurrence.T.dot(df_user[user_id])
                    self.logger.debug("[get_recommendations] Rec: %s", rec)
                except:
                    self.logger.warning("[get_recommendations] user_ and item_ratings seem not synced")
                    self._sync_user_item_ratings()
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
        self.logger.debug("[get_recommendations] Rec after item_based or not: %s", rec)

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
        self.logger.debug("[get_recommendations] global_rec:\n %s", global_rec)

        if item_based:
            # If the user has rated all items, return an empty list
            rated = df_user[user_id] != 0
            self.logger.debug("Rated: %s", rated)
            return [i for i in global_rec.index if not rated.get(i, False)][:max_recs]
        else:
            return list(global_rec.index)[:max_recs]


    def get_user_info(self, user_id):
        """
        Return user's rated items: {'item1': 3, 'item3': 1...}
        :param user_id:
        :return:
        """
        return self.user_ratings[user_id]


    def get_items(self, n=10):
        """
        Return n items
        :param n: number of items
        :return:
        """
        result = []
        for k in self.items.keys():
            result.append(self.items[k])
            if len(result) > n:
                break
        return result


    def drop_db(self):
        """
        Drop the whole db, unsafe!
        Return list of collections
        :return:
        """
        pass
        #if self.db:
        #    self.mongo_client.drop_database(self.mongo_db_name)
        #    return self.db.collection_names()

