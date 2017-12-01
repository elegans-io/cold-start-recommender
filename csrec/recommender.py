import pandas as pd
import numpy as np
from time import time
import logging
from csrec.tools.singleton import Singleton
from csrec import factory_dal

class Recommender(Singleton):
    """
    Cold Start Recommender
    """
    def __init__(self, dal_name='mem', dal_params={}, max_rating=5, log_level=logging.INFO):
        # Logger initialization
        self.logger = logging.getLogger("csrc")
        self.logger.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        self.logger.addHandler(ch)
        self.logger.debug("============ Logger initialized ================")

        # initialization of datastore attribute
        self.db = factory_dal.Dal.get_dal(name=dal_name, **dal_params)  # instantiate an in memory database

        # registering callback functions for datastore events
        self.db.register(self.db.serialize, self.on_serialize)
        self.db.register(self.db.restore, self.on_restore)

        # Algorithm's specific attributes
        self._items_cooccurrence = pd.DataFrame  # cooccurrence of items
        self.cooccurrence_updated = 0.0
        # Info in item_meaningful_info with whom some user has actually interacted
        self._categories_cooccurrence = {}  # cooccurrence of categories

        # categories --same as above, but separated as they are not always available
        self.items_by_popularity = []  # can be recomputed on_restore
        self.last_serialization_time = 0.0  # Time of data backup
        # configurations:
        self.max_rating = max_rating

    def on_serialize(self, filepath, return_value):
        if return_value is None or return_value:
            self.last_serialization_time = time()
        else:
            self.logger.error("[on_serialize] data backup failed on file %s, last successful backup at: %f" %
                              (filepath,
                               self.last_serialization_time))

    def on_restore(self, filepath, return_value):
        if return_value is not None and not return_value:
            self.logger.error("[on_restore] restore from serialized data fail: ", filepath)
        else:
            self._create_cooccurrence()

    def _create_cooccurrence(self):
        """
        Create or update the co-occurrence matrix
        :return:
        """
        all_ratings = self.db.get_item_actions()
        df = pd.DataFrame(all_ratings).fillna(0).astype(int)  # convert dictionary to pandas dataframe

        # calculate co-occurrence matrix
        # sometime will print the warning: "RuntimeWarning: invalid value encountered in true_divide"
        # use np.seterr(divide='ignore', invalid='ignore') to suppress this warning
        df_items = (df / df).replace(np.inf, 0).replace(np.nan, 0)  # calculate co-occurrence matrix and normalize to 1
        co_occurrence = df_items.fillna(0).dot(df_items.T)
        self._items_cooccurrence = co_occurrence

        # update co-occurrence matrix for items categories
        df_n_cat_item = {}

        info_used = self.db.get_info_used()
        if len(info_used) > 0:
            n_categories_item_ratings = self.db.get_n_categories_item_ratings()
            for i in info_used:
                df_n_cat_item[i] = pd.DataFrame(n_categories_item_ratings.get(i)).fillna(0).astype(int)

            for i in info_used:
                if type(df_n_cat_item.get(i)) == pd.DataFrame:
                    df_n_cat_item[i] = (df_n_cat_item[i] / df_n_cat_item[i]).replace(np.nan, 0)
                    self._categories_cooccurrence[i] = df_n_cat_item[i].T.dot(df_n_cat_item[i])

        self.cooccurrence_updated = time()

    def compute_items_by_popularity(self):
        """
        As per name, get self.items_by_popularity
        :return: None
        """
        df_item = pd.DataFrame(self.db.get_item_actions()).T.fillna(0).astype(int).sum()
        df_item.sort_values(inplace=True, ascending=False)
        pop_items = list(df_item.index)
        all_items = set(self.db.get_items().keys())
        self.items_by_popularity = (pop_items + list(all_items - set(pop_items)))

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
        :param user_id: the user id as in the collection of 'users'
        :param max_recs: number of recommended items to be returned
        :param fast: Compute the co-occurrence matrix only if it is one hour old or
                     if matrix and user vector have different dimension
        :return: list of recommended items
        """
        user_id = str(user_id).replace('.', '')
        df_tot_cat_user = {}
        df_n_cat_user = {}
        rec = pd.Series()
        user_has_rated_items = False  # has user rated some items?
        rated_infos = []  # user has rated the category (e.g. the category "author" etc)
        df_user = None
        if self.db.get_item_actions(user_id=user_id):  # compute item-based rec only if user has rated smt
            user_has_rated_items = True
            # Just take user_id for the user vector
            df_user = pd.DataFrame(self.db.get_item_actions()).fillna(0).astype(int)[[user_id]]
        info_used = self.db.get_info_used()
        if len(info_used) > 0:
            tot_categories_user_ratings = self.db.get_tot_categories_user_ratings()
            n_categories_user_ratings = self.db.get_n_categories_user_ratings()
            for i in info_used:
                if i in tot_categories_user_ratings and user_id in tot_categories_user_ratings[i]:
                    rated_infos.append(i)
                    df_tot_cat_user[i] =\
                        pd.DataFrame(tot_categories_user_ratings.get(i)).fillna(0).astype(int)[[user_id]]
                    df_n_cat_user[i] =\
                        pd.DataFrame(n_categories_user_ratings.get(i)).fillna(0).astype(int)[[user_id]]

        if user_has_rated_items:
            if not fast or (time() - self.cooccurrence_updated > 1800):
                self._create_cooccurrence()
            try:
                # this might fail if the cooccurrence was not updated (fast)
                # and the user rated a new item.
                # In this case the matrix and the user-vector have different
                # dimension
                # print("DEBUG [get_recommendations] Trying cooccurrence dot df_user")
                # print("DEBUG [get_recommendations] _items_cooccurrence: %s", self._items_cooccurrence)
                # print("DEBUG [get_recommendations] df_user: %s", df_user)
                rec = self._items_cooccurrence.T.dot(df_user[user_id])
                # self.logger.debug("[get_recommendations] Rec worked: %s", rec)
            except:
                self.logger.debug("[get_recommendations] 1st rec production failed, calling _create_cooccurrence.")
                self._create_cooccurrence()
                rec = self._items_cooccurrence.T.dot(df_user[user_id])
                self.logger.debug("[get_recommendations] Rec: %s", rec)
            # Sort by cooccurrence * rating:
            rec.sort_values(inplace=True, ascending=False)

            # If necessary, add popular items
            if len(rec) < max_recs:
                if not fast or len(self.items_by_popularity) == 0:
                    self.compute_items_by_popularity()
                for v in self.items_by_popularity:
                    if len(rec) == max_recs:
                        break
                    elif v not in rec.index:
                        n = len(rec)
                        # supposing score goes down according to Zipf distribution
                        rec.set_value(v, rec.values[n - 1]*n/(n+1.))

        else:
            if not fast or len(self.items_by_popularity) == 0:
                self.compute_items_by_popularity()
            for i, v in enumerate(self.items_by_popularity):
                if len(rec) == max_recs:
                    break
                rec.set_value(v, self.max_rating / (i+1.))  # As comment above, starting from max_rating
#        print("DEBUG [get_recommendations] Rec after item_based or not: %s", rec)

        # Now, the worse case we have is the user has not rated, then rec=popular with score starting from max_rating
        # and going down as 1/i

        # User info on rated categories (in info_used)
        global_rec = rec.copy()
        if len(info_used) > 0:
            cat_rec = {}
            if not fast or (time() - self.cooccurrence_updated > 1800):
                self._create_cooccurrence()
            for cat in rated_infos:
                # get average rating on categories
                user_vec = df_tot_cat_user[cat][user_id] / df_n_cat_user[cat][user_id].replace(0, 1)
                # print("DEBUG [get_recommendations]. user_vec:\n", user_vec)
                try:
                    cat_rec[cat] = self._categories_cooccurrence[cat].T.dot(user_vec)
                    cat_rec[cat].sort_values(inplace=True, ascending=False)
                    #print("DEBUG [get_recommendations] cat_rec (try):\n %s", cat_rec)
                except:
                    self._create_cooccurrence()
                    cat_rec[cat] = self._categories_cooccurrence[cat].T.dot(user_vec)
                    cat_rec[cat].sort(ascending=False)
                    #print("DEBUG [get_recommendations] cat_rec (except):\n %s", cat_rec)
                for item_id, score in rec.items():
                    #print("DEBUG [get_recommendations] rec_item_id: %s", k)
                    try:
                        item = self.db.get_items(item_id=item_id)
                        item_info_value = item.get(cat, "")

                        #print("DEBUG get_recommendations. item value for %s: %s", cat, item_info_value)
                        # In case the info value is not in cat_rec (as it can obviously happen
                        # because a rec'd item coming from most popular can have the value of
                        # an info (author etc) which is not in the rec'd info
                        if item_info_value:
                            global_rec[item_id] = score + cat_rec.get(cat, {}).get(item_info_value, 0)
                    except Exception as e:
                        self.logger.error("item %s, category %s", item_id, cat)
                        logging.exception(e)
        global_rec.sort_values(inplace=True, ascending=False)
#        print("DEBUG [get_recommendations] global_rec:\n %s", global_rec)

        if user_has_rated_items:
            # If the user has rated all items, return an empty list
            rated = df_user[user_id] != 0
            # rated.get is correct (pycharm complains, knows no pandas)
            items = [i for i in global_rec.index if not rated.get(i, False)]
            if items:
                return items[:max_recs]
            else:
                return []
        else:
            try:
                return list(global_rec.index)[:max_recs]
            except:
                return None
