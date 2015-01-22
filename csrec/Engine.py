__author__ = 'mario'

from abc import ABCMeta, abstractmethod
import logging
import json

class Dimension():
    """
    Different kinds of nodes in a multi-partite graph.
    At the moment just to have type checking
    """
    def __init__(self, name, kind_of_dimension):
        assert kind_of_dimension in ['info', 'entity']
        self.name = name
        self.kind = kind_of_dimension

    def category_table_name(self, category, relationship_type, prefix=None):
        """
        category as in "author" for books. eg:
        tot_author_rated is going to be the table with total ratings of authors
        n_author_rated is going to be the table with number of ratings of authors
        """
        if not prefix:
            return self.name + '_' + str(category) + '_' + relationship_type
        else:
            return str(prefix) + '_' + self.name + '_' + str(category) + '_' + relationship_type


class Engine():
    """
    Engine behind the recommender.

    We approach the recommendation problem as bipartite graph problem, where
    the two sets (dimensions) are called *info* and *entities*.

    Contrary to purely bipartite, there might be relationship between nodes
    of the sub-graph (e.g. user follow user).

    Info-nodes provide information about the entity, to which they are related.

    For instance, if entities are book-shop's customers, info can be books, but
    also authors, publishers etc.

    What the recommender actually needs is an engine which accept info/entities'
    values and returns new relationships. It does that in two ways:

        * Find new possible relationships:
            1. info -> recommended entity
            2. entity -> recommended info

        * Collapse one dimension of the graph
            1. entity -> similar entity
            2. info -> similar info

    """

    def __init__(self, data_object, log_level=logging.DEBUG):
        try:
            assert (DataBase in type(data_object).__bases__)
        except:
            raise Exception('data_class must be a DataBase')

        self.logger = logging.getLogger("engine")
        self.logger.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        self.logger.addHandler(ch)
        self.logger.debug("============ Creating Engine Instance ================")


    def reduce_graph(self, dimension, algorithm):
        """
        Using the favourite algorithm, reduce the graph (e.g. cooccurrence of items
        weighted with log-likelihood ratio).

        Calls different

        :param dimension in ['info', 'entity']
        :return: nothing, but updates/creates the similarity matrix (e.g. cooccurrence, llr etc)
        """
        pass


    def _reduce_graph_pandas_cooccurrence(self):
        """
        Take data_object, build a pandas.dataframe and compute cooccurrence
        :return:
        """
        pass


    def _reduce_graph_pandas_loglikelihood(self):
        """
        Take data_object, build a pandas.dataframe and compute llr
        :return:
        """
        pass


    def rank_nodes(self, dimension, algorithm):
        """
        Rank nodes according to some algorithm (eg popularity for info
        or centrality for users)
        :param dimension:
        :param algorithm: [popularity, page_rank]
        :return:
        """
        pass

    def rank_nodes_by_popularity(self):
        """
        Rank nodes according to the number of connections
        """


class DataBase():
    """
    Give access to the data
    """
    __metaclass__ = ABCMeta

    # def __init__(self, info_name, entity_name):
    #     self.info = Dimension(info_name, 'info')
    #     self.entity = Dimension(entity_name, 'entity')

    @abstractmethod
    def add_relationship(self, info_id, entity_id, relationship_type, weight=1.,
                         info_categories = None, only_categories=None):
        """
        Add an existing relationship between info_id and entity_id
        (e.g. user rated 5 an item)

        :param info_id: id
        :param entity_id: id
        :param relationship_type: rating, etc
        :param weight: a number
        :param info_categories: keys in the info dict that should be taken in account (e.g. author for book etc)
        :param only_categories: T/F take only the keys above, not the _id
        :return:
        """
        pass


    @abstractmethod
    def insert_node(self, dimension, node_dict, _id="_id"):
        """
        item_dict is
        :param item_dict:
        :param _id:
        :return:
        """
        pass


    @abstractmethod
    def collapse_nodes(self, dimension, id_old, id_new):
        """
        All relationship of id_old are moved to id_new.

            . If id_old does not exist does nothing (warning)
            . If id_new does not exist just rename

        :param dimension:  in ['info', 'entity']
        :param id_old
        :param id_new
        :return: nothing, update the similarity matrix
        """
        pass



class DataMongo(DataBase):
    """
    DataBase in MongoDB
    """
    def __init__(self, info_name, entity_name,
                 mongo_host, mongo_db_name, mongo_replica_set=None,
                 log_level=logging.DEBUG):
        self.info = Dimension(info_name, 'info')
        self.entity = Dimension(entity_name, 'entity')

        self.logger.debug("============ Host: %s", str(mongo_host))
        if mongo_replica_set is not None:
            self.logger.debug("============ Replica: %s", str(mongo_replica_set))

        if mongo_replica_set is not None:
            from pymongo import MongoReplicaSetClient
            self.mongo_host = mongo_host
            self.mongo_replica_set = mongo_replica_set
            self.mongo_db_name = mongo_db_name
            self.mongo_client = MongoReplicaSetClient(self.mongo_host,
                                                      replicaSet=self.mongo_replica_set)
            self.db = self.mongo_client[self.mongo_db_name]
            # reading --for producing recommendations-- could be even out of sync.
            # this can be added if most replicas are in-memory
            # self.db.read_preference = ReadPreference.SECONDARY_PREFERRED
        else:
            from pymongo import MongoClient
            self.mongo_host = mongo_host
            self.mongo_replica_set = mongo_replica_set
            self.mongo_db_name = mongo_db_name
            self.mongo_client = MongoClient(self.mongo_host)
            self.db = self.mongo_client[self.mongo_db_name]
        #TBD SHOULD NOT...
        # # If these tables do not exist, it might create problems
        # if not self.db['user_ratings'].find_one():
        #     self.db['user_ratings'].insert({})
        # if not self.db['item_ratings'].find_one():
        #     self.db['item_ratings'].insert({})


    def insert_node(self, dimension, node_dict, _id="_id"):
        """
        item_dict is
        :param item_dict:
        :param _id:
        :return:
        """
        assert type(dimension) == Dimension
        for k, v in node_dict.items():
            if k is not "_id":
                self.db[dimension.name].update({"_id": node_dict[_id]},
                                        {"$set": {k: v}},
                                        upsert=True)


    def add_relationship(self, info_id, entity_id, relationship_type, weight=1.,
                         info_categories = None, only_categories=None):
        """
        Add an existing relationship between info_id and entity_id
        (e.g. user rated 5 an item)

        :param info_id: id
        :param entity_id: id
        :param relationship_type: rating, etc
        :param weight: a number
        :param info_categories: keys in the info dict that should be taken in account (e.g. author for book etc)
        :param only_categories: T/F take only the keys above, not the _id
        :return:
        """
        if not info_categories:
            info_categories = []
        info_id = str(info_id).replace('.', '')
        entity_id = str(entity_id).replace('.', '')

        # If the item is not stored, we don't have its categories
        # Therefore do categories only if the item is found stored
        info_item = self.db[self.info.name].find_one({"_id": info_id})
        if info_item:
            if len(info_categories) > 0:
                self.logger.debug('[add_relationship] Looking for the following category: %s', info_categories)
                for k, v in info_item.items():
                    if k in info_categories and v is not None:  # sometimes the value IS None
                        self.logger.debug("[add_relationship] Adding %s to info_categories and create relative collections",
                                          k)
                        self.db['utils'].update({"_id": 1},
                                                {"$addToSet": {'info_categories': k}},
                                                upsert=True)

                        # Some items' attributes are lists (e.g. tags: [])
                        try:
                            v = json.loads(v.replace("'", '"'))
                        except:
                            pass
                        if not hasattr(v, '__iter__'):
                            values = [str(v)]
                        else:
                            values = [str(i) for i in v]  # It's going to be a key, no numbers

                        # see comments above

                        entity_tot_name = self.entity.category_table_name(category=k, relationship_type=relationship_type, prefix='tot')
                        entity_n_name = self.entity.category_table_name(category=k, relationship_type=relationship_type, prefix='n')
                        info_tot_name = self.info.category_table_name(category=k, relationship_type=relationship_type, prefix='tot')
                        info_n_name = self.info.category_table_name(category=k, relationship_type=relationship_type, prefix='n')

                        for value in values:
                            if len(value) > 0:
                                self.db[entity_tot_name].update({'_id': entity_id},
                                                                         {'$inc': {value: float(weight)}},
                                                                          upsert=True)
                                self.db[entity_n_name].update({'_id': entity_id},
                                               {'$inc': {value: 1}},
                                                upsert=True)
                                self.db[info_tot_name].update({'_id': value},
                                               {'$inc': {entity_id: float(weight)}},
                                                upsert=True)
                                self.db[info_n_name].update({'_id': value},
                                               {'$inc': {entity_id: 1}},
                                                upsert=True)
                                self.db[self.info.name].update(
                                    {"_id": info_id},
                                    {"$set": {k: value}},
                                    upsert=True
                                )
        else:
            self.insert_node(self.info, {"_id": info_id})  # Obviously there won't be categories...

        if not only_categories:
            self.db[self.entity.category_table_name(category=k, relationship_type=relationship_type)].update(
                {"_id": entity_id},
                {"$set": {info_id: float(weight)}},
                upsert=True
            )
            self.db[self.info.category_table_name(category=k, relationship_type=relationship_type)].update(
                {"_id": info_id},
                {"$set": {entity_id: float(weight)}},
                upsert=True
            )


    def collapse_nodes(self, dimension, id_old, id_new):
        """
        All relationship of id_old are moved to id_new.

            . If id_old does not exist does nothing (warning)
            . If id_new does not exist just rename

        :param dimension:  in ['info', 'entity']
        :param id_old
        :param id_new
        :return: nothing, update the similarity matrix
        """
        pass



