__author__ = 'mario'

from abc import ABCMeta, abstractmethod
import logging

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
        :return:
        """


class DataBase():
    """
    Give access to the data
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def add_relationship(self, info_id, entity_id, weight=1.):
        """
        Add an existing relationship between info_id and entity_id
        (e.g. user rated 5 an item)

        :param info_id: id
        :param entity_id: id
        :param weight: a number
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

