# database abstraction layer factory

from csrec.exceptions import *


class Dal:
    @staticmethod
    def get_implemented_dal():
        """
        get the implemented dal
        :return: a set of implemented dal
        """

        implemented_dal = {
            'mem'
        }
        return implemented_dal

    @staticmethod
    def get_dal_supported_parameters(name):
        """
        get a list of supported parameters by a dal implementation and their description

        :param name: the name of any DAL implementation i.e. mem
        :return: the dictionary with the supported parameters and the description
        """

        if name == 'mem':  # in memory implementation of dal
            import csrec.mem_dal as mem_dal
            return mem_dal.Database.get_init_parameters_description()
        else:
            raise NotImplementedError

    @staticmethod
    def get_dal(name, **params):
        """
        database abstraction layer factory

        exception: raise a InitializationError if any error occur

        :param name: the name of any DAL implementation i.e. mem
        :param params: the dictionary of the parameters supported by the implementation. Refer to the
            help of the init process of the specific instance for the supported parameters.
        :return: an instance of the DAL implementation if the initialization was successfully executed
        """

        if name == 'mem':  # in memory implementation of dal
            import csrec.mem_dal as mem_dal
            try:
                dal_instance = mem_dal.Database()
            except InitializationException as exc:
                raise InitializationException("unable to initialize: " % name)
        else:
            raise NotImplementedError

        dal_instance.init(**params)

        return dal_instance
