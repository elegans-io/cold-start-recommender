import json
from os.path import dirname

with open(dirname(__file__) + '/pkg_info.json') as fp:
    _info = json.load(fp)

__version__ = _info['version']
__author__ = _info['author']
__license__ = _info['license']
__maintainer__ = _info['maintainer']
__email__ = _info['email']

from csrec.factory_dal import Dal
from csrec.recommender import Recommender
