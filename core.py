import sys
import logging
import numpy as np

from tools.basic import sha1_encode, lazy_property


class GlobalConfig(object):

    @staticmethod
    def set_random_seed(seed):
        np.random.seed(seed)


class Loggable(object):

    @lazy_property
    def logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            hdlr = logging.StreamHandler(sys.stdout)
            hdlr.setFormatter(logging.Formatter(
                '%(asctime)s|%(levelname)s|%(process)d|%(name)s.%(funcName)s::%(lineno)s'
                '\t%(message)s'))
            logger.addHandler(hdlr)
        return logger


class Runnable(object):

    def __init__(self, env):
        self._env = env
        self._is_alive = True

    @property
    def is_alive(self):
        return self._is_alive

    def terminate(self):
        self._is_alive = False


class BaseObject(object):

    def __init__(self, *args):
        self._id = sha1_encode(*args)

    @property
    def id(self):
        return repr(hex(self._id))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return self._id

from json import JSONEncoder


def _default(self, obj):
    return getattr(obj.__class__, '__json__', _default.default)(obj)

_default.default = JSONEncoder().default  # Save unmodified default.
JSONEncoder.default = _default # replacement
