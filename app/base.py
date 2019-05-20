from numbers import Number
from collections import namedtuple
from enum import Enum

from core import BaseObject
from tools.func import Function


class MessageType(Enum):

    TASK_EXEC_REQ = 'TASK_EXEC_REQ'
    TASK_EXEC_RESP = 'TASK_EXEC_RESP'
    CACHE_INSERT_REQ = 'CACHE_INSERT_REQ'
    CACHE_INSERT_RESP = 'CACHE_INSERT_RESP'
    CACHE_RETRIEVE_REQ = 'CACHE_RETRIEVE_REQ'
    CACHE_RETRIEVE_RESP = 'CACHE_RETRIEVE_RESP'
    CACHE_INSERT_ACK = 'CACHE_INSERT_ACK'
    CACHE_UDPATE_REQ = 'CACHE_UPDATE_REQ'
    CACHE_REMOVE_REQ = 'CACHE_REMOVE_REQ'
    CACHE_METADATA_REQ = 'CACHE_METADATA_REQ'
    CACHE_METADATA_RESP = 'CACHE_METADATA_RESP'
    CACHE_WL_ADD_REQ = 'CACHE_WL_ADD_REQ'
    CACHE_WL_ADD_RESP = 'CACHE_WL_ADD_RESP'
    CACHE_WL_REQUESTER_REQ = 'CACHE_WL_REQUESTER_REQ'
    CACHE_WL_REQUESTER_RESP = 'CACHE_WL_REQUESTER_RESP'
    CACHE_WL_REQUEST_REQ = 'CACHE_WL_REQUEST_REQ'
    CACHE_WL_REQUEST_RESP = 'CACHE_WL_REQUEST_RESP'
    CACHE_WL_CLEAR_REQ = 'CACHE_WL_CLEAR_REQ'
    CACHE_WL_CLEAR_RESP = 'CACHE_WL_CLEAR_RESP'
    CACHE_TRANSFER_REQ = 'CACHE_TRANSFER_REQ'
    CACHE_TRANSFER_RESP = 'CACHE_TRANSFER_RESP'
    CACHE_TRANSFER_DECLINE = 'CACHE_TRANSFER_DECLINE'
    CACHE_REDIRECT = 'CACHE_REDIRECT'
    MASTER_INSERT_REQ = 'MASTER_INSERT_REQ'


Message = namedtuple('Message', 'timestamp subject src dst type size data trace')
Subject = namedtuple('Subject', 'timestamp src dst data')


class Data(BaseObject):
    """
    Data

    """

    def __init__(self, key, size):
        """

        :param key: data key
        :param size: data size
        """
        assert isinstance(size, Number)
        super(Data, self).__init__(key)
        self._key = key
        self._size = size

    @property
    def key(self):
        return self._key

    @property
    def size(self):
        return self._size

    def __repr__(self):
        return repr((self.key, self._size))


class Operation(BaseObject):
    """
    Operation

    """

    def __init__(self, name, exec_func, exec_time_func, output_size_func):
        """

        :param name: unique name of the operation
        :param exec_func: function executed by this operation on input data
        :param exec_time_func: function that estimates operation runtime based
                             on input data size
        :param output_size_func

        """
        assert isinstance(exec_func, Function)
        assert isinstance(exec_time_func, Function)
        assert isinstance(output_size_func, Function)
        super(Operation, self).__init__(name)
        self._name = name
        self._exec_func = exec_func
        self._exec_time_func = exec_time_func
        self._output_size_func = output_size_func

    @property
    def name(self):
        return self._name

    @property
    def exec_func(self):
        return self._exec_func

    @property
    def exec_time_func(self):
        return self._exec_time_func

    @property
    def output_size_func(self):
        return self._output_size_func

    def operate_on(self, data):
        """

        :param data: input data
        :return: Task instance
        """
        return Task(self, data)

    def __repr__(self):
        return repr(self._name)


class Task(BaseObject):
    """

    """

    def __init__(self, operation, input_data):
        """

        :param operation:
        :param data:
        """
        assert isinstance(operation, Operation)
        assert isinstance(input_data, Data)
        super(Task, self).__init__(operation, input_data)
        self._operation = operation
        self._input = input_data

    @property
    def operation(self):
        return self._operation

    @property
    def input(self):
        return self._input

    @property
    def output(self):
        return Data(self.operation.exec_func(self._input.key),
                    self.operation.output_size_func(self._input.size))

    @property
    def exec_time(self):
        return self._operation.exec_time_func(self._input.size)

    def __repr__(self):
        return repr((self._input.key, self._input.size, self._operation.name))
