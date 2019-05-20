import numpy as np

from numbers import Number
from collections import namedtuple

from core import Loggable
from app.base import Data, Operation, Task
from tools.basic import lazy_property
from tools.dist import ProbabilityDistribution, parse_dist
from tools.func import parse_function


LoadConfig = namedtuple('LoadConfig',
                        'n_tasks, n_data, size_dist, size_dist_params,'
                        'n_ops, op_exec_func, op_exec_func_params,'
                        'op_exec_time_func, op_exec_time_func_params,'
                        'op_output_size_func, op_output_size_func_params,'
                        'data_access_dist, data_access_dist_params,'
                        'op_access_dist, op_access_dist_params')


class Load(Loggable):

    def __init__(self, cfg, random_state=None):
        self._cfg = cfg
        self._random_state = random_state
        self._tasks = self._init_task_generator(self._cfg).tasks

    @property
    def tasks(self):
        return self._tasks

    @property
    def random_state(self):
        return self._random_state

    @random_state.setter
    def random_state(self, random_state):
        assert random_state is None or isinstance(random_state, np.random.RandomState)
        self._random_state = random_state
        self._tasks = self._init_task_generator(self._cfg).tasks

    @lazy_property
    def total_output_size(self):
        return sum([t.output.size for t in set(self._tasks)])

    @lazy_property
    def n_unique_tasks(self):
        return len(set(self._tasks))

    def _init_task_generator(self, cfg):
        return TaskGenerator(**cfg._asdict(),
                             random_state=self._random_state)


class LoadGenerator(Loggable):

    def __init__(self, n, random_state):
        super(LoadGenerator, self).__init__()
        self._n = n
        self._random_state = random_state

    @property
    def n(self):
        return self._n

    @property
    def random_state(self):
        return self._random_state

    @random_state.setter
    def random_state(self, random_state):
        assert random_state is None or isinstance(random_state, np.random.RandomState)
        self._random_state = random_state

    def generate(self):
        raise NotImplemented


class DataGenerator(LoadGenerator):

    def __init__(self, n, size_dist, dist_params, random_state=None):
        super(DataGenerator, self).__init__(n, random_state)
        dist_params = dict(dist_params)
        dist_params.update(dict(random_state=random_state))
        self._size_dist = parse_dist(size_dist)
        self._dist_params = dist_params
        dist = self._size_dist(**dist_params)
        self._data = [Data(i + 1, dist.generate() + 1) for i in range(n)]
        # print(sum(d.size for d in self._data))

    @property
    def data(self):
        return list(self._data)

    @property
    def size_dist(self):
        return self._size_dist

    @property
    def dist_params(self):
        return self._dist_params


class OperationGenerator(LoadGenerator):

    def __init__(self, n, exec_func, exec_func_params,
                 exec_time_func, exec_time_func_params,
                 output_size_func, output_size_func_params,
                 random_state=None):
        super(OperationGenerator, self).__init__(n, random_state)
        self._exec_funcs = self._create_funcs(n, exec_func, exec_func_params)
        self._exec_time_funcs = self._create_funcs(n, exec_time_func,
                                                   exec_time_func_params)
        self._output_size_funcs = self._create_funcs(n, output_size_func,
                                                     output_size_func_params)
        self._ops = [Operation(i + 1, self._exec_funcs[i], self._exec_time_funcs[i],
                               self._output_size_funcs[i])
                     for i in range(n)]

    @property
    def ops(self):
        return list(self._ops)

    @property
    def exec_funcs(self):
        return list(self._exec_funcs)

    @property
    def exec_time_funcs(self):
        return list(self._exec_time_funcs)

    @property
    def output_size_funcs(self):
        return list(self._output_size_funcs)

    def _create_funcs(self, n, func_name, params):
        func = parse_function(func_name)
        for k in dict(params):
            if isinstance(params[k], Number):
                params[k] = [params[k]] * 2
            elif len(params[k]) < 2:
                params[k] = [params[k][0]] * 2
        funcs = [func(**{k : self._random_state.uniform(*params[k]) for k in params})
                 for _ in range(n)]
        return funcs


class TaskGenerator(LoadGenerator):

    def __init__(self, n_tasks, n_data, size_dist, size_dist_params,
                 n_ops, op_exec_func, op_exec_func_params,
                 op_exec_time_func, op_exec_time_func_params,
                 op_output_size_func, op_output_size_func_params,
                 data_access_dist, data_access_dist_params,
                 op_access_dist, op_access_dist_params,
                 random_state=None):
        super(TaskGenerator, self).__init__(n_tasks, random_state)
        self._data_gen = DataGenerator(n_data, size_dist, size_dist_params, random_state)
        self._op_gen = OperationGenerator(n_ops, op_exec_func, op_exec_func_params,
                                          op_exec_time_func, op_exec_time_func_params,
                                          op_output_size_func, op_output_size_func_params,
                                          random_state)
        self._data_access_dist = parse_dist(data_access_dist)
        self._data_access_dist_params = data_access_dist_params
        self._op_access_dist = parse_dist(op_access_dist)
        self._op_access_dist_params = op_access_dist_params
        self._tasks = self._init_tasks()

    @property
    def tasks(self):
        return self._tasks

    @property
    def n_data(self):
        return self._data_gen.n

    @property
    def n_ops(self):
        return self._op_gen.n

    def _init_tasks(self):
        data, ops = self._data_gen.data, self._op_gen.ops
        da_params = dict(self._data_access_dist_params)
        da_params.update(dict(random_state=self._random_state))
        oa_params = dict(self._op_access_dist_params)
        oa_params.update(dict(random_state=self._random_state))
        data_access_dist = self._data_access_dist(**da_params)
        op_access_dist = self._op_access_dist(**oa_params)
        tasks, instances = [], {}
        for _ in range(self._n):
            data_idx = int(data_access_dist.generate())%len(data)
            op_idx = int(op_access_dist.generate())%len(ops)
            if (data_idx, op_idx) not in instances:
                instances[data_idx, op_idx] = Task(ops[op_idx], data[data_idx])
            tasks.append(instances[data_idx, op_idx])
        return tasks
