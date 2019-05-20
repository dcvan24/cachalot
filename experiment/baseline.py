import os
import time
import json
import datetime
import numpy as np
import multiprocessing as mp

from collections import Counter

from cache.lru import LRUCache
from cache.lfu import LFUCache
from infrastructure.base import Cluster
from experiment.runner import ParallelExperimentRunner
from app.load import LoadConfig


def count_and_sort(items):
    counter = sorted(Counter(items).items(), key=lambda x: x[1], reverse=True)
    return zip(*counter)


class LFULRUComparison(ParallelExperimentRunner):

    def __init__(self, run_dir, cfg):
        super(LFULRUComparison, self).__init__(run_dir, cfg)
        mgr = mp.Manager()
        self._results = mgr.dict()

    def main(self, id, env, load):
        # json.dump(load.stat, open('%s/stat.json'%run_dir, 'w'), indent=2)
        local_res = {}
        n_unique_tasks, n_tasks = load.n_unique_tasks, len(load.tasks)
        perfect_hit_rate = 1 - n_unique_tasks/n_tasks
        scale = int(np.log10(n_tasks)) + 1
        for capacity_factor in reversed(range(scale)):
            capacity = int(n_tasks * 10 ** (-capacity_factor))
            for cache_type in (LRUCache, LFUCache):
                print('Type: {}, capacity: {}'.format(cache_type.__name__, capacity))
                cache = cache_type(env, capacity)
                hit_rate = yield env.process(self._send_request(env, load, cache))
                norm_hit_rate = hit_rate/perfect_hit_rate
                print('Hit rate: {}'.format(norm_hit_rate))
                local_res.setdefault(cache_type.__name__, []).append(norm_hit_rate)
            if capacity * 10 > load.total_output_size:
                break
        self._results[id] = local_res

    def on_completion(self):
        json.dump(self._results.copy(), open('%s/results.json'%self._run_dir, 'w'),
                  indent=2)

    @staticmethod
    def _send_request(env, load, cache):
        cluster = Cluster('cluster', env, None)
        n_hits = 0
        for t in load.tasks:
            res = cache.retrieve(t.id)
            if res is None:
                yield env.process(cluster.run_task(t))
                yield env.process(cache.insert(t.id, t.output))
            else:
                n_hits += 1
        return n_hits/len(load.tasks)


def run_experiment():
    cfg = LoadConfig(n_tasks=10 ** 6,
                     n_data=10 ** 4, size_dist='uniform',
                     size_dist_params=dict(lo=1, hi=1), n_ops=1, op_dist='uniform',
                     op_dist_params=dict(lo=1, hi=1), op_exec_func='linear',
                     op_exec_func_params=dict(w=0, b=0), op_exec_time_func='linear',
                     op_exec_time_func_params=dict(w=0, b=1), op_output_size_func='linear',
                     op_output_size_func_params=dict(w=0, b=1), data_access_dist='power',
                     data_access_dist_params=dict(a=.8, n_ranks=10 ** 4),
                     op_access_dist='uniform', op_access_dist_params=dict(lo=1, hi=1))

    run_dir = 'runs/%d'%int(time.mktime(datetime.datetime.now().timetuple()))
    os.makedirs(run_dir)
    LFULRUComparison(run_dir, cfg).run(60)


if '__main__' == __name__:
    run_experiment()











