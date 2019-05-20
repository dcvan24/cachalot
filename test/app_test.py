import simpy
import time
import unittest

import matplotlib.pyplot as plt

from unittest import TestCase
from collections import Counter

from core import Loggable, GlobalConfig
from infrastructure.base import Cluster
from app.base import *
from cache.lru import *
from cache.lfu import *
from tools.func import *
from tools.dist import *
from app.load import *

from test.mock import MockLRUCache, MockLFUCache
from test.deprecated import DeprecatedLRUCache


def plot_freq(items, show_labels=True, xtick_int=10):
    labels, values = zip(*Counter(items).items())
    idx, width = np.arange(len(values)), .5
    plt.bar(idx, sorted(values, reverse=True), width)
    if show_labels:
        plt.xticks(np.arange(0, len(values), xtick_int), labels, rotation='vertical')
    plt.show()


class BaseTest(TestCase, Loggable):

    def setUp(self):
        self._num_items = 5
        self._env = simpy.Environment()
        self._cluster = Cluster(self._env)
        self._prepare()

    def _prepare(self):
        key_dist = ZipfDistribution(1000, a=1.1)
        data_size_dist = ZipfDistribution(1000, a=1.1, step=64)

        op_access_dist = ZipfDistribution(1000, a=1.1)

        GlobalConfig.set_random_seed(123)
        exec_func = LinearFunction()
        exec_func.set_params_range(1, 4)
        exec_func.randomize_params()

        exec_time_func = LinearFunction(2, 3)
        output_size_func = LinearFunction(1, 3)

        self._data_gen = DataGenerator(1000, key_dist, data_size_dist)
        self._op_gen = OperationGenerator(1000, op_access_dist, exec_func,
                                          exec_time_func, output_size_func)
        self._task_gen = TaskGenerator(10 ** 5, self._op_gen, self._data_gen)
        self._cache_capacity = 10000

    def test_task_dist(self):
        for scale_factor in range(1):
            self._task_gen.n = 10 ** (5 + scale_factor)
            tasks = []
            for t in self._task_gen.generate():
                self._env.process(self._cluster._run_task(t))
                self._env.run()
                tasks.append(t)
            task_ids = [t.id for t in tasks]
            plot_freq(task_ids, False)

            output_sizes = [round(t.output.size, 2) for t in tasks]
            plot_freq(output_sizes, True, xtick_int=50)

    def test_hit_rate_with_varying_capacity(self):
        def run_tasks(cache, hit_rates):
            num_hits = 0
            for t in self._task_gen.generate():
                res = cache.retrieve(t.id)
                if res:
                    num_hits += 1
                else:
                    yield self._env.process(self._cluster._run_task(t))
                    cache_item = TaskCacheItem(t)
                    yield self._env.process(cache.insert(t.id, cache_item))
            hit_rates.append(num_hits * 1./self._task_gen.n)

        results = {}
        for cache_type in (LRUCache, LFUCache):
            for scale in range(3, 4):
                capacity = 10 ** (scale + 2)
                self.logger.info('Capacity: {}, cache type: {}'.format(
                    cache_type.__name__, capacity))
                cache = cache_type(self._env, capacity)
                hit_rates = results.setdefault(cache_type.__name__, [])
                self._env.process(run_tasks(cache, hit_rates))
                self._env.run()
                self.logger.info('Hit rate: {}'.format(hit_rates[-1]))

        for cache_type, hit_rates in results.items():
            plt.plot(np.arange(len(hit_rates)), hit_rates, label=cache_type)
        plt.legend()
        plt.show()

    def test_hit_rate_with_varying_tasks(self):
        def run_tasks(cache, hit_rates, n_tasks):
            num_hits = 0
            self._task_gen.n = n_tasks
            for t in self._task_gen.generate():
                res = cache.retrieve(t.id)
                if res:
                    num_hits += 1
                else:
                    yield self._env.process(self._cluster._run_task(t))
                    cache_item = TaskCacheItem(t)
                    yield self._env.process(cache.insert(t.id, cache_item))
            hit_rates.append(num_hits * 1./n_tasks)

        results = {}
        for cache_type in (LRUCache, LFUCache):
            for scale in range(5):
                n_tasks = 10 ** (scale + 2)
                self.logger.info('Capacity: {}, # of tasks: {}'.format(
                    cache_type.__name__, n_tasks))
                cache = cache_type(self._env, 1000)
                hit_rates = results.setdefault(cache_type.__name__, [])
                self._env.process(run_tasks(cache, hit_rates, n_tasks))
                self._env.run()
                self.logger.info('Hit rate: {}'.format(hit_rates[-1]))

        for cache_type, hit_rates in results.items():
            self.logger.info('{}: {}'.format(cache_type, repr(hit_rates)))
            plt.plot(np.arange(len(hit_rates)), hit_rates, label=cache_type)
        plt.legend()
        plt.show()

    def test_lru_cache_integration(self):
        test = self._validate_cache_behavior(LRUCache(self._env, self._cache_capacity),
                                             MockLRUCache(self._cache_capacity))
        self._run_test(test)

    def test_lfu_cache_integration(self):
        test = self._validate_cache_behavior(LFUCache(self._env, self._cache_capacity),
                                             MockLFUCache(self._cache_capacity))
        self._run_test(test)

    @unittest.skip
    def test_lru_cache_performance(self):
        self._run_test(self._test_lru_cache_performance())

    def _test_lru_cache_performance(self):
        cluster = self._cluster
        results = {}
        for _ in range(20):
            for cache in (LRUCache(self._env, self._cache_capacity),
                          DeprecatedLRUCache(self._env, self._cache_capacity)):
                start_time = time.time()
                num_hits = 0
                for i, t in enumerate(self._task_gen.generate()):
                    res = cache.retrieve(t.id)
                    if not res:
                        yield self._env.process(cluster._run_task(t))
                        cache_item = TaskCacheItem(t)
                        yield self._env.process(cache.insert(t.id, cache_item))
                    else:
                        num_hits += 1
                stats = results.setdefault(cache.__class__.__name__, {})
                stats.setdefault('runtime', []).append(time.time() - start_time)
                stats.setdefault('hit_rate', []).append(num_hits * 1./self._task_gen.n)

        for cache_name in results:
            runtime = results[cache_name]['runtime']
            hit_rate = results[cache_name]['hit_rate']
            self.logger.info('{}\tavg: {}, stdev: {}, avgHitRate: {}'.format(
                cache_name, np.mean(runtime), np.std(runtime), np.mean(hit_rate)))

    def _run_test(self, test):
        self._env.process(test)
        self._env.run()

    def _validate_cache_behavior(self, real_cache, mock_cache):
        cluster = self._cluster
        total_saving, num_hits = 0, 0
        for t in self._task_gen.generate():
            res = real_cache.retrieve(t.id)
            self.logger.info('Task id: {}'.format(t.id))
            self.assertEqual(res, mock_cache.retrieve(t.id))
            if res:
                num_hits += 1
                self.logger.info('Cache hit on {}'.format(t.id))
                total_saving += res.exec_time
                self.logger.info('Saved {} time units'.format(res.exec_time))
            else:
                self.logger.info('Cache miss on {}'.format(t.id))
                yield self._env.process(cluster._run_task(t))
                cache_item = TaskCacheItem(t)
                self.logger.info('{} requested, {} left'.format(
                    cache_item.size, real_cache.space_left))
                yield self._env.process(real_cache.insert(t.id, cache_item))
                mock_cache.insert(t.id, cache_item)
                self.logger.info('Inserted into cache, {} left'.format(
                    cache_item.size, real_cache.space_left))
        hit_rate = num_hits * 1./self._task_gen.n
        self.logger.info('Saved {} time units in total'.format(total_saving))
        self.logger.info('Cache hit rate: {}'.format(hit_rate))


class ZipDistTest(TestCase, Loggable):

    @unittest.skip
    def test_zipf_dist_size(self):
        for scale_factor in range(2, 7):
            dist = ZipfDistribution(10 ** scale_factor)
            values = [s for s in dist.generate()]
            plot_freq(values, True)

    @unittest.skip
    def test_zipf_dist_shape(self):
        """
        Increasing shape value results in shorter tails. Popular items will occur more
        frequently with the larger shape value

        """
        for shape in range(2, 5):
            dist = ZipfDistribution(10 ** 6, a=shape)
            plot_freq([d for d in dist.generate()], True)







