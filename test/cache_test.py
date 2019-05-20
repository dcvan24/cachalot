import random
import unittest
import simpy
from unittest import TestCase
from core import Loggable
from cache.lru import *


class LRUQueueTest(TestCase, Loggable):

    def setUp(self):
        self._num_nodes = 5
        self._num_iters = 5

        self._q, self._nodes = self._prepare()

    def test_enqueue(self):
        q, nodes = self._q, self._nodes
        for n in nodes:
            self.assertTrue(n in q)
        self.assertEqual(len(q), len(nodes))
        self.assertEqual(nodes[-1], q._front)
        self.assertEqual(nodes[0], q._rear)

    def test_dequeue_last_node(self):
        q, nodes = self._q, self._nodes
        node_rm = nodes[0]
        q.dequeue()
        del nodes[0]
        self.assertEqual(len(q), len(nodes))
        self.assertFalse(node_rm in q)
        self._check_queue_two_way(q, nodes)

    def test_dequeue_specified_node(self):
        q, nodes = self._q, self._nodes
        for _ in range(self._num_iters):
            idx = random.randint(0, len(nodes) - 1)
            self.logger.info('Dequeued node index: {}'.format(idx))
            node_rm = nodes[idx]
            q.dequeue(node_rm)
            del nodes[idx]
            self.assertEqual(len(q), len(nodes))
            self.assertFalse(node_rm in q)
            self._check_queue_two_way(q, nodes)

    def test_move_random_to_front(self):
        q, nodes = self._q, self._nodes
        for _ in range(self._num_iters):
            idx = random.randint(0, len(nodes) - 1)
            self.logger.info('Moved node index: {}'.format(idx))
            node_mv = nodes[idx]
            if idx != len(nodes) - 1:
                nodes[idx: -1] = nodes[idx + 1:]
                nodes[-1] = node_mv
            q.move_to_front(node_mv)
            self._check_queue_two_way(q, nodes)

    def _prepare(self):
        q = LRUQueue()
        nodes = [LRUQueueNode() for _ in range(self._num_nodes)]
        for n in nodes:
            q.enqueue(n)
        return q, nodes

    def _check_queue_two_way(self, q, nodes):
        front = q._front
        rear = q._rear
        for i, n in enumerate(nodes):
            self.assertEqual(front, nodes[len(nodes) - i - 1])
            front = front.next
            self.assertEqual(rear, nodes[i])
            rear = rear.prev


class LRUCacheTest(TestCase, Loggable):

    def setUp(self):
        self._num_entries = 5
        self._capacity = 100
        self._cache, self._items = self._prepare()

    def test_insert(self):
        self._env.process(self._test_insert())
        self._env.run()

    def test_evict(self):
        self._env.process(self._test_evict())
        self._env.run()

    def test_retrieve(self):
        self._env.process(self._test_retrieve())
        self._env.run()

    def _prepare(self):
        num_entries = self._num_entries
        capacity = self._capacity
        self._env = simpy.Environment()
        cache = LRUCache(self._env, self._capacity)
        items = [CacheItem(i, (num_entries - i) * (capacity/10)) for i in range(num_entries)]
        return cache, items

    def _test_insert(self):
        env = self._env
        cache, items = self._cache, self._items
        expect_capacity_used, n_entries = 0, 0

        # Insert entries.
        # Capacity runs out at the third entry, therefore least recently used entries are
        # removed to make space for the new entry
        for i, e in enumerate(items):
            yield env.process(cache.insert(i, e))
            expect_capacity_used += e.size - items[0].size if i == 2 else e.size
            n_entries += 0 if i == 2 else 1
            self.logger.info('Capacity used - expect: {}, actual: {}'.format(
                expect_capacity_used, cache.space_used))
            self.assertEqual(cache.space_used, expect_capacity_used)
            self.assertEqual(cache.space_left,
                             self._capacity - expect_capacity_used)
            self.assertEqual(len(cache), n_entries)

        # insert duplicate entry, do nothing
        yield env.process(cache.insert(1, items[1]))
        self.logger.info('Capacity used - expect: {}, actual: {}'.format(
            expect_capacity_used, cache.space_used))
        self.assertEqual(len(cache), 4)
        self.assertEqual(cache.space_used, expect_capacity_used)

    def _test_evict(self):
        env = self._env
        cache, items = self._cache, self._items
        expect_capacity_used = 0
        for i in range(0, 2):
            yield env.process(cache.insert(i, items[i]))
            expect_capacity_used += items[i].size

        # evict a non-existent entry, do nothing
        yield env.process(cache.evict(3))
        self.logger.info('Capacity used - expect: {}, actual: {}'.format(
            expect_capacity_used, cache.space_used))
        self.assertEqual(len(cache), 2)
        self.assertEqual(cache.space_used, expect_capacity_used)

        # evict an existing entry
        yield env.process(cache.evict(1))
        expect_capacity_used -= items[1].size
        self.logger.info('Capacity used - expect: {}, actual: {}'.format(
            expect_capacity_used, cache.space_used))
        self.assertEqual(len(cache), 1)
        self.assertEqual(cache.space_used, expect_capacity_used)

    def _test_retrieve(self):
        env = self._env
        cache, items = self._cache, self._items
        actual_capacity_used = 0
        for i in range(0, 2):
            yield env.process(cache.insert(i, items[i]))
            actual_capacity_used += items[i].size

        # retrieve a non-existing entry
        self.assertEqual(cache.retrieve(2), None)

        # retrieve the entry first inserted
        self.assertEqual(cache.retrieve(0), items[0])

        # insert another entry, the second-inserted entry will be evicted
        # instead of the first one
        yield env.process(cache.insert(2, items[2]))
        actual_capacity_used += items[2].size - items[1].size
        self.assertFalse(1 in cache)
        self.assertEqual(cache.space_used, actual_capacity_used)


if __name__ == '__main__':
    unittest.main()



