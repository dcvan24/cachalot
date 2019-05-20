from collections import OrderedDict
from cache.base import CacheItem, CacheEntry
from core import Loggable


class MockCache(Loggable):

    def __init__(self, capacity):
        super(MockCache, self).__init__()
        self._capacity = capacity
        self._used = 0

    @property
    def total_capacity(self):
        return self._capacity

    @property
    def capacity_available(self):
        return self._capacity - self._used

    def insert(self, key, item):
        raise NotImplemented

    def retrieve(self, key):
        raise NotImplemented
    

class MockLRUCache(MockCache):

    def __init__(self, capacity):
        super(MockLRUCache, self).__init__(capacity)
        self._store = OrderedDict()

    def insert(self, key, item):
        assert isinstance(item, CacheItem)
        if item.size > self.total_capacity:
            return
        while item.size > self.capacity_available:
            _, item_rm = self._store.popitem(last=False)
            self._used -= item_rm.size
        self._store[key] = item
        self._used += item.size

    def retrieve(self, key):
        item = self._store.get(key)
        if item:
            del self._store[key]
            self._store[key] = item
        return item


class MockLFUCache(MockCache, Loggable):

    def __init__(self, capacity):
        super(MockLFUCache, self).__init__(capacity)
        self._store = {}
        self._queues = {}

    def insert(self, key, item):
        if item.size > self.total_capacity:
            return
        ce = MockLFUCacheEntry(0, key, item)
        while item.size > self.capacity_available:
            least_freq = min(self._queues)
            queue = self._queues[least_freq]
            _, ce_rm = queue.popitem(last=False)
            del self._store[ce_rm.key]
            self._used -= ce_rm.value.size
            self.logger.debug('Removed: {}({}), capacity available: {}'.format(
                ce_rm.key, least_freq, self.capacity_available))
            if len(queue) == 0:
                del self._queues[least_freq]
        self._store[key] = ce
        self._queues.setdefault(0, OrderedDict())[key] = ce
        self._used += ce.value.size

    def retrieve(self, key):
        ce = self._store.get(key)
        if ce:
            old_queue = self._queues[ce.freq]
            del old_queue[key]
            if len(old_queue) == 0:
                del self._queues[ce.freq]
            ce.freq += 1
            self._queues.setdefault(ce.freq, OrderedDict())[key] = ce
        return ce.value if ce else None


class MockLFUCacheEntry(CacheEntry):

    def __init__(self, timestamp, key, item):
        super(MockLFUCacheEntry, self).__init__(timestamp, key, item)
        self.freq = 0

