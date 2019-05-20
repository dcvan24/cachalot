"""
LRU implementation

Reference:
[1] Silberschatz, Abraham, James L. Peterson, and Peter B. Galvin. Operating system concepts.
Addison-Wesley Longman Publishing Co., Inc., 1991.

"""
from cache.base import CacheBase, CacheEntry
from collections import OrderedDict


class LRUCache(CacheBase):

    def __init__(self, env, capacity):
        super(LRUCache, self).__init__(env, capacity)
        self._store = OrderedDict()

    def insert(self, key, item):
        if item.size > self.capacity:
            self.logger.debug('Max: {}, input: {}'.format(self.space_left, item.size))
            return
        ce = CacheEntry(self._env.now, key, item)
        if key in self._store:
            # Update a cache entry if it is already in cache.
            # Since the new value may have a different size from the old one, the cache
            # evicts the old one and then insert the new one to complete the update
            self.logger.debug('{} is found in cache, update'.format(key))
            self.evict(key)
        while self.space_left < item.size:
            self.logger.debug('Left: {}, input: {}'.format(self.space_left, item.size))
            _, ce_rm = self._store.popitem(False)
            yield self._storage.put(ce_rm.value.size)
        self._store[key] = ce
        yield self._storage.get(ce.value.size)

    def evict(self, key):
        if key not in self._store:
            self.logger.debug('{} is not found in cache'.format(key))
            return
        ce_rm = self._store[key]
        del self._store[key]
        yield self._storage.put(ce_rm.value.size)

    def retrieve(self, key):
        if key not in self._store:
            self.logger.debug('{} is not found in cache'.format(key))
            return None
        ce = self._store[key]
        ce.access(self._env.now)
        del self._store[key]
        self._store[key] = ce
        return ce.value
