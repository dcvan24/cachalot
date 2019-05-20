"""
Deprecated classes for testing purpose

"""
from cache.base import CacheBase, CacheEntry, CacheItem


class LRUQueue(object):

    def __init__(self):
        self._nodes = set()
        self._front = None
        self._rear = None

    def enqueue(self, node):
        """

        :param node:
        :return:
        """
        assert isinstance(node, LRUQueueNode)
        node.next = self._front
        if len(self) == 0:
            self._front = self._rear = node
        else:
            self._front.prev = node
            self._front = node
        self._nodes.add(node)

    def dequeue(self, node=None):
        """

        :return: dequeued node
        """
        if len(self) == 0:
            return
        node_rm = self._rear if node is None else node
        assert isinstance(node_rm, LRUQueueNode)
        if node_rm not in self._nodes:
            return
        if node_rm.prev:
            node_rm.prev.next = node_rm.next
        if node_rm.next:
            node_rm.next.prev = node_rm.prev
        if node_rm == self._front:
            self._front = node_rm.next
        if node_rm == self._rear:
            self._rear = node_rm.prev
        node_rm.prev = node_rm.next = None
        self._nodes.remove(node_rm)
        return node_rm

    def move_to_front(self, node):
        """

        :param node:
        :return:
        """
        assert isinstance(node, LRUQueueNode)
        if node not in self._nodes or node == self._front:
            return
        self.enqueue(self.dequeue(node))

    def __len__(self):
        return len(self._nodes)

    def __contains__(self, node):
        assert isinstance(node, LRUQueueNode)
        return node in self._nodes


class LRUQueueNode(object):

    def __init__(self):
        self._prev = None
        self._next = None

    @property
    def prev(self):
        return self._prev

    @property
    def next(self):
        return self._next

    @prev.setter
    def prev(self, node):
        assert node is None or isinstance(node, LRUQueueNode)
        self._prev = node

    @next.setter
    def next(self, node):
        assert node is None or isinstance(node, LRUQueueNode)
        self._next = node


class DeprecatedLRUCache(CacheBase):

    def __init__(self, env, capacity):
        CacheBase.__init__(self, env, capacity)
        self._queue = LRUQueue()

    def insert(self, key, item):
        assert isinstance(item, CacheItem)
        self.logger.info('Input size: {}, capacity available: {}'.format(
            item.size, self.space_left))
        if item.size > self.capacity:
            return
        ce = LRUCacheEntry(self._env.now, key, item)
        if key in self._store:
            # Update a cache entry if it is already in cache.
            # Since the new value may have a different size from the old one, the cache
            # evicts the old one and then insert the new one to complete the update
            self.logger.info('{} is found in cache, update'.format(key))
            self.evict(key)
        while self.space_left < ce.value.size:
            self.logger.info('Capacity available: {}, requested: {}'.format(
                self.space_left, ce.value.size))
            ce_rm = self._queue.dequeue()
            del self._store[ce_rm.key]
            yield self._storage.put(ce_rm.value.size)
        self._queue.enqueue(ce)
        self._store[key] = ce
        yield self._storage.get(ce.value.size)

    def evict(self, key):
        if key not in self._store:
            self.logger.info('{} is not found in cache'.format(key))
            return
        ce_rm = self._store[key]
        self._queue.dequeue(ce_rm)
        del self._store[key]
        yield self._storage.put(ce_rm.key.size)

    def retrieve(self, key):
        if key not in self._store:
            self.logger.info('{} is not found in cache'.format(key))
            return None
        ce = self._store[key]
        ce.access(self._env.now)
        self._queue.move_to_front(ce)
        return ce.value


class LRUCacheEntry(CacheEntry, LRUQueueNode):

    def __init__(self, timestamp, key, value):
        CacheEntry.__init__(self, timestamp, key, value)
        LRUQueueNode.__init__(self)
