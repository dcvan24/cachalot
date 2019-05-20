import bisect as bi

from collections import OrderedDict


class LRVQueueNode(object):

    def __init__(self, val):
        self._val = val
        self._prev = None
        self._next = None
        self._items = OrderedDict()

    def add_item(self, key, ce):
        self._items[key] = ce
        ce.parent = self

    def del_item(self, key):
        item = self._items.pop(key, None)
        if item:
            item.parent = None

    @property
    def items(self):
        return self._items

    @property
    def val(self):
        return self._val

    @property
    def last_item(self):
        return list(self._items.items())[0][0]

    @property
    def prev(self):
        return self._prev

    @property
    def next(self):
        return self._next

    @prev.setter
    def prev(self, prev):
        assert prev is None or isinstance(prev, LRVQueueNode)
        self._prev = prev

    @next.setter
    def next(self, next):
        assert next is None or isinstance(next, LRVQueueNode)
        self._next = next

    def __len__(self):
        return len(self._items)


class LRVQueue(object):

    def __init__(self, func, inflate=False):
        self._func = func
        self._items = {}
        self._q_heads = {}
        self._values = []
        self._front = None
        self._rear = None
        self._inflate = inflate
        self._base = 0

    @property
    def base(self):
        return self._base

    @property
    def least_value(self):
        return self._front.val if self._front else 0

    @property
    def items(self):
        return dict(self._items)

    def get(self, key):
        return self._items[key] if key in self._items else None

    def enqueue(self, key, ce):
        val = self._func(ce)
        if key in self._items:
            node = self._items[key].parent
            # print('Old val: %f, new val: %f'%(node.val, val))
            node.del_item(key)
            if len(node) == 0:
                self._remove_node(node)
        new_node = self._q_heads[val] if val in self._q_heads else self._create_node(val)
        new_node.add_item(key, ce)
        self._items[key] = ce

    def dequeue(self, key=None):
        if self._front is None:
            return None
        key = key if key else self._front.last_item
        if key not in self._items:
            return None
        ce = self._items[key]
        node = ce.parent
        node.del_item(key)
        if len(node) == 0:
            self._remove_node(node)
        del self._items[key]
        if self._inflate:
            self._base = node.val
        return ce

    def _create_node(self, val):
        new_node = LRVQueueNode(val)
        if len(self._values) == 0:
            pos = 0
            self._front = self._rear = new_node
        elif val < self._values[0]:
            pos = 0
            next = self._q_heads[self._values[pos]]
            new_node.next = next
            next.prev = new_node
            self._front = new_node
        elif val > self._values[-1]:
            pos = len(self._values)
            prev = self._q_heads[self._values[-1]]
            prev.next = new_node
            new_node.prev = prev
            self._rear = new_node
        else:
            pos = bi.bisect(self._values, val)
            prev = self._q_heads[self._values[pos - 1]]
            next = self._q_heads[self._values[pos]]
            new_node.prev = prev
            new_node.next = next
            prev.next = new_node
            next.prev = new_node
        self._q_heads[val] = new_node
        self._values.insert(pos, val)
        return new_node

    def _remove_node(self, node):
        if node.prev is not None:
            node.prev.next = node.next
        if node.next is not None:
            node.next.prev = node.prev
        if node == self._front:
            self._front = node.next
        if node == self._rear:
            self._rear = node.prev
        node.prev = node.next = None
        pos = bi.bisect_left(self._values, node.val)
        if self._values[pos] == node.val:
            del self._values[pos]
        self._q_heads.pop(node.val, None)

    def __contains__(self, key):
        return key in self._items
