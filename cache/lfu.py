from collections import OrderedDict


class FrequencyQueue(object):

    def __init__(self):
        self._items = {}
        self._front = None
        self._rear = None

    def get(self, key):
        return self._items[key] if key in self._items else None

    def enqueue(self, key, ce):
        if len(self._items) == 0:
            self._front = self._rear = FrequencyQueueNode(0)
            self._front.add_item(key, ce)
        elif key in self._items:
            node = self._items[key].parent
            if node.next is None or node.next.freq - node.freq > 1:
                new_node = FrequencyQueueNode(node.freq + 1)
                new_node.next = node.next
                new_node.prev = node
                if node.next is not None:
                    node.next.prev = new_node
                node.next = new_node
            new_node = node.next
            node.del_item(key)
            if len(node) == 0:
                self._remove_node(node)
            new_node.add_item(key, ce)
        else:
            if self._front.freq > 0:
                new_node = FrequencyQueueNode(0)
                new_node.prev = self._front.prev
                new_node.next = self._front
                if self._front.prev is not None:
                    self._front.prev.next = new_node
                self._front.prev = new_node
                self._front = new_node
            self._front.add_item(key, ce)
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
        del self._items[key]
        if len(node) == 0:
            self._remove_node(node)
        return ce

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

    def __contains__(self, key):
        return key in self._items


class FrequencyQueueNode(object):

    def __init__(self, freq):
        self._freq = freq
        self._prev = None
        self._next = None
        self._items = OrderedDict()

    def add_item(self, key, ce):
        self._items[key] = ce
        ce.parent = self

    def del_item(self, key):
        if key not in self._items:
            return
        del self._items[key]

    @property
    def freq(self):
        return self._freq

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
        assert prev is None or isinstance(prev, FrequencyQueueNode)
        self._prev = prev

    @next.setter
    def next(self, next):
        assert next is None or isinstance(next, FrequencyQueueNode)
        self._next = next

    def __len__(self):
        return len(self._items)
