import numpy as np
import bisect as bi

from collections import OrderedDict

from core import Loggable
from infrastructure.base import Node, Network, LoadGenerator, Cluster


class CacheClassCatalog(Loggable):

    def __init__(self, n_classes, network):
        super(CacheClassCatalog, self).__init__()
        self._n_classes = n_classes
        self._network = network
        self._entries = set()

    @property
    def n_classes(self):
        return self._n_classes

    def add_cache_entry(self, ce):
        self._entries.add(ce)

    def remove_cache_entry(self, ce):
        self._entries.discard(ce)

    def create_classes(self):
        util_vals = [ce.total_util for ce in self._entries]
        max_val, min_val = max(util_vals), min(util_vals)
        step = (max_val - min_val)/self._n_classes
        self.logger.info('min: {}, max: {}, step: {}'.format(min_val, max_val, step))
        bins = np.arange(min_val, max_val, step)
        classes = {}
        for ce in self._entries:
            idx = bi.bisect(bins, ce.total_util)
            if idx == 0:
                self.logger.warn('Invalid util value of cache entry {}: '
                                 '{}'.format(ce.id, ce.total_util))
                continue
            classes.setdefault(idx, CacheClass(idx)).add(ce)
        self.logger.info('Class distribution ({} in total):'.format(len(classes)))
        for c in classes:
            self.logger.info('\t class: {}, base: {}, '
                             '# of entries: {}'.format(c,
                                                       bins[c] if c < 100 else bins[c - 1],
                                                       len(classes[c])))
        return classes

    def __contains__(self, ce):
        return ce in self._entries


class CacheClass(object):

    def __init__(self, id):
        self._id = id
        self._entries = set()

    @property
    def id(self):
        return self._id

    @property
    def is_empty(self):
        return len(self._entries) == 0

    @property
    def size(self):
        size = {}
        for ce in self._entries:
            for site in ce.storage_sites:
                size.setdefault(site, 0)
                size[site] += ce.size
        return size

    @property
    def util(self):
        util = {}
        for ce in self._entries:
            for site in ce.util:
                util.setdefault(site, 0)
                util[site] += ce.util[site]
        return util

    def get_entries_by_storage_site(self, site):
        return [e for e in self._entries if site in e.storage_sites]

    def add(self, ce):
        self._entries.add(ce)

    def remove(self, ce):
        self._entries.discard(ce)

    def __len__(self):
        return len(self._entries)

    def __json__(self):
        return dict(id=self.id,
                    size=self.size,
                    util=self.util)


class CacheBase(Loggable):

    def __init__(self, env, capacity):
        """

        :param env:
        :param capacity: total capacity of the cache node in MB
        """
        Loggable.__init__(self)
        self._env = env
        self._capacity = capacity
        self._space_used = 0
        self._store = {}

    @property
    def capacity(self):
        return self._capacity

    @property
    def space_left(self):
        return self._capacity - self._space_used

    @property
    def space_used(self):
        return self._space_used

    def lookup(self, key):
        """

        :param key:
        :return:
        """
        return self._store.get(key)

    def insert(self, item):
        """

        :param item
        :return:
        """
        raise NotImplemented

    def retrieve(self, key):
        """

        :param key: cache entry ID
        :return:
        """
        raise NotImplemented

    def _occupy(self, ce):
        ce.add_storage_site(self._env.now, self._id)
        self._space_used += ce.size

    def _reclaim(self, ce):
        ce.remove_storage_site(self._env.now, self._id)
        self._space_used -= ce.size

    def __contains__(self, key):
        return key in self._store


class CacheItem(Loggable):

    def __init__(self, key, value):
        self._key = key
        self._value = value
        self._num_hits = {}
        self._storage_sites = set()
        self._last_access_time = {}
        self._avg_access_int = {}
        self._insert_log = []
        self._evict_log = []
        self._retrieve_log = []

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

    @property
    def size(self):
        return self._value.output.size

    @property
    def num_hits(self):
        return sum(self._num_hits.values())

    @property
    def last_access_time(self):
        return max(self._last_access_time.values())

    @property
    def access_sites(self):
        return list(self._num_hits.keys())

    @property
    def storage_sites(self):
        return list(self._storage_sites)

    @property
    def insert_log(self):
        return list(self._insert_log)

    @property
    def evict_log(self):
        return list(self._evict_log)

    @property
    def retrieve_log(self):
        return list(self._retrieve_log)

    @property
    def avg_access_int(self):
        return float(np.mean([i[0] for i in self._avg_access_int.values]))

    def add_storage_site(self, timestamp, site):
        self._insert_log.append((timestamp, site))
        self._storage_sites.add(site)

    def remove_storage_site(self, timestamp, site):
        self._evict_log.append((timestamp, site))
        self._storage_sites.discard(site)

    def get_num_hits(self, site):
        return self._num_hits.get(site, 0)

    def get_last_access_time(self, site):
        return self._last_access_time.get(site)

    def get_avg_access_int(self, site):
        access_int = self._avg_access_int.get(site)
        return access_int[0] if access_int else 0

    def access(self, timestamp, site):
        last_access_int = timestamp - self._last_access_time.get(site, 0)
        self._last_access_time[site] = timestamp
        self._num_hits.setdefault(site, 0)
        self._num_hits[site] += 1
        access_int = self._avg_access_int.setdefault(site, [0, 0])
        access_int[0] = (access_int[0] * access_int[1] + last_access_int)/(access_int[1] + 1)
        access_int[1] += 1
        self._retrieve_log.append((timestamp, site))
        self.logger.debug('Access distribution: {}'.format(self._num_hits))

    def copy(self):
        item_cp = CacheItem(self._key, self._value)
        item_cp._num_hits = dict(self._num_hits)
        item_cp._storage_sites = set(self._storage_sites)
        item_cp._last_access_time = dict(self._last_access_time)
        return item_cp

    def update(self, new_item, exclude=None):
        self._storage_sites = set(new_item.storage_sites)
        for s in new_item.access_sites:
            if s == exclude:
                continue
            self._num_hits[s] = new_item.get_num_hits(s)
            self._last_access_time[s] = new_item.get_last_access_time(s)


class CacheEntry(Loggable):

    def __init__(self, item, network=None):
        super(CacheEntry, self).__init__()
        self._item = item
        self._network = network
        self._parent = None

    @property
    def id(self):
        return self._item.key

    @property
    def item(self):
        return self._item

    @property
    def key(self):
        return self._item.key

    @property
    def value(self):
        return self._item.value

    @property
    def size(self):
        return self._item.size

    @property
    def num_hits(self):
        return self._item.num_hits

    @property
    def last_access_time(self):
        return self._item.last_access_time

    @property
    def access_sites(self):
        return self._item.access_sites

    @property
    def storage_sites(self):
        return self._item.storage_sites

    @property
    def avg_access_int(self):
        return self._item.avg_access_int

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        self._parent = parent

    def get_num_hits(self, site):
        return self._item.get_num_hits(site)

    def get_last_access_time(self, site):
        return self._item.get_last_access_time(site)

    def get_avg_access_int(self, site):
        return self._item.get_avg_access_int(site)

    def access(self, timestamp, site):
        self._item.access(timestamp, site)

    def add_storage_site(self, timestamp, site):
        self._item.add_storage_site(timestamp, site)

    def remove_storage_site(self, timestamp, site):
        self._item.remove_storage_site(timestamp, site)

    def copy(self):
        return CacheEntry(self._item.copy())

    def update(self, item, exclude=None):
        self._item.update(item, exclude)


class CacheNode(CacheBase, Node):

    def __init__(self, id, env, network, capacity):
        CacheBase.__init__(self, env, capacity)
        Node.__init__(self, id, env, network)
        self._pending_messages = {}
        self._env.process(self._handle_data())
        self._env.process(self._handle_messages())

    def _handle_data(self):
        raise NotImplemented

    def _handle_messages(self):
        raise NotImplemented


class CacheNetwork(Network):

    def __init__(self, env):
        super(CacheNetwork, self).__init__(env)
        self._num_nodes = 0
        self._master = None

    @property
    def capacity(self):
        return sum(n.capacity for n in self.cache_slaves)

    @property
    def master(self):
        return self._master

    @property
    def space_left(self):
        return sum(n.space_left for n in self.cache_slaves)

    @property
    def space_used(self):
        return sum(n.space_used for n in self.cache_slaves)

    @property
    def cache_slaves(self):
        return self.get_nodes_by_type(CacheNode)

    @property
    def clusters(self):
        return self.get_nodes_by_type(Cluster)

    @property
    def load_gens(self):
        return self.get_nodes_by_type(LoadGenerator)

    def is_cache_node(self, id):
        return id in self._g and isinstance(self._g.node[id]['instance'], CacheNode)

    def add_load_generator(self, dest, tasks, req_int_lam=None, random_state=None):
        id = self._num_nodes
        self._num_nodes += 1
        return self.add_node(id, LoadGenerator(id, self._env, self, dest, tasks,
                                               req_int_lam, random_state))

    def add_cluster(self):
        id = self._num_nodes
        self._num_nodes += 1
        return self.add_node(id, Cluster(id, self._env, self))

    def add_cache_master(self, master_type, **master_params):
        id = self._num_nodes
        self._num_nodes += 1
        self._master = master_type(id, self._env, self, **master_params)
        return self.add_node(id, self._master)

    def add_cache_slave(self, capacity, node_type, **slave_params):
        assert issubclass(node_type, CacheNode)
        id = self._num_nodes
        self._num_nodes += 1
        return self.add_node(id, node_type(id, self._env, self, capacity, **slave_params))

    def find_closest_nodes(self, anchor, nodes):
        if not nodes:
            return []
        nodes = [(n, self.estimate_transfer_time(anchor, n, 1)) for n in nodes]
        min_d = min(d for _, d in nodes)
        return [n for n, d in nodes if min_d == d]

    def to_dict(self):
        return dict(
            nodes=[dict(id=n.id, capacity=n.capacity) for n in self.cache_slaves],
            links=[dict(source=u, target=v, cost=self.estimate_transfer_time(u, v, 1))
                   for u, v, d in self._g.edges(data=True)
                   if self.is_cache_node(u) and self.is_cache_node(v)])
