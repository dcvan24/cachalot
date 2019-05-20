import os
import json
import datetime
import subprocess

from collections import OrderedDict
from subprocess import Popen

from cache.base import CacheEntry, CacheClassCatalog
from cache.lfu import FrequencyQueue
from cache.lrv import LRVQueue
from infrastructure.base import Node
from app.base import MessageType, Message, Subject


def parse_master_type(type_name):
    if 'lfu' == type_name:
        return LFUCacheMaster
    elif 'lru' == type_name:
        return LRUCacheMaster
    elif 'network-lrv' == type_name:
        return NetworkAwareLRVMaster
    elif 'lp' == type_name:
        return LinearProgramMaster
    elif 'cooperative' == type_name:
        return CooperativeCacheMaster
    elif 'epoch' == type_name:
        return EpochBasedCooperativeCacheMaster
    elif 'no-op' == type_name:
        return NoOpCacheMaster
    return None


class CacheMaster(Node):

    def __init__(self, id, env, network, load_aware=False, **kwargs):
        super(CacheMaster, self).__init__(id, env, network)
        self._load_aware = load_aware
        self._pending_insertions = set()
        self._store = {}
        self._waitlist = {}
        self._flat_waitlist = {}

        self._insert_time = []
        self._evict_time = []
        self._retrieve_time = []

    @property
    def capacity(self):
        return self._network.capacity

    @property
    def space_left(self):
        return self._network.space_left

    @property
    def space_used(self):
        return self._network.space_used

    @property
    def insert_time(self):
        return self._insert_time

    @property
    def evict_time(self):
        return self._evict_time

    @property
    def retrieve_time(self):
        return self._retrieve_time

    def start(self, n):
        for _ in range(n):
            self._env.process(self._listen_messages())

    def add_to_waitlist(self, key, requester, subject):
        self.logger.debug('{} is being processed, add {} to wait list'.format(key, requester))
        self._waitlist.setdefault(key, {}).setdefault(requester, []).append(subject)
        self._flat_waitlist.setdefault(key, OrderedDict())[subject] = subject.timestamp

    def get_least_recent_subject(self, key):
        subjects = self._flat_waitlist.get(key, OrderedDict())
        return list(subjects.items())[0] if subjects else (None, None)

    def clear_waitlist(self, key, requester):
        subjects = self._waitlist.get(key, {}).pop(requester, [])
        for s in subjects:
            self._flat_waitlist.pop(s, None)
        return subjects

    def insert(self, src, key, item):
        raise NotImplemented

    def ack_insert(self, item, node_id, status):
        raise NotImplemented

    def retrieve(self, requester, key):
        raise NotImplemented

    def get_metadata(self, key):
        raise NotImplemented

    def remove_cache(self, node, key, trace):
        ce = self._store.get(key, None)
        if ce is None:
            return
        ce.remove_storage_site(self._env.now, node)
        if len(ce.storage_sites) == 0:
            self._store.pop(key, None)
            return
        self._update_cache_entry(ce, trace)

    def _update_cache_entry(self, ce, trace):
        for s in ce.storage_sites:
            self._send_cache_update_request(s, ce.item, trace)

    def _find_available_slave(self, src, size):
        slaves = [s.id for s in self._network.cache_slaves if s.space_left >= size]
        if not slaves:
            return None
        if src in slaves:
            return src
        if self._load_aware:
            return min(slaves,
                       key=lambda s: self._network.estimate_transfer_time(src, s, size))
        return max(slaves,
                   key=lambda s: self._network.get_link(src, s).bw)

    def _evict(self, ce):
        ce_site = ce.storage_sites[0]
        slave = self._network.get_node(ce_site)
        slave.evict(ce.key)

    def _send_cache_insert_response(self, subject, dst, node_id, item, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_INSERT_RESP, 0,
                                       dict(node=node_id, item=item),
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _send_cache_retrieve_response(self, subject, dst, node_id, delay, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_RETRIEVE_RESP, 0,
                                       dict(node=node_id, delay=delay),
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _send_cache_update_request(self, dst, item, trace):
        self.send_message(dst, Message(self._env.now, None, self._id, dst,
                                       MessageType.CACHE_UDPATE_REQ, 0, item,
                                       trace + [self._id]))

    def _send_cache_metadata_response(self, subject, dst, meta, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_METADATA_RESP, 0, meta,
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _send_cache_wl_add_response(self, subject, dst, lr, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_ADD_RESP, 0, lr,
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _send_cache_wl_requester_response(self, subject, dst, requesters, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_REQUESTER_RESP, 0, requesters,
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _send_cache_wl_request_response(self, subject, dst, requests, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_REQUEST_RESP, 0, requests,
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _send_cache_wl_clear_response(self, subject, dst, requests, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_CLEAR_RESP, 0, requests,
                                       trace + [(self._id,
                                                 self._env.now - subject.timestamp)]))

    def _listen_messages(self):
        while self.is_alive:
            pkt = yield self._msg.get()
            msg, subject = pkt.msg, pkt.msg.subject
            if msg.type == MessageType.CACHE_INSERT_REQ:
                item = msg.data
                node = self.insert(msg.src, item.key, item)
                self._send_cache_insert_response(subject, msg.src, node, item, msg.trace)
            elif msg.type == MessageType.CACHE_RETRIEVE_REQ:
                key = msg.data
                node, delay = self.retrieve(msg.src, key)
                self._send_cache_retrieve_response(subject, msg.src, node, delay, msg.trace)
            elif msg.type == MessageType.CACHE_INSERT_ACK:
                self.ack_insert(msg.data['item'], msg.data['node'], msg.data['status'])
            elif msg.type == MessageType.CACHE_REMOVE_REQ:
                self.remove_cache(msg.data['node'], msg.data['key'], msg.trace)
            elif msg.type == MessageType.CACHE_METADATA_REQ:
                meta = self.get_metadata(msg.data)
                self._send_cache_metadata_response(subject, msg.src, meta, msg.trace)
            elif msg.type == MessageType.CACHE_WL_ADD_REQ:
                self.add_to_waitlist(msg.data, msg.src, subject)
                lr = self.get_least_recent_subject(msg.data)
                self._send_cache_wl_add_response(subject, msg.src, lr, msg.trace)
            elif msg.type == MessageType.CACHE_WL_REQUESTER_REQ:
                rs = [(k, v[0]) for k, v in self._waitlist.get(msg.data, {}).items()
                      if len(v) > 0]
                self._send_cache_wl_requester_response(subject, msg.src, rs, msg.trace)
            elif msg.type == MessageType.CACHE_WL_REQUEST_REQ:
                key, node = msg.data['key'], msg.data['node']
                requests = list(self._waitlist.get(key, {}).get(node, []))
                self._send_cache_wl_request_response(subject, msg.src, requests, msg.trace)
            elif msg.type == MessageType.CACHE_WL_CLEAR_REQ:
                requests = self.clear_waitlist(msg.data, msg.src)
                self._send_cache_wl_clear_response(subject, msg.src, requests, msg.trace)
            else:
                self.logger.warn('Unknown message type: {}'.format(msg.type))


class NoOpCacheMaster(CacheMaster):

    def __init__(self, id, env, network, **kwargs):
        super(NoOpCacheMaster, self).__init__(id, env, network, **kwargs)
        self._store = {}

    def get_metadata(self, key):
        return self._store.get(key, None)

    def insert(self, src, key, item):
        if key in self._store or key in self._pending_insertions:
            self.logger.debug('Item {} is already in cache'.format(key))
            return None
        item.access(self._env.now, src)
        insert_start = datetime.datetime.now()
        self._store[key] = item
        self._insert_time.append(
            (datetime.datetime.now() - insert_start).total_seconds())
        return src

    def retrieve(self, requester, key):
        retrieve_start = datetime.datetime.now()
        item = self._store.get(key)
        if not item:
            return None, None
        item.access(self._env.now, requester)
        self._retrieve_time.append(
            (datetime.datetime.now() - retrieve_start).total_seconds())
        return requester, 0


class CooperativeCacheMaster(CacheMaster):

    def __init__(self, id, env, network, **kwargs):
        super(CooperativeCacheMaster, self).__init__(id, env, network, **kwargs)
        self._store = {}

    def insert(self, src, key, item):
        if key in self._store or key in self._pending_insertions:
            self.logger.debug('Item {} is already in cache'.format(key))
            return None
        slave = self._find_available_slave(src, item.size)
        if not slave:
            slave = src
        item.access(self._env.now, src)
        if self._load_aware and self._network.estimate_transfer_time(
                src, slave, item.size) >= item.value.exec_time:
            slave = src
        self._pending_insertions.add(key)
        return slave

    def get_metadata(self, key):
        return self._store.get(key, None)

    def ack_insert(self, item, node, status):
        if item.key not in self._pending_insertions:
            return
        ce = CacheEntry(item.copy())
        self._pending_insertions.remove(item.key)
        if status:
            insert_start = datetime.datetime.now()
            ce.add_storage_site(self._env.now, node)
            self._store[item.key] = ce
            self._insert_time.append(
                (datetime.datetime.now() - insert_start).total_seconds())

    def retrieve(self, requester, key):
        retrieve_start = datetime.datetime.now()
        ce = self._store.get(key)
        if not ce or len(ce.storage_sites) == 0:
            return None, None
        ce.access(self._env.now, requester)
        self._update_cache_entry(ce, [self._id])
        if requester in ce.storage_sites:
            return requester, 0
        delay_map = {s: self._network.get_link(s, requester).estimate_delay(s, ce.size)
                     for s in ce.storage_sites}
        site = min(delay_map.keys(), key=lambda x: delay_map.get(x))
        self._retrieve_time.append(
            (datetime.datetime.now() - retrieve_start).total_seconds())
        return site, delay_map[site]

    def _get_closest_slave(self, src, sites=None):
        return min(sites if sites else self._network.cache_slaves,
                   key=lambda s: self._network.estimate_transfer_time(src.id, s.id, 1))


class EpochBasedCooperativeCacheMaster(CooperativeCacheMaster):

    def __init__(self, id, env, network, epoch, **kwargs):
        super(EpochBasedCooperativeCacheMaster, self).__init__(id, env, network, **kwargs)
        self._is_optimizing = False
        self._epoch = epoch
        self._env.process(self._run_epoch())

    def _run_epoch(self):
        while all(not l.is_finished for l in self._network.load_gens):
            yield self._env.timeout(self._epoch)
            self._optimize()

    def _optimize(self):
        cache_slaves = self._network.cache_slaves
        self._signal_optimize_start()
        plan, updates = {}, []
        n_iter = 0
        while True:
            n_iter += 1
            self.logger.info('Iteration {} starts'.format(n_iter))
            for s in cache_slaves:
                report = s.optimize()
                if not report:
                    continue
                plan.setdefault(s, []).append(report)
                updates.append((s, report[0], report[1]))
            self.logger.info('Iteration {} finishes'.format(n_iter))
            if len(updates) == 0:
                self.logger.info('Converge at Iteration {}'.format(n_iter))
                break
            self.logger.info('Iteration {}: {} updates reported'.format(n_iter,
                                                                         len(updates)))
            for u in updates:
                site, gain, loss = u
                gain.add_storage_site(self._env.now, site.id)
                loss.remove_storage_site(self._env.now, site.id)
            updates = []
        self._execute_plan(plan)
        self._signal_optimize_finish()

    def insert(self, src, key, item):
        if self._is_optimizing:
            return 
        super(EpochBasedCooperativeCacheMaster, self).insert(src, key, item)

    def _signal_optimize_start(self):
        self._is_optimizing = True
        snapshot = {k: CacheEntry(v) for k, v in self._store.items()}
        for s in self._network.cache_slaves:
            s.init_optimization(snapshot)

    def _execute_plan(self, plan):
        for s in self._network.cache_slaves:
            if s not in plan:
                continue
            for g, l in plan[s]:
                s.evict(l.key)
                site = min(g.storage_sites,
                           key=lambda x: self._network.estimate_transfer_time(x, s.id, 1))

                self._send_master_insert_req(self._network.get_node(site), s.id, g.item)
                self._pending_insertions.add(g.key)

    def _signal_optimize_finish(self):
        for s in self._network.cache_slaves:
            s.finish_optimization()
        self._is_optimizing = False


class LRUCacheMaster(CacheMaster):

    def __init__(self, id, env, network, **kwargs):
        super(LRUCacheMaster, self).__init__(id, env, network, **kwargs)
        self._store = OrderedDict()

    def insert(self, src, key, item):
        if item.size > self.capacity:
            self.logger.debug('Max: {}, input: {}'.format(self.space_left, item.size))
            return
        if key in self._store or key in self._pending_insertions:
            self.logger.debug('Insertion of item {} is already requested'.format(key))
            return
        while self.space_left < item.size:
            self.logger.debug('Left: {}, input: {}'.format(self.space_left, item.size))
            evict_start = datetime.datetime.now()
            _, ce_rm = self._store.popitem(False)
            self._evict_time.append(
                (datetime.datetime.now() - evict_start).total_seconds())
            self._evict(ce_rm)
            self.logger.debug('Space left: {}'.format(self.space_left))
        slave = self._find_available_slave(src, item.size)
        ce = CacheEntry(key, item)
        ce.access(self._env.now, src.id)
        if slave == src:
            slave.insert(item)
            insert_start = datetime.datetime.now()
            self._store[key] = ce
            self._insert_time.append(
                (datetime.datetime.now() - insert_start).total_seconds())
        else:
            self._send_master_insert_req(src, slave.id, item)
            self._pending_insertions.add(key)

    def ack_insert(self, item, status):
        if item.key in self._pending_insertions:
            self._pending_insertions.remove(item.key)
            if status:
                insert_start = datetime.datetime.now()
                self._store[item.key] = CacheEntry(item)
                self._insert_time.append(
                    (datetime.datetime.now() - insert_start).total_seconds())

    def retrieve(self, requester, key):
        retrieve_start = datetime.datetime.now()
        if key not in self._store:
            self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        ce = self._store[key]
        ce.access(self._env.now, requester)
        del self._store[key]
        self._store[key] = ce
        self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
        nodes = self._network.find_closest_nodes(requester, ce.storage_sites)
        return nodes[0] if nodes else None


class LFUCacheMaster(CacheMaster):

    def __init__(self, id, env, network, **kwargs):
        super(LFUCacheMaster, self).__init__(id, env, network, **kwargs)
        self._store = FrequencyQueue()

    def insert(self, src, key, item):
        if item.size > self.capacity:
            self.logger.debug('Max: {}, input: {}'.format(self.capacity, item.size))
            return
        if key in self._store or key in self._pending_insertions:
            self.logger.debug('Insertion of item {} is already requested'.format(key))
            return
        self.logger.debug('Space left: {}'.format(self.space_left))
        while self.space_left < item.size:
            self.logger.debug('Left: {}, input: {}'.format(self.space_left, item.size))
            evict_start = datetime.datetime.now()
            ce_rm = self._store.dequeue()
            self._evict_time.append(
                (datetime.datetime.now() - evict_start).total_seconds())
            self.logger.debug('Evicting item {}'.format(ce_rm.key))
            self._evict(ce_rm)
        slave = self._find_available_slave(src, item.size)
        ce = CacheEntry(key, item)
        ce.access(self._env.now, src.id)
        if slave == src:
            slave.insert(item)
            insert_start = datetime.datetime.now()
            self._store.enqueue(key, ce)
            self._insert_time.append(
                (datetime.datetime.now() - insert_start).total_seconds())
        else:
            self._send_master_insert_req(src, slave.id, item)
            self._pending_insertions.add(key)

    def ack_insert(self, item, status):
        if item.key in self._pending_insertions:
            self._pending_insertions.remove(item.key)
            if status:
                insert_start = datetime.datetime.now()
                self._store.enqueue(item.key, CacheEntry(item))
                self._insert_time.append(
                    (datetime.datetime.now() - insert_start).total_seconds())

    def retrieve(self, requester, key):
        retrieve_start = datetime.datetime.now()
        ce = self._store.get(key)
        if not ce:
            self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        ce.access(self._env.now, requester)
        self._store.enqueue(key, ce)
        self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
        nodes = self._network.find_closest_nodes(requester, ce.storage_sites)
        return nodes[0] if nodes else None


class LRVCacheMaster(CacheMaster):

    def __init__(self, id, env, network, util_func, **kwargs):
        super(LRVCacheMaster, self).__init__(id, env, network, **kwargs)
        self._store = LRVQueue(util_func)

    def insert(self, src, key, item):
        if item.size > self.capacity:
            self.logger.debug('Max: {}, input: {}'.format(self.capacity, item.size))
            return
        if key in self._pending_insertions:
            self.logger.debug('Insertion of item {} is already requested'.format(key))
            return
        self.logger.debug('Space left: {}'.format(self.space_left))
        while self.space_left < item.size:
            self.logger.debug('Left: {}, input: {}'.format(self.space_left, item.size))
            evict_start = datetime.datetime.now()
            ce_rm = self._store.dequeue()
            self._evict_time.append(
                (datetime.datetime.now() - evict_start).total_seconds())
            self.logger.debug('Evicting item {}'.format(ce_rm.key))
            self._evict(ce_rm)
        slave = self._find_available_slave(src, item.size)
        ce = CacheEntry(key, item)
        ce.access(self._env.now, src.id)
        if slave == src:
            slave.insert(item)
            insert_start = datetime.datetime.now()
            self._store.enqueue(key, ce)
            self._insert_time.append(
                (datetime.datetime.now() - insert_start).total_seconds())
        else:
            self._send_master_insert_req(src, slave.id, item)
            self._pending_insertions.add(key)

    def ack_insert(self, item, status):
        if item.key in self._pending_insertions:
            self._pending_insertions.remove(item.key)
            if status:
                insert_start = datetime.datetime.now()
                self._store.enqueue(item.key, CacheEntry(item))
                self._insert_time.append(
                    (datetime.datetime.now() - insert_start).total_seconds())

    def retrieve(self, requester, key):
        retrieve_start = datetime.datetime.now()
        ce = self._store.get(key)
        if not ce:
            self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        ce.access(self._env.now, requester)
        self._store.enqueue(key, ce)
        self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
        nodes = self._network.find_closest_nodes(requester, ce.storage_sites)
        return nodes[0] if nodes else None


class NetworkAwareLRVMaster(LRVCacheMaster):

    def __init__(self, id, env, network, **kwargs):
        super(NetworkAwareLRVMaster, self).__init__(id, env, network,
                                                    self._util_func, **kwargs)

    def _util_func(self, ce):
        util = 0
        for s in ce.access_sites:
            n_hits = ce.get_num_hits(s)
            slave = self._network.find_closest_nodes(s, ce.storage_sites)
            if slave is None:
                continue
            proj_saving = 11 - self._network.estimate_transfer_time(s, slave, ce.size)
            util += n_hits * proj_saving
        return util


class LinearProgramMaster(CacheMaster):

    def __init__(self, id, env, network, threshold=1., n_classes=100):
        super(LinearProgramMaster, self).__init__(id, env, network)
        self._store = {}
        self._threshold = threshold
        self._is_solver_invoked = False
        self._last_optimize_chkpt = 0
        self._last_insert_chkpt = 0
        self._running_solver = False
        self._n_rejected = 0
        self._catalog = CacheClassCatalog(n_classes, self._network)

        # stats collection
        self._solver_time = []
        self._transfer_time = []
        self._num_evicted = []
        self._num_transferred = []

        self._env.process(self._optimize_every_epoch())

    @property
    def solver_time(self):
        return self._solver_time

    @property
    def transfer_time(self):
        return self._transfer_time

    @property
    def num_evicted(self):
        return self._num_evicted

    @property
    def num_transferred(self):
        return self._num_transferred

    def insert(self, src, key, item):
        self._last_insert_chkpt = self._env.now
        if self._running_solver:
            self._n_rejected += 1
            self.logger.debug('Rejected due to optimization: {}'.format(self._n_rejected))
            return
        if item.size > self.capacity:
            self.logger.debug('Max: {}, input: {}'.format(self.space_left, item.size))
            return
        # self._optimize()
        self.logger.debug('Space left: {}'.format(self.space_left))
        slave = self._find_available_slave(src, item.size)
        if not slave:
            self.logger.warn('Out of space, abort inserting item {}'.format(key))
            return
        ce = CacheEntry(key, item, self._network)
        ce.access(self._env.now, src.id)
        if slave == src:
            slave.insert(item)
            insert_start = datetime.datetime.now()
            self._store[key] = ce
            self._add_to_cache_class(ce)
            self._insert_time.append(
                (datetime.datetime.now() - insert_start).total_seconds())
        else:
            self._pending_insertions.add(key)
            self._send_master_insert_req(src, slave.id, item)

    def ack_insert(self, item, status):
        if item.key in self._pending_insertions:
            self._pending_insertions.remove(item.key)
            if status:
                insert_start = datetime.datetime.now()
                ce = CacheEntry(item)
                self._store[item.key] = ce
                self._add_to_cache_class(ce)
                self._insert_time.append(
                    (datetime.datetime.now() - insert_start).total_seconds())

    def retrieve(self, requester, key):
        retrieve_start = datetime.datetime.now()
        if key not in self._store:
            self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        ce = self._store[key]
        if not ce.storage_sites:
            self._retrieve_time.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        self._retrieve_time.append(
            (datetime.datetime.now() - retrieve_start).total_seconds())
        nodes = self._network.find_closest_nodes(requester, ce.storage_sites)
        ce.access(self._env.now, requester)
        return nodes[0] if nodes else None

    def _optimize(self):
        self._running_solver = True
        # call LP to evict and transfer items
        solver_start = datetime.datetime.now()
        classes = self._catalog.create_classes()
        plan = self._invoke_linear_program(classes)
        self._solver_time.append(
            (datetime.datetime.now() - solver_start).total_seconds())
        if not plan:
            return
        # perform evictions
        self._evict_lp(plan['evict'], classes)
        # perform transfers
        self._transfer_lp(plan['transfer'], classes)
        self._last_optimize_chkpt = self._env.now
        self._running_solver = False

    def _optimize_every_epoch(self, epoch=30*60):
        while self._env.now - self._last_insert_chkpt < 20:
            self.logger.info('Epoch optimization triggered at {}'.format(self._env.now))
            yield self._env.timeout(epoch)
            self._optimize()

    def _add_to_cache_class(self, ce):
        self._catalog.add_cache_entry(ce)

    def _remove_from_cache_class(self, ce):
        self._catalog.remove_cache_entry(ce)

    def _invoke_linear_program(self, classes):
        if self._is_solver_invoked:
            self.logger.info('Solver has been invoked, waiting ...')
            return
        self.logger.info('Invoking LP solver ...')
        self._is_solver_invoked = True

        sage_fn = 'sage-%s.json'%os.getpid()

        json.dump(dict(g=self._network.to_dict(),
                       threshold=self._threshold,
                       classes=list(classes.values())),
                  open(sage_fn, 'w'))
        cur_path = os.path.dirname(os.path.abspath(__file__))
        out, err = Popen(('sage %s/../solver/solver.py %s'%(cur_path, sage_fn)).split(' '),
                         stdout=subprocess.PIPE,
                         shell=False).communicate()
        if err:
            self.logger.warn(err)
        self._is_solver_invoked = False
        if isinstance(out, bytes):
            out = out.decode()
        return json.loads(out) if not err else None

    def _evict_lp(self, plan, classes):
        self.logger.info('Eviction set size: {}'.format(len(plan)))
        for e in plan:
            self._greedy_evict(classes=classes, **e)

    def _transfer_lp(self, plan, classes):
        self.logger.info('Transfer set size: {}'.format(len(plan)))
        for t in plan:
            self._greedy_transfer(classes=classes, **t)

    def _greedy_evict(self, class_id, node_id, amount, classes):
        self.logger.info('\tclass: {}, node: {}, amount: '
                         '{}'.format(class_id, node_id, amount))
        slave = self._network.get_node(node_id)
        entries = classes[class_id].get_entries_by_storage_site(node_id)
        eviction_set, evict_start = [], datetime.datetime.now()
        for e in sorted(entries, key=lambda x: x.get_util(node_id)/x.size):
            eviction_set.append(e)
            amount -= e.size
            if amount <= 0:
                break
        self._num_evicted.append(len(eviction_set))
        self._evict_time.append(
            (datetime.datetime.now() - evict_start).total_seconds())
        for e in eviction_set:
            self.logger.debug('Evicting item {} from {}'.format(e.key, node_id))
            self._remove_from_cache_class(e)
            slave.evict(e.key)
            self._store.pop(e.key, None)

    def _greedy_transfer(self, class_id, src, dst, amount, classes):
        msg_src, msg_dst = self._network.get_node(dst), self._network.get_node(src)
        entries = [ce for ce in classes[class_id].get_entries_by_storage_site(src)
                   if ce in self._catalog]
        proj_space_left = msg_src.space_left
        transfer_set, transfer_start = [], datetime.datetime.now()
        for e in sorted(entries, key=lambda x: -x.get_util(src)/x.size):
            if e.size > amount:
                if e.size <= proj_space_left:
                    self.logger.debug('Transfer item {} from {} to '
                                      '{}'.format(e.key, src, dst))
                    transfer_set.append((msg_src, msg_dst.id, e.key))
                break
            self.logger.debug('Transfer item {} from {} to {}'.format(e.key, src, dst))
            transfer_set.append((msg_src, msg_dst.id, e.key))
            proj_space_left -= e.size
            amount -= e.size
        self._num_transferred.append(len(transfer_set))
        self._transfer_time.append(
            (datetime.datetime.now() - transfer_start).total_seconds())
        for t in transfer_set:
            self._send_master_transfer_req(*t)





