import datetime
import numpy as np

from cache.base import CacheNode, CacheEntry
from app.base import Message, MessageType
from cache.lfu import FrequencyQueue
from cache.lrv import LRVQueue


def parse_slave_type(type_name):
    if 'no-op' == type_name:
        return NoOpCacheSlave
    elif 'lfu' == type_name:
        return LFUCacheSlave
    elif 'lrv' == type_name:
        return NetworkAwareLRVSlave
    elif 'gd' == type_name:
        return GradientDescentLRVSlave
    return None


class CacheSlave(CacheNode):

    def __init__(self, id, env, network, capacity,
                 auto_replication=False, share_compute=False, network_aware=False,
                 **kwargs):
        super(CacheSlave, self).__init__(id, env, network, capacity)
        self._eviction_set = set()
        self._congestion_list = {}
        self._metadata_list = {}
        self._wl_clear_list = {}
        self._wl_requester_list = {}
        self._wl_request_list = {}

        self._num_hits = 0
        self._tx_data = []
        self._tx_transfer_time = []
        self._exec_data = []
        self._exec_time = []

        self._rx_dropped = 0
        self._local_hit = 0
        self._remote_hit = 0
        self._auto_replication = auto_replication
        self._share_compute = share_compute
        self._network_aware = network_aware

        self._processed = set()

    @property
    def num_hits(self):
        return self._num_hits

    @property
    def tx_data(self):
        return self._tx_data

    @property
    def tx_transfer_time(self):
        return self._tx_transfer_time

    @property
    def exec_data(self):
        return self._exec_data

    @property
    def exec_time(self):
        return self._exec_time

    @property
    def rx_dropped(self):
        return self._rx_dropped

    def get(self, key):
        return self._store.get(key)

    def evict(self, key):
        raise NotImplemented

    def update_cache_entry(self, item):
        raise NotImplemented

    def increment_cache_hit(self):
        self.logger.debug('Cache hit on {}, total: {}'.format(self._id, self._num_hits))
        self._num_hits += 1

    def _send_task_exec_request(self, subject, dst, size, item, trace):
        self.send(dst, Message(self._env.now, subject, self._id, dst,
                               MessageType.TASK_EXEC_REQ, size, item,
                               trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_task_exec_response(self, subject, dst, size, item, trace):
        self.send(dst, Message(self._env.now, subject, self._id, dst,
                               MessageType.TASK_EXEC_RESP, size, item,
                               trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_transfer_request(self, subject, dst, item, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                          MessageType.CACHE_TRANSFER_REQ, 0, item,
                          trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_transfer_response(self, subject, dst, size, item, trace):
        return self.send(dst,
                         Message(self._env.now, subject, self._id, dst,
                                 MessageType.CACHE_TRANSFER_RESP, size, item,
                                 trace + [(self._id, self._env.now - subject.timestamp)]),
                         subject.data.exec_time)

    def _send_cache_transfer_declined_message(self, subject, dst, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_TRANSFER_DECLINE, 0, None,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_redirect_message(self, subject, dst, trace):
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_REDIRECT, 0, None,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_insert_request(self, subject, item, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_INSERT_REQ, 0, item,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_insert_ack(self, subject, item, status, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_INSERT_ACK, 0,
                                       dict(item=item, node=self._id, status=status),
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_retrieve_request(self, subject, key, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_RETRIEVE_REQ, 0, key,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_master_insert_request(self, subject, dst, item, trace):
        self.send(dst, Message(self._env.now, subject, self._id, dst,
                               MessageType.MASTER_INSERT_REQ, item.size, item,
                               trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_remove_request(self, key):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, None, self._id, dst,
                                       MessageType.CACHE_REMOVE_REQ, 0,
                                       dict(node=self._id, key=key), [self._id]))

    def _send_cache_metadata_request(self, subject, key, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_METADATA_REQ, 0, key,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_wl_add_request(self, subject, key, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_ADD_REQ, 0, key,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_wl_requester_request(self, subject, key, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_REQUESTER_REQ, 0, key,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_wl_request_request(self, subject, key, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_REQUEST_REQ, 0,
                                       dict(key=key, node=self._id),
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _send_cache_wl_clear_request(self, subject, key, trace):
        dst = self._network.master.id
        self.send_message(dst, Message(self._env.now, subject, self._id, dst,
                                       MessageType.CACHE_WL_CLEAR_REQ, 0, key,
                                       trace + [(self._id, self._env.now - subject.timestamp)]))

    def _get_generation_cost(self, task, site):
        cluster = self._network.clusters[0].id
        return task.exec_time + \
               self._network.estimate_transfer_time(site, cluster, task.output.size)

    def _handle_data(self):
        while self.is_alive:
            pkt = yield self._data.get()
            msg, subject = pkt.msg, pkt.msg.subject
            if msg.type == MessageType.TASK_EXEC_REQ:
                task = msg.data
                self._send_cache_retrieve_request(subject, task.id, msg.trace)
                self._pending_messages[subject] = msg
            elif msg.type == MessageType.TASK_EXEC_RESP:
                if subject in self._processed:
                    continue
                item = msg.data
                if self._share_compute:
                    self._send_cache_wl_clear_request(subject, item.key, msg.trace)
                    self._wl_clear_list[subject] = msg
                else:
                    self._handle_task_output(msg, [msg.subject], msg.trace)
            elif msg.type == MessageType.CACHE_TRANSFER_RESP:
                item = msg.data
                if self._share_compute:
                    self._send_cache_wl_request_request(subject, item.key, msg.trace)
                    self._wl_request_list[subject] = (item, msg, False)
                else:
                    orig_msg = self._pending_messages.pop(subject, None)
                    if orig_msg is None:
                        if subject not in self._processed:
                            self.logger.warn('Node {}: {} is not in the pending '
                                             'list'.format(self._id, subject))
                        continue
                    self._remote_hit += 1
                    self._processed.add(subject)
                    self._tx_data.append(item.size)
                    self._tx_transfer_time.append(
                        self._network.estimate_transfer_time(msg.src, self._id, item.size))
                    if self._auto_replication:
                        self.insert(item)
                    self.increment_cache_hit()
                    self._send_task_exec_response(subject, orig_msg.src, item.size, item,
                                                  msg.trace)
            elif msg.type == MessageType.MASTER_INSERT_REQ:
                item = msg.data
                if item.key in self._eviction_set:
                    self._eviction_set.remove(item.key)
                else:
                    status = self.insert(item)
                    if not status:
                        self._rx_dropped += 1
                    self._send_cache_insert_ack(subject, item, status, msg.trace)
            else:
                self.logger.warn('Unknown message type: {}'.format(msg.type))

    def _handle_messages(self):
        while self.is_alive:
            pkt = yield self._msg.get()
            msg, subject = pkt.msg, pkt.msg.subject
            if msg.type == MessageType.CACHE_INSERT_RESP:
                node, item = msg.data['node'], msg.data['item']
                if node is None:
                    continue
                if node == self._id:
                    status = self.insert(item)
                    self._send_cache_insert_ack(subject, item, status, msg.trace)
                else:
                    self._send_master_insert_request(subject, node, item, msg.trace)
            elif msg.type == MessageType.CACHE_RETRIEVE_RESP:
                node, delay = msg.data['node'], msg.data['delay']
                msg = self._pending_messages[subject]
                task = msg.data
                if node is None:
                    # cache miss anywhere
                    self._send_task_exec_request(subject, msg.dst, msg.size, msg.data,
                                                 msg.trace)
                    if self._share_compute:
                        self._send_cache_wl_add_request(subject, task.id, msg.trace)
                elif node == self._id:
                    ce = self.retrieve(task.id)
                    if ce:
                        if self._share_compute:
                            self._send_cache_wl_request_request(subject, task.id, msg.trace)
                            self._wl_request_list[subject] = (ce.item, msg, True)
                            self._send_cache_wl_requester_request(subject, task.id, msg.trace)
                            self._wl_requester_list[subject] = ce.item
                        else:
                            orig_msg = self._pending_messages.pop(subject, None)
                            if orig_msg is None:
                                if subject not in self._processed:
                                    self.logger.warn('{} is not in the pending '
                                                     'list'.format(subject))
                                continue
                            self._processed.add(subject)
                            self._local_hit += 1
                            self.increment_cache_hit()
                            self._send_task_exec_response(subject, orig_msg.src, ce.size,
                                                          ce.item, msg.trace)
                    else:
                        if self._share_compute:
                            self._send_cache_wl_add_request(subject, task.id, msg.trace)
                        else:
                            self._exec_data.append(msg.size)
                            self._exec_time.append(self._get_generation_cost(task, self._id))
                            self._send_task_exec_request(subject, msg.dst, msg.size,
                                                         msg.data, msg.trace)
                else:
                    if self._share_compute:
                        self._send_cache_wl_add_request(subject, task.id, msg.trace)
                    if self._network_aware:
                        self._metadata_list[subject] = (msg, node, delay)
                        self._send_cache_metadata_request(subject, task.id, msg.trace)
                    else:
                        self._send_cache_transfer_request(subject, node, task.id, msg.trace)
            elif msg.type == MessageType.CACHE_TRANSFER_REQ:
                # send the requested cache entry to the requester
                ce = self.retrieve(msg.data)
                data_size = ce.size if ce else 0
                if data_size:
                    tx_time = self._network.estimate_transfer_time(self._id, msg.src,
                                                                   data_size)
                    if self._network_aware and tx_time > ce.value.exec_time:
                        self._send_cache_redirect_message(subject, msg.src, msg.trace)
                        continue
                    assert subject.data.id == ce.key
                    self._send_cache_transfer_response(subject, msg.src, data_size,
                                                       ce.item, msg.trace)
                else:
                    self._send_cache_transfer_declined_message(subject, msg.src,
                                                               msg.trace)
            elif msg.type == MessageType.CACHE_TRANSFER_DECLINE:
                dst, task = subject.dst, subject.data
                orig_msg = self._pending_messages.get(subject)
                if not orig_msg:
                    if subject not in self._processed:
                        self.logger.warn('{} is not in the pending list'.format(subject))
                    continue
                if self._share_compute:
                    self._send_cache_wl_add_request(subject, task.id, msg.trace)
                else:
                    self._exec_data.append(task.input.size)
                    self._exec_time.append(self._get_generation_cost(task, self._id))
                    self._send_task_exec_request(subject, dst, task.input.size, task,
                                                 msg.trace)
            elif msg.type == MessageType.CACHE_REDIRECT:
                dst, task = subject.dst, subject.data
                orig_msg = self._pending_messages.get(subject)
                if not orig_msg:
                    if subject not in self._processed:
                        self.logger.warn('{} is not in the pending list'.format(subject))
                    continue
                self._congestion_list[subject] = orig_msg
                self._send_task_exec_request(subject, dst, task.input.size, task,
                                             msg.trace)
            elif msg.type == MessageType.CACHE_METADATA_RESP:
                orig_msg, node, delay = self._metadata_list.pop(subject, None)
                if orig_msg is None:
                    self.logger.warn('{} is not found in the metadata wait list'.format(subject))
                    continue
                meta = msg.data
                self.logger.debug('self: {}, selected: {}, local?: {}'.format(
                    self._id, node, self._id in meta.storage_sites))
                if self._network_aware and delay > meta.value.exec_time:
                    self._congestion_list[subject] = orig_msg
                    self._send_task_exec_request(subject, orig_msg.dst, orig_msg.size,
                                                 orig_msg.data, msg.trace)
                else:
                    self._send_cache_transfer_request(subject, node, meta.key, msg.trace)
            elif msg.type == MessageType.CACHE_WL_ADD_RESP:
                task = subject.data
                orig_msg = self._pending_messages.get(subject)
                if not orig_msg:
                    if subject not in self._processed:
                        self.logger.warn('{} is not in the pending list'.format(subject))
                    continue
                _, lr_timestamp = msg.data
                if lr_timestamp and self._env.now - lr_timestamp < task.exec_time:
                    continue
                self._exec_data.append(orig_msg.size)
                self._exec_time.append(self._get_generation_cost(task, self._id))
                self._send_task_exec_request(subject, orig_msg.dst, orig_msg.size,
                                             orig_msg.data, msg.trace)
            elif msg.type == MessageType.CACHE_WL_REQUESTER_RESP:
                item = self._wl_requester_list.pop(subject, None)
                if item is None:
                    self.logger.warn('{} is not in the wl_requester wait list'.format(subject))
                    continue
                requesters = msg.data
                for r, s in requesters:
                    if r == self._id:
                        continue
                    if self._network_aware \
                            and self._network.estimate_transfer_time(
                                self._id, r, item.size) > item.value.exec_time:
                        continue
                    assert subject.data.id == item.key
                    self._send_cache_transfer_response(s, r, item.size, item, msg.trace)
            elif msg.type == MessageType.CACHE_WL_REQUEST_RESP:
                item, last_msg, is_local = self._wl_request_list.pop(subject,
                                                                     (None, None, None))
                if item is None:
                    self.logger.warn('{} is not in the wl_request wait '
                                     'list'.format(subject))
                    continue
                subjects = list(msg.data) + [subject]
                for s in subjects:
                    orig_msg = self._pending_messages.pop(s, None)
                    if not orig_msg:
                        if s not in self._processed:
                            self.logger.warn('Node {}: {} is not in the pending '
                                             'list'.format(self._id, s))
                        continue
                    self._processed.add(s)
                    if is_local:
                        self._local_hit += 1
                    else:
                        self._remote_hit += 1
                    if subject == s:
                        if not is_local:
                            self._tx_data.append(item.size)
                            self._tx_transfer_time.append(
                                self._network.estimate_transfer_time(last_msg.src,
                                                                     self._id, item.size))
                        if self._auto_replication:
                            self.insert(item)
                    else:
                        item.access(self._env.now, self._id)
                    self.increment_cache_hit()
                    self._send_task_exec_response(s, orig_msg.src, item.size, item,
                                                  msg.trace)
            elif msg.type == MessageType.CACHE_WL_CLEAR_RESP:
                orig_msg = self._wl_clear_list.pop(subject, None)
                if orig_msg is None:
                    continue
                subjects = msg.data
                subjects.append(subject)
                self._handle_task_output(orig_msg, subjects, msg.trace)
            elif msg.type == MessageType.CACHE_UDPATE_REQ:
                self.update_cache_entry(msg.data)
            else:
                self.logger.debug('Unknown message type: {}'.format(msg.type))

    def _handle_task_output(self, msg, subjects, trace):
        subject, item = msg.subject, msg.data
        if subject in self._congestion_list:
            self.increment_cache_hit()
        else:
            self._send_cache_insert_request(subject, item.copy(), trace)
        for s in subjects:
            orig_msg = self._pending_messages.pop(s, None)
            if not orig_msg:
                if s not in self._processed:
                    self.logger.warn('{} is not in the pending list'.format(s))
                continue
            self._processed.add(s)
            self._congestion_list.pop(s, None)
            if s != subject:
                item.access(self._env.now, self._id)
                self.increment_cache_hit()
            self._send_task_exec_response(s, orig_msg.src, msg.size, item, trace)
        if self._share_compute:
            self._send_cache_wl_requester_request(subject, item.key, msg.trace)
            self._wl_requester_list[subject] = item


class NoOpCacheSlave(CacheSlave):

    def __init__(self, id, env, network, capacity, **kwargs):
        super(NoOpCacheSlave, self).__init__(id, env, network, capacity, **kwargs)

    def insert(self, item):
        if item.key in self._store:
            self.logger.debug('Item {} is already in cache'.format(item.key))
            return False
        if item.size > self.space_left:
            self.logger.warn(
                'Slave {}: item {} rejected due to out of space'.format(self._id, item.key))
            return False
        ce = CacheEntry(item)
        self._occupy(ce)
        self._store[ce.key] = ce
        self.logger.debug('Item {} has been added in cache, '
                          '{} space is occupied'.format(ce.key, ce.size))
        return True

    def evict(self, key):
        ce_rm = self._store.pop(key, None)
        if ce_rm:
            self._reclaim(ce_rm)
            self.logger.debug('Item {} has been evicted, '
                              '{} space is freed'.format(key, ce_rm.size))
        else:
            # the item to evict may be on the way, add to eviction set
            self.logger.debug('Item {} is not found, added to eviction set'.format(key))
            self._eviction_set.add(key)
        return ce_rm

    def lookup(self, key):
        return self._store.get(key)

    def retrieve(self, key):
        return self.get(key)


class AutonomousCacheSlave(CacheSlave):

    def __init__(self, id, env, network, capacity, **kwargs):
        super(AutonomousCacheSlave, self).__init__(id, env, network, capacity, **kwargs)
        self._insert_runtime = []
        self._evict_runtime = []
        self._retrieve_runtime = []

    @property
    def insert_runtime(self):
        return self._insert_runtime

    @property
    def evict_runtime(self):
        return self._evict_runtime

    @property
    def retrieve_runtime(self):
        return self._retrieve_runtime

    def evict(self, key):
        raise NotImplemented


class LFUCacheSlave(AutonomousCacheSlave):

    def __init__(self, id, env, network, capacity, **kwargs):
        super(LFUCacheSlave, self).__init__(id, env, network, capacity, **kwargs)
        self._store = FrequencyQueue()

    def insert(self, item):
        if item.key in self._store:
            self.logger.debug('Item {} is already in cache'.format(item.key))
            return False
        if item.size > self.capacity:
            self.logger.debug('Max {}, input: {}'.format(self.capacity, item.size))
            return False
        self.logger.debug('Node {}, space left: {}'.format(self._id, self.space_left))
        ce = CacheEntry(item)
        while self.space_left < item.size:
            evict_start = datetime.datetime.now()
            ce_rm = self._store.dequeue()
            self._evict_runtime.append(
                (datetime.datetime.now() - evict_start).total_seconds())
            self.logger.debug('Evicting item {}'.format(ce_rm.key))
            self._reclaim(ce_rm)
            self._send_cache_remove_request(item.key)
        self._occupy(ce)
        insert_start = datetime.datetime.now()
        self._store.enqueue(item.key, ce)
        self._insert_runtime.append(
            (datetime.datetime.now() - insert_start).total_seconds())
        return True

    def retrieve(self, key):
        retrieve_start = datetime.datetime.now()
        ce = self._store.get(key)
        if not ce:
            self._retrieve_runtime.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        self._store.enqueue(key, ce)
        self._retrieve_runtime.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
        return ce

    def update_cache_entry(self, item):
        ce = self._store.get(item.key)
        if not ce:
            return
        ce.update(item, exclude=self._id)


class LRVCacheSlave(AutonomousCacheSlave):

    def __init__(self, id, env, network, capacity,
                 util_func=None, inflate=False, **kwargs):
        super(LRVCacheSlave, self).__init__(id, env, network, capacity, **kwargs)
        self._func = util_func
        self._store = LRVQueue(util_func, inflate)

    def insert(self, item):
        if item.key in self._store:
            self.logger.debug('Item {} is already in cache'.format(item.key))
            return False
        if item.size > self.capacity:
            self.logger.debug('Max {}, input: {}'.format(self.capacity, item.size))
            return False
        self.logger.debug('Node {}, space left: {}'.format(self._id, self.space_left))
        ce = CacheEntry(item)
        if not self._before_insert(ce):
            return False
        while self.space_left < ce.size:
            evict_start = datetime.datetime.now()
            ce_rm = self._store.dequeue()
            self._evict_runtime.append(
                (datetime.datetime.now() - evict_start).total_seconds())
            self._reclaim(ce_rm)
            self._send_cache_remove_request(item.key)
            self.logger.debug('Evicting item {}'.format(ce_rm.key))
        self._occupy(ce)
        insert_start = datetime.datetime.now()
        self._store.enqueue(item.key, ce)
        self._insert_runtime.append(
            (datetime.datetime.now() - insert_start).total_seconds())
        return True

    def retrieve(self, key):
        retrieve_start = datetime.datetime.now()
        ce = self._store.get(key)
        if not ce:
            self._retrieve_runtime.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
            return None
        self.logger.debug("Cache entry parent: {}".format(ce.parent))
        self._store.enqueue(key, ce)
        self._retrieve_runtime.append(
                (datetime.datetime.now() - retrieve_start).total_seconds())
        return ce

    def update_cache_entry(self, item):
        ce = self._store.get(item.key)
        if not ce:
            return
        ce.update(item, exclude=self._id)

    def _before_insert(self, ce):
        return True


class NetworkAwareLRVSlave(LRVCacheSlave):

    def __init__(self, id, env, network, capacity, util_func, **kwargs):
        super(NetworkAwareLRVSlave, self).__init__(id, env, network, capacity,
                                                   self._get_util_func(util_func),
                                                   **kwargs)

    def _get_util_func(self, util_func):
        if 'weight_sum_sub' == util_func:
            return self._weight_sum_sub
        elif 'nectar' == util_func:
            return self._nectar
        elif 'greedy_dual' == util_func:
            return self._greedy_dual
        elif 'freq' == util_func:
            return self._freq
        return None

    def _before_insert(self, ce):
        if self._func != self._weight_sum_sub or not ce.storage_sites:
            return True
        gain, trans_cost = self._calc_utility_gain(ce)
        if gain > self._store.least_value and gain * trans_cost > self._store.least_value:
            sites = set(ce.storage_sites)
            sites.add(self._id)
            self.logger.info('Gain: %f, transfer cost: %f, least: %f, key: %d, size: %f, sites: %s'%(
                gain, trans_cost, self._store.least_value, ce.value.input.key, ce.size, sorted(sites)))
        return gain > self._store.least_value and gain * trans_cost > self._store.least_value

    def _calc_utility_gain(self, ce):
        gain, cum_saving_ratio = 0, 0
        sites = set(ce.storage_sites)
        for s in ce.access_sites:
            gen_cost = self._get_generation_cost(ce.value, s)
            sites.add(self._id)
            site = self._network.find_closest_nodes(s, sites)[0]
            if site != self._id:
                continue
            new_cost = self._network.estimate_transfer_time(s, site, ce.size)
            sites.discard(self._id)
            site = self._network.find_closest_nodes(s, sites)
            old_cost = gen_cost if len(site) == 0 \
                else self._network.estimate_transfer_time(s, site[0], ce.size)
            last_ts = ce.get_last_access_time(s)
            recency = (self._env.now - last_ts) if last_ts else 0
            n_hits = ce.get_num_hits(s)
            delta_cost = max(old_cost - new_cost, 0)
            cum_saving_ratio += delta_cost/gen_cost
            gain += n_hits * delta_cost/(recency + 1)
        return gain/ce.size, cum_saving_ratio

    def _freq(self, ce):
        ce.access(self._env.now, self._id)
        if not ce.storage_sites:
            self.logger.warn('Premature cache entry: {}'.format(ce.id))
            return 0
        return ce.get_num_hits(self._id)

    def _nectar(self, ce):
        ce.access(self._env.now, self._id)
        if not ce.storage_sites:
            self.logger.warn('Premature cache entry: {}'.format(ce.id))
            return 0
        last_ts = ce.get_last_access_time(self._id)
        recency = (self._env.now - last_ts) if last_ts else 0
        n_hits = ce.get_num_hits(self._id)
        gen_cost = self._get_generation_cost(ce.value, self._id)
        return gen_cost * n_hits / (ce.size * (recency + 1))

    def _greedy_dual(self, ce):
        ce.access(self._env.now, self._id)
        if not ce.storage_sites:
            self.logger.warn('Premature cache entry: {}'.format(ce.id))
            return 0
        return ce.get_num_hits(self._id)/ce.size

    def _weight_sum_sub(self, ce):
        ce.access(self._env.now, self._id)
        if not ce.storage_sites:
            self.logger.warn('Premature cache entry: {}'.format(ce.id))
            return 0
        util, _ = self._calc_utility_gain(ce)
        return util


class GradientDescentLRVSlave(LRVCacheSlave):

    def __init__(self, id, env, network, capacity, **kwargs):
        kwargs['util_func'] = self._cost_function
        super(GradientDescentLRVSlave, self).__init__(id, env, network, capacity, **kwargs)
        self._optimizing = False
        self._global_store = None
        self._local_store = None

    def insert(self, item):
        if self._optimizing:
            return False
        super(GradientDescentLRVSlave, self).insert(item)

    def evict(self, key):
        if key in self._store:
            self._store.dequeue(key)

    def init_optimization(self, snapshot):
        self._optimizing = True
        self._global_store = dict(snapshot)
        self._local_store = {k: v.copy() for k, v in self._store.items.items()}

    def finish_optimization(self):
        self._optimizing = False
        self._global_store = None
        self._local_store = None

    def optimize(self):
        gc, lc = self._global_store, self._local_store
        rc = [gc.get(id) for id in set(gc.keys()).difference(set(lc.keys()))]
        loss_candidates = [ce for ce in lc.values() if len(ce.storage_sites) > 1]
        if len(loss_candidates) == 0:
            return None
        loss_ce = min(loss_candidates, key=lambda ce: self._calc_loss(ce))
        loss = self._cost_function(loss_ce)
        gain_ce = max(rc, key=lambda ce: self._calc_gain(ce))
        gain = self._calc_gain(gain_ce)
        return (gain_ce, loss_ce) if gain - loss > 0 else None

    def _calc_loss(self, ce):
        if len(ce.storage_sites) == 0:
            self._global_store.pop(ce.key, None)
            self._local_store.pop(ce.key, None)
            return 0
        if self._id not in ce.storage_sites:
            self._local_store.pop(ce.key, None)
        net = self._network
        sites = [s for s in ce.access_sites
                 if self._id in net.find_closest_nodes(s, ce.storage_sites)]
        costs = {}
        for s in sites:
            site = net.find_closest_nodes(s, ce.storage_sites)[0]
            gen_cost = self._get_generation_cost(ce.value, s)
            trans_cost = net.estimate_transfer_time(s, site, ce.size)
            costs[s] = min(gen_cost, trans_cost)
        storage_sites = set(ce.storage_sites)
        storage_sites.discard(self._id)
        loss = 0
        for s in sites:
            site = net.find_closest_nodes(s, storage_sites)[0]
            gen_cost = self._get_generation_cost(ce.value, s)
            trans_cost = net.estimate_transfer_time(s, site, ce.size)
            new_cost = min(gen_cost, trans_cost)
            loss += max(costs[s] - new_cost, 0)/ce.size
        return loss

    def _calc_gain(self, ce):
        if len(ce.storage_sites) == 0:
            self._global_store.pop(ce.key, None)
            self._local_store.pop(ce.key, None)
            return 0
        if self._id not in ce.storage_sites:
            self._local_store.pop(ce.key, None)
        net = self._network
        sites = [s for s in ce.access_sites
                 if self._id not in net.find_closest_nodes(s, ce.storage_sites)]
        costs = {}
        for s in sites:
            site = net.find_closest_nodes(s, ce.storage_sites)[0]
            gen_cost = self._get_generation_cost(ce.value, s)
            trans_cost = net.estimate_transfer_time(s, site, ce.size)
            costs[s] = min(gen_cost, trans_cost)
        storage_sites = set(ce.storage_sites)
        storage_sites.add(self._id)
        gain = 0
        for s in sites:
            site = net.find_closest_nodes(s, storage_sites)[0]
            if site != self._id:
                continue
            gen_cost = self._get_generation_cost(ce.value, s)
            trans_cost = net.estimate_transfer_time(s, site, ce.size)
            new_cost = min(gen_cost, trans_cost)
            gain += max(costs[s] - new_cost, 0)/ce.size
        return gain

    def _cost_function(self, ce):
        ce.access(self._env.now, self._id)
        if not ce.storage_sites:
            self.logger.warn('Premature cache entry: {}'.format(ce.id))
            return 0
        net = self._network
        cost = 0
        for s in ce.access_sites:
            site = self._network.find_closest_nodes(s, ce.storage_sites)[0]
            gen_cost = self._get_generation_cost(ce.value, s)
            trans_cost = net.estimate_transfer_time(s, site, ce.size)
            cost += min(gen_cost, trans_cost)/ce.size
        return cost

