import sys
import simpy
import numpy as np
import networkx as nx

from numbers import Number
from collections import namedtuple

from core import BaseObject, Runnable, Loggable
from app.base import Message, MessageType, Subject


Packet = namedtuple('Packet', 'src dst size msg')


class Network(Runnable, Loggable):

    def __init__(self, env):
        Runnable.__init__(self, env)
        Loggable.__init__(self)
        self._g = nx.Graph()

    @property
    def nodes(self):
        return [d['instance'] for _, d in self._g.nodes(True)]

    @property
    def links(self):
        return [d['instance'] for _, _, d in self._g.edges(data=True)]

    def has_node(self, id):
        return id in self._g

    def has_link(self, src, dst):
        return self._g.has_edge(src, dst)

    def add_node(self, id, instance):
        self._g.add_node(id, instance=instance)
        return instance

    def add_link(self, src, dst, bw=None, lat=0):
        if src not in self._g:
            raise ValueError('Node {} is not in this network'.format(src))
        if dst not in self._g:
            raise ValueError('Node {} is not in this network'.format(dst))
        src_node, dst_node = self._g.node[src]['instance'], self._g.node[dst]['instance']
        data_link = Link(self._env, self, src_node, dst_node, bw, lat)
        msg_link = Link(self._env, self, src_node, dst_node, 0, lat)
        self._g.add_edge(src, dst, instance=data_link, message=msg_link)
        src_node.add_link(data_link, msg_link)
        dst_node.add_link(data_link, msg_link)
        return data_link

    def get_nodes_by_type(self, type):
        return [d['instance'] for _, d in self._g.nodes_iter(data=True)
                if isinstance(d['instance'], type)]

    def get_node(self, id):
        """

        :param id:
        :return:
        """
        if id not in self._g:
            raise ValueError('Node {} is not found'.format(id))
        return self._g.node[id]['instance']

    def get_link(self, src, dst):
        if not self._g.has_edge(src, dst):
            raise ValueError('Link between {} and {} is not found'.format(src, dst))
        return self._g[src][dst]['instance']

    def estimate_transfer_time(self, src, dst, size):
        if src == dst:
            return 0
        if not self._g.has_edge(src, dst):
            return sys.float_info.max
        return self._g[src][dst]['instance'].estimate_delay(src, size)


class Node(BaseObject, Runnable, Loggable):

    def __init__(self, id, env, network):
        BaseObject.__init__(self, id)
        Runnable.__init__(self, env)
        Loggable.__init__(self)
        self._network = network
        self._id = id
        self._neighbors = {}
        self._data = simpy.Store(env)
        self._msg = simpy.Store(env)

    @property
    def id(self):
        return self._id

    def add_link(self, data_link, msg_link):
        if data_link.src != self and data_link.dst != self:
            return
        if msg_link.src != self and msg_link.dst != self:
            return
        dst = data_link.src if data_link.dst == self else data_link.dst
        self._neighbors.setdefault(dst.id, dict(data=data_link,
                                                msg=msg_link))
        self._env.process(self._listen_data(data_link))
        self._env.process(self._listen_message(msg_link))

    def send(self, dst, msg, timeout=None):
        if dst not in self._neighbors:
            raise ValueError(
                'Destination {} is not reachable from {}'.format(dst, self._id))
        link = self._neighbors[dst]['data']
        pkt = Packet(self._id, dst, msg.size, msg)
        return link.send(self, pkt, timeout)

    def recv(self):
        return self._data.get()

    def send_message(self, dst, msg):
        if dst not in self._neighbors:
            raise ValueError(
                'Destination {} is not reachable from {}'.format(dst, self._id))
        link = self._neighbors[dst]['msg']
        pkt = Packet(self._id, dst, msg.size, msg)
        link.send(self, pkt)

    def recv_message(self):
        return self._msg.get()

    def _listen_data(self, link):
        while self.is_alive:
            pkt = yield link.recv(self)
            self._data.put(pkt)

    def _listen_message(self, link):
        while self.is_alive:
            pkt = yield link.recv(self)
            self._msg.put(pkt)

    def __json__(self):
        return self._id


class Link(Runnable, Loggable):

    def __init__(self, env, network, src, dst, bw=None, latency=0.):
        Runnable.__init__(self, env)
        Loggable.__init__(self)
        self._network = network
        self._src = src
        self._dst = dst
        self._lock = simpy.Resource(env, capacity=1)
        self._src_in = simpy.Store(env)
        self._src_out = simpy.Store(env)
        self._dst_in = simpy.Store(env)
        self._dst_out = simpy.Store(env)
        self._src_trans_start = 0
        self._src_delay = None
        self._dst_trans_start = 0
        self._dst_delay = None
        self._bw = bw
        self._latency = latency
        self._connect()

    @property
    def src(self):
        return self._src

    @property
    def dst(self):
        return self._dst

    @property
    def bw(self):
        return self._bw

    @property
    def latency(self):
        return self._latency

    def send(self, node, pkt, timeout=None):
        q = self._src_in if node == self._src else self._dst_in
        # est_delay = sum([p.size for p in q.items]
        #                 + [pkt.size])/self._bw if self._bw else 0
        # if q == self._src_in and self._src_delay:
        #     est_delay += self._src_delay - (self._env.now - self._src_trans_start)
        # elif q == self._dst_in and self._dst_delay:
        #     est_delay += self._dst_delay - (self._env.now - self._dst_trans_start)
        # if timeout and est_delay > timeout:
        #     self.logger.debug('Est. delay: {}, timeout: {}'.format(est_delay, timeout))
        #     return False
        q.put(pkt)
        return True

    def recv(self, node):
        q = self._src_out if node == self._src else self._dst_out
        return q.get()

    def estimate_delay(self, id, size):
        delay = 0
        if id == self._src.id and self._bw:
            delay += sum([pkt.size for pkt in self._src_in.items] + [size])/self._bw
        if id == self._dst.id and self._bw:
            delay += sum([pkt.size for pkt in self._dst_in.items] + [size])/self._bw
        if id == self._src.id and self._src_delay and self._src_trans_start:
            delay += self._src_delay - (self._env.now - self._src_trans_start)
        if id == self._dst.id and self._dst_delay and self._dst_trans_start:
            delay += self._dst_delay - (self._env.now - self._dst_trans_start)
        return delay

    def _connect(self):
        def connect(in_q, out_q):
            while self.is_alive:
                # if self._dst.id != 10000:
                #     est_delay = sum([p.size for p in in_q.items])/self._bw if self._bw else 0
                #     if est_delay > 0:
                #         self.logger.info('src: {}, dst: {}, bw: {}, in: {}, delay: {}'.format(
                #             self._src.id, self._dst.id, self._bw, len(in_q.items), est_delay))
                # if len(out_q.items) > 10 and self._dst.id != 10000:
                # self.logger.info('src: {}, dst: {}, bw: {}, out: {}'.format(
                #     self._src.id, self._dst.id, self._bw, len(out_q.items)))
                #self.logger.info('src: {}, dst: {}, in: {}'.format(self._src.id, self._dst.id, len(in_q.items)))
                #self.logger.info('src: {}, dst: {}, in: {}'.format(self._src.id, self._dst.id, len(out_q.items)))
                pkt = yield in_q.get()
                if self._latency:
                    yield self._env.timeout(self._latency)
                if self._bw:
                    transfer_time = pkt.size/self._bw
                    if in_q == self._src_in:
                        self._src_trans_start = self._env.now
                        self._src_delay = transfer_time
                    else:
                        self._dst_trans_start = self._env.now
                        self._dst_delay = transfer_time
                    self.logger.debug('Transfer packet {}, size: {}, time: {}'.format(
                        pkt.msg.subject, pkt.size, transfer_time))
                    yield self._env.timeout(transfer_time)
                    if in_q == self._src_in:
                        self._src_trans_start = 0
                        self._src_delay = 0
                    else:
                        self._dst_trans_start = 0
                        self._dst_delay = 0
                out_q.put(pkt)
        self._env.process(connect(self._src_in, self._dst_out))
        self._env.process(connect(self._dst_in, self._src_out))

    def __json__(self):
        return dict(src=self._src.id, dst=self._dst.id,
                    bw=self._bw, lat=self._latency)


class Cluster(Node):

    def __init__(self, id, env, network):
        """

        :param id:
        :param env:
        :param network:
        """
        super(Cluster, self).__init__(id, env, network)
        self._exec_time = []

    @property
    def exec_time(self):
        return self._exec_time

    def run_task(self, task):
        start_time = self._env.now
        yield self._env.timeout(task.exec_time)
        self._exec_time.append(task.exec_time)
        self.logger.debug('Task {} runs for {} time '
                          'units'.format(task.id, self._env.now - start_time))
        return task

    def start(self, n):
        for _ in range(n):
            self._env.process(self._listen_incoming_tasks())

    def _listen_incoming_tasks(self):
        from cache.base import CacheItem
        while self.is_alive:
            pkt = yield self._data.get()
            msg, task = pkt.msg, pkt.msg.data
            yield self._env.process(self.run_task(task))
            if (self._env.now - msg.timestamp) > task.exec_time * 1.1:
                self.logger.info('Runtime: %f, time elapsed: %f'%(task.exec_time, self._env.now - msg.timestamp))
            self.send(msg.src, Message(self._env.now, msg.subject, self._id, msg.src,
                                       MessageType.TASK_EXEC_RESP, task.output.size,
                                       CacheItem(task.id, task), msg.trace + [(self._id, self._env.now - msg.subject.timestamp)]))


class LoadGenerator(Node):

    def __init__(self, id, env, network, dest, tasks,
                 req_int_lam=None, random_state=None):
        """

        :param id:
        :param env:
        :param network:
        :param dest:
        :param load:
        :param req_int_lam:
        :param random_state:
        """
        super(LoadGenerator, self).__init__(id, env, network)
        self._tasks = tasks
        self._dest = dest
        self._req_int_lam = req_int_lam
        self._random_state = random_state if random_state else np.random
        self._pending_requests = {}

        self._wait_time = []
        self._wait_time_per_key = {}
        self._items = set()
        self._is_finished = False

        self._env.process(self._send_request())
        self._env.process(self._listen_responses())

    @property
    def wait_time(self):
        return self._wait_time

    @property
    def wait_time_per_key(self):
        return self._wait_time_per_key

    @property
    def items(self):
        return self._items

    @property
    def random_state(self):
        return self._random_state

    @property
    def is_finished(self):
        return self._is_finished

    def add_link(self, data_link, msg_link):
        if len(self._neighbors) > 1:
            raise Exception(
                '{} cannot have more than one link'.format(self.__class__.__name__))
        super(LoadGenerator, self).add_link(data_link, msg_link)

    def _send_request(self):
        if len(self._neighbors) < 1:
            raise Exception('{} is not connected to any other node'.format(self._id))
        gateway = next(iter(self._neighbors.keys()))
        for i, t in enumerate(self._tasks):
            timestamp = self._env.now
            subject = Subject(timestamp, self._id, self._dest, t)
            msg = Message(timestamp, subject, self._id, self._dest,
                          MessageType.TASK_EXEC_REQ, t.input.size, t, [(self._id, 0)])
            self._pending_requests[subject] = msg
            self.send(gateway, msg)
            # self.logger.info('Client {}: sent {} requests'.format(self._id, i))
            if self._req_int_lam:
                req_int = self._random_state.poisson(self._req_int_lam)
                self.logger.debug('Request interval: {}'.format(req_int))
                yield self._env.timeout(req_int)
        self._is_finished = True
        self.logger.info('Client {} finishes submitting tasks'.format(self._id))

    def _listen_responses(self):
        count = 0
        while self.is_alive:
            pkt = yield self._data.get()
            req = self._pending_requests.pop(pkt.msg.subject, None)
            item = pkt.msg.data
            self._items.add(item)
            if req:
                assert item.value.id == pkt.msg.subject.data.id
                task = req.data
                time_elapsed = self._env.now - req.timestamp
                time_estimated = task.exec_time
                trace = pkt.msg.trace + [(self._id, time_elapsed)]
                if time_elapsed > time_estimated:
                    self.logger.debug('Exec time: %f, size: %f, time used: %f, trace: %s'%(
                            time_estimated, task.output.size, time_elapsed, trace))
                count += 1
                # self.logger.info('Client {}: received {} responses'.format(self._id, count))
                self.logger.debug('Client {} received a response of {} at {}, '
                                  'request sent at {}, time elapsed: {}'
                                  ''.format(self._id, req.data.input.key,
                                            self._env.now, req.timestamp,
                                            time_elapsed))
                self._wait_time.append(time_elapsed)
                self._wait_time_per_key.setdefault(
                    req.data.input.key, []).append(time_elapsed)
                self.logger.debug('Task {} is fulfilled in {} time '
                                  'units'.format(req.subject, time_elapsed))
            else:
                self.logger.warn('Unknown task {}'.format(pkt.msg.subject))


class Buffer(Loggable):

    def __init__(self, env, capacity=None):
        assert capacity is None or (isinstance(capacity, Number) and capacity >= 0)
        super(Buffer, self).__init__()
        self._env = env
        self._store = simpy.Store(env)
        self._capacity = simpy.Container(env, capacity, capacity) if capacity else None

    @property
    def space_available(self):
        return self._capacity.level

    def read(self):
        item = yield self._store.get()
        if self._capacity:
            self._capacity.put(item.size)
        return item

    def write(self, item):
        assert hasattr(item, 'size')
        if self._capacity and item.size > self._capacity.level:
            return False
        self._store.put(item)
        if self._capacity:
            self._capacity.get(item.size)
        return True


if __name__ == '__main__':
    from app.load import Load, LoadConfig
    from cache.base import CacheNetwork
    from cache.master import LRUCacheMaster, LinearProgramMaster, LFUCacheMaster
    from cache.slave import NoOpCacheSlave, LFUCacheSlave

    env = simpy.Environment()

    # net = CacheNetwork(env)
    #
    # net.add_cache_slave(0, 300, LFUCacheSlave)
    # net.add_cache_slave(1, 300, LFUCacheSlave)
    # net.add_cache_slave(2, 300, LFUCacheSlave)

    net = CacheNetwork(env, LinearProgramMaster)

    net.add_cache_slave(0, 300, NoOpCacheSlave)
    net.add_cache_slave(1, 300, NoOpCacheSlave)
    net.add_cache_slave(2, 300, NoOpCacheSlave)

    cluster = net.add_cluster(3)

    cfg4 = LoadConfig(n_tasks=10 ** 4,
                      n_data=10 ** 4, size_dist='uniform',
                      size_dist_params=dict(lo=1, hi=1), n_ops=1, op_dist='uniform',
                      op_dist_params=dict(lo=1, hi=1), op_exec_func='linear',
                      op_exec_func_params=dict(w=0, b=0), op_exec_time_func='linear',
                      op_exec_time_func_params=dict(w=0, b=1), op_output_size_func='linear',
                      op_output_size_func_params=dict(w=0, b=1),
                      # data_access_dist='uniform',
                      # data_access_dist_params=dict(lo=1, hi=10 ** 3),
                      data_access_dist='power',
                      data_access_dist_params=dict(a=.8, n_ranks=10 ** 3),
                      op_access_dist='uniform', op_access_dist_params=dict(lo=1, hi=1))
    cfg5 = LoadConfig(n_tasks=10 ** 4,
                      n_data=10 ** 4, size_dist='uniform',
                      size_dist_params=dict(lo=1, hi=1), n_ops=1, op_dist='uniform',
                      op_dist_params=dict(lo=1, hi=1), op_exec_func='linear',
                      op_exec_func_params=dict(w=0, b=0), op_exec_time_func='linear',
                      op_exec_time_func_params=dict(w=0, b=1), op_output_size_func='linear',
                      op_output_size_func_params=dict(w=0, b=1),
                      data_access_dist='power',
                      data_access_dist_params=dict(a=.8, n_ranks=10 ** 3),
                      # data_access_dist='uniform',
                      # data_access_dist_params=dict(lo=1, hi=10 ** 3),
                      op_access_dist='uniform', op_access_dist_params=dict(lo=1, hi=1))
    cfg6 = LoadConfig(n_tasks=10 ** 4,
                      n_data=10 ** 4, size_dist='uniform',
                      size_dist_params=dict(lo=1, hi=1), n_ops=1, op_dist='uniform',
                      op_dist_params=dict(lo=1, hi=1), op_exec_func='linear',
                      op_exec_func_params=dict(w=0, b=0), op_exec_time_func='linear',
                      op_exec_time_func_params=dict(w=0, b=1), op_output_size_func='linear',
                      op_output_size_func_params=dict(w=0, b=1),
                      # data_access_dist='uniform',
                      # data_access_dist_params=dict(lo=1, hi=10 ** 3),
                      data_access_dist='power',
                      data_access_dist_params=dict(a=.8, n_ranks=10 ** 3),
                      op_access_dist='uniform', op_access_dist_params=dict(lo=1, hi=1))

    net.add_load_generator(4, 3, Load(cfg4), req_int_range=(1, 5), random_state=np.random.RandomState(10))
    net.add_load_generator(5, 3, Load(cfg5), req_int_range=(5, 10), random_state=np.random.RandomState(25))
    net.add_load_generator(6, 3, Load(cfg6), req_int_range=(1, 10), random_state=np.random.RandomState(59))

    net.add_link(0, 1, .5)
    net.add_link(1, 2, .3)
    net.add_link(0, 2, .2)
    for i in range(3):
        net.add_link(i, 3, .1)

    net.add_link(0, 4)
    net.add_link(1, 5)
    net.add_link(2, 6)

    env.run()
    print(cluster.num_exec)
    for n in net.cache_slaves:
        print('Node {}: {}'.format(n.id, n.num_hits))



