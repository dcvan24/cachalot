import json
import numpy as np

from collections import Counter

from experiment.runner import ParallelExperimentRunner
from cache.base import CacheNetwork
from cache.master import parse_master_type
from cache.slave import parse_slave_type
from app.load import Load
from tools.dist import parse_dist


def parse_tuple(t, random_state):
    if not t:
        return 0
    if isinstance(t, tuple) or isinstance(t, list):
        if len(t) == 1:
            return t[0]
        else:
            return random_state.uniform(*t[:2])
            # mu, sigma = (t[0] + t[1])/2, .5
            # bw = random_state.normal(mu, sigma)
            # while bw > t[1] or bw < t[0]:
            #     bw = random_state.normal(mu, sigma)
            # return bw
    return None


class CacheNetworkTest(ParallelExperimentRunner):

    def __init__(self, cfg, run_dir):
        super(CacheNetworkTest, self).__init__(cfg, run_dir)

    def main(self, id, env, seed):
        random_state = np.random.RandomState(seed)
        net, load = self._create_network(env, random_state)
        return dict(network=net, seed=seed, load=load)

    def on_finish(self, id, runtime, **kwargs):
        verbose = self._cfg.verbose
        profile = dict(id=id, seed=kwargs['seed'], runtime=runtime)
        network = kwargs['network']
        profile['task_dist'] = list(Counter([t.input.key
                                             for t in kwargs['load'].tasks]).items())
        profile['network'] = dict(nodes=[n.__json__() for n in network.nodes],
                                  links=[l.__json__() for l in network.links])

        profile['slave'] = {attr_name :
                            [(n.id, getattr(n, attr_name)) for n in network.cache_slaves]
                            for attr_name in ('num_hits', 'rx_dropped')}

        slave_verbose = verbose.get('slave', {})
        for attr_name in ('tx_data', 'tx_transfer_time', 'exec_data', 'exec_time',
                          'insert_runtime','evict_runtime', 'retrieve_runtime'):
            profile['slave'][attr_name] = [(n.id,
                                            (len(getattr(n, attr_name)),
                                             float(np.sum(getattr(n, attr_name))
                                                   if getattr(n, attr_name) else 0),
                                             float(np.max(getattr(n, attr_name))
                                                   if getattr(n, attr_name) else 0),
                                             float(np.min(getattr(n, attr_name))
                                                   if getattr(n, attr_name) else 0),
                                             float(np.mean(getattr(n, attr_name))
                                                   if getattr(n, attr_name) else 0),
                                             float(np.std(getattr(n, attr_name))
                                                   if getattr(n, attr_name) else 0),
                                             getattr(n, attr_name)
                                             if slave_verbose.get(attr_name, False)
                                             else []))
                                           for n in network.cache_slaves
                                           if hasattr(n, attr_name)]
        if network.master is not None:
            master_verbose = verbose.get('verbose', {})
            profile['master'] = dict((attr_name,
                                      (len(getattr(network.master, attr_name)),
                                       float(np.sum(getattr(network.master, attr_name))
                                             if getattr(network.master, attr_name)
                                             else 0),
                                       float(np.max(getattr(network.master, attr_name))
                                             if getattr(network.master, attr_name)
                                             else 0),
                                       float(np.min(getattr(network.master, attr_name))
                                             if getattr(network.master, attr_name)
                                             else 0),
                                       float(np.mean(getattr(network.master, attr_name))
                                             if getattr(network.master, attr_name)
                                             else 0),
                                       float(np.std(getattr(network.master, attr_name))
                                             if getattr(network.master, attr_name)
                                             else 0),
                                       getattr(network.master, attr_name)
                                       if master_verbose.get(attr_name, False) else []))
                                     for attr_name in ('insert_time', 'evict_time',
                                                       'retrieve_time', 'solver_time',
                                                       'transfer_time', 'num_evicted',
                                                       'num_transferred')
                                     if hasattr(network.master, attr_name))
        profile['wait_time'] = [(n.id, len(n.wait_time),
                                 float(np.sum(n.wait_time) if n.wait_time else 0),
                                 float(np.max(n.wait_time) if n.wait_time else 0),
                                 float(np.min(n.wait_time) if n.wait_time else 0),
                                 float(np.mean(n.wait_time) if n.wait_time else 0),
                                 float(np.std(n.wait_time) if n.wait_time else 0),
                                 n.wait_time if verbose.get('wait_time', False) else [])
                                for n in network.load_gens]
        profile['exec_time'] = [(n.id, len(n.exec_time),
                                 float(np.sum(n.exec_time) if n.exec_time else 0),
                                 float(np.max(n.exec_time) if n.exec_time else 0),
                                 float(np.min(n.exec_time) if n.exec_time else 0),
                                 float(np.mean(n.exec_time) if n.exec_time else 0),
                                 float(np.std(n.exec_time) if n.exec_time else 0),
                                 n.exec_time if verbose.get('exec_time', False) else [])
                                for n in network.clusters]
        profile['wait_time_per_key'] = {}
        for n in network.load_gens:
            for k, v in n.wait_time_per_key.items():
                profile['wait_time_per_key'].setdefault(k, []).extend(v)
        for k, v in dict(profile['wait_time_per_key']).items():
            profile['wait_time_per_key'][k] = (
                len(v),
                float(np.sum(v)) if v else 0,
                float(np.max(v)) if v else 0,
                float(np.min(v)) if v else 0,
                float(np.mean(v)) if v else 0,
                float(np.std(v)) if v else 0,
                v if verbose.get('wait_time_per_key', False) else [],
            )
        profile['trace'] = {}
        if verbose.get('trace', False):
            insert, evict, retrieve = self._collect_trace(network.load_gens)
            profile['trace']['insert'] = insert
            profile['trace']['evict'] = evict
            profile['trace']['retrieve'] = retrieve
        json.dump(profile, open('%s/result-%d.json'%(self._run_dir, id), 'w'))

    def _create_network(self, env, random_state):
        cfg = self._cfg
        master_type = parse_master_type(cfg.master_type)
        slave_type = parse_slave_type(cfg.slave_type)
        net = CacheNetwork(env)
        load = Load(cfg.load, random_state)
        tasks = self._skew_task_distribution(load.tasks, random_state)
        #part = len(load.tasks)//cfg.n_nodes
        #tasks = {i : load.tasks[i * part : (i + 1) * part] for i in range(cfg.n_nodes)}
        cluster = net.add_cluster()
        master = net.add_cache_master(master_type, **cfg.master_params)
        for i in range(cfg.n_nodes):
            # dist = sorted(Counter([t.input.key for t in tasks[i]]).items(),
            #               key=lambda x: x[0])[:10]
            # print('Node %i: %s'%(i, dist))
            cache_node = net.add_cache_slave(cfg.capacity//cfg.n_nodes, slave_type,
                                             **cfg.slave_params)
            load_gen = net.add_load_generator(cluster.id, tasks[i],
                                              req_int_lam=cfg.req_int_lam,
                                              random_state=random_state)
            net.add_link(cache_node.id, load_gen.id)
            net.add_link(cache_node.id, master.id)
            net.add_link(cache_node.id, cluster.id,
                         bw=parse_tuple(cfg.cluster_bw, random_state),
                         lat=parse_tuple(cfg.cluster_lat, random_state))
        cluster.start(cfg.load.n_tasks)
        master.start(cfg.load.n_tasks)
        sources, dests = list(net.cache_slaves), list(net.cache_slaves)
        random_state.shuffle(sources)
        random_state.shuffle(dests)
        n_links = cfg.n_nodes * (cfg.n_nodes - 1) // 2
        # bws = random_state.uniform(*cfg.cache_bw, n_links)
        bounds = cfg.cache_bw
        bws = random_state.lognormal(0.5, 1, n_links * 3) * 100
        bws = [i for i in (bws * bounds[1]/np.max(bws)) if bounds[0] <= i <= bounds[1]][:n_links]

        random_state.shuffle(bws)
        idx = 0
        for src in sources:
            for dst in dests:
                if src.id == dst.id:
                    net.add_link(src.id, dst.id, bw=0, lat=0)
                    continue
                if net.has_link(src.id, dst.id):
                    continue
                net.add_link(src.id, dst.id,
                             bw=bws[idx],
                             lat=parse_tuple(cfg.cache_lat, random_state))
                idx += 1
        self.logger.info('Average: %f, Stdev: %f'%(np.mean(bws), np.std(bws)))
        return net, load

    def _skew_task_distribution(self, tasks, random_state):
        cfg = self._cfg
        unique_tasks = {t.input.key : t for t in tasks}
        req_dist_type = parse_dist(cfg.req_dist)
        bins = {}
        sugar = 0
        for i, count in Counter([t.input.key for t in tasks]).items():
            req_dist = req_dist_type(**cfg.req_dist_params)
            for _ in range(count):
                j = req_dist.generate()
                node_id = (int(j) + sugar - 1)%cfg.n_nodes
                bins.setdefault(node_id, []).append(unique_tasks[i])
            sugar += 1
        for b in bins.values():
            random_state.shuffle(b)
        return bins
    
    @staticmethod
    def _collect_trace(load_gens):
        items = set()
        for l in load_gens:
            items |= l.items
        insertions, evictions, retrievals = {}, {}, {}
        for i in items:
            for r in i.insert_log:
                insertions.setdefault(i.key, []).append(r)
            for r in i.evict_log:
                evictions.setdefault(i.key, []).append(r)
            for r in i.retrieve_log:
                retrievals.setdefault(i.key, []).append(r)
        for d in (insertions, evictions, retrievals):
            for k in dict(d):
                d[k] = sorted(d[k], key=lambda x: x[0])
        return insertions, evictions, retrievals





