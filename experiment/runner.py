import os
import time
import json
import simpy
import inspect
import datetime
import multiprocessing as mp

from collections import namedtuple

from core import Loggable
from app.load import LoadConfig


RunConfig = namedtuple('RunConfig',
                       'seeds n_runs n_nodes capacity '
                       'master_type master_params slave_type slave_params '
                       'cluster_bw cluster_lat cache_bw cache_lat '
                       'req_int_lam req_dist req_dist_params load verbose')


class ParallelExperimentRunner(Loggable):

    def __init__(self, cfg, run_dir):
        self._cfg = cfg
        self._run_dir = run_dir

    def main(self, id, env, seed):
        raise NotImplemented

    def on_completion(self):
        json.dump(self._cfg._asdict(), open('%s/cfg.json'%self._run_dir, 'w'))

    def on_finish(self, id, runtime, **kwargs):
        raise NotImplemented

    def run(self):
        with mp.Pool(mp.cpu_count()) as pool:
           pool.map(self._spawn, range(self._cfg.n_runs))
        self.on_completion()

    def _spawn(self, id):
        seeds = self._cfg.seeds
        seed = seeds[id] if len(seeds) > id else int(time.time()) + id ** 2
        self.logger.info('Random seed: {}'.format(seed))
        env = simpy.Environment()
        r_data = self.main(id, env, seed)
        start_time = datetime.datetime.now()
        env.run()
        runtime = str(datetime.datetime.now() - start_time)
        self.logger.info('Thread {} time elapsed: {}'.format(id, runtime))
        self.on_finish(id, runtime, **r_data)


class ExperimentOrchestrator(Loggable):

    def __init__(self, experiment, vars, cfg_file=None):
        self._experiment = experiment
        self._vars = vars
        self._run_dir = os.path.dirname(os.path.abspath(
            inspect.getouterframes(inspect.currentframe())[1][1]))
        self._cfg = self._parse_cfg(cfg_file)

    def run(self):
        result_path = '%s/results'%self._run_dir
        figure_path = '%s/figures'%self._run_dir
        if not os.path.exists(result_path):
            os.makedirs(result_path)
        if not os.path.exists(figure_path):
            os.makedirs(figure_path)
        self._run(dict(self._cfg), 0, [])

    def _run(self, cfg, lv, attrs):
        if lv == len(self._vars):
            run_dir = '%s/results/%s'%(self._run_dir, '-'.join(attrs))
            if not os.path.exists(run_dir):
                os.makedirs(run_dir)
            exp_instance = self._experiment(self._convert_cfg(cfg), run_dir)
            self.logger.info('Running: {}'.format(str(attrs)))
            exp_instance.run()
            return
        cfg = dict(cfg)
        attr_name, vals = self._vars[lv]
        for v in vals:
            cfg[attr_name] = v
            attrs.append(str(attr_name))
            attrs.append(str(v))
            self._run(cfg, lv + 1, attrs)
            attrs.pop()
            attrs.pop()

    def _parse_cfg(self, cfg_file):
        if not cfg_file:
            cfg_file = '%s/cfg.json'%self._run_dir
        return json.load(open(cfg_file, 'r'))

    @staticmethod
    def _convert_cfg(cfg):
        cfg = dict(cfg)
        cfg['load'] = LoadConfig(**cfg['load'])
        return RunConfig(**cfg)

if __name__ == '__main__':
    from experiment.cache_network import CacheNetworkTest
    orch = ExperimentOrchestrator(CacheNetworkTest, [('n_nodes', (100, ))],
                                  cfg_file='../config/cache_network.json')
    orch.run()
