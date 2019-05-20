import os
import json
import numpy as np
import matplotlib.pyplot as plt

from os.path import abspath, dirname


def find_data(cond):
    data_dir = '%s/../../data/lrv-lfu'%dirname(abspath(__file__))
    data = []
    for run_dir in os.listdir(data_dir):
        run_dir_path = '%s/%s'%(data_dir, run_dir)
        if not os.listdir(run_dir_path):
            os.rmdir(run_dir_path)
            continue
        cfg = json.load(open('%s/cfg.json'%run_dir_path))
        if not cond(cfg):
            continue
        print(cfg['slave_type'], cfg['cache_bw'][1], run_dir)
        data.append(dict(cfg=cfg,
                         data=[json.load(open('%s/%s'%(run_dir_path, fn)))
                               for fn in os.listdir(run_dir_path)
                               if fn.startswith('result')]))
    return data


def get_xlabel(xlabel):
    if xlabel == 'n_nodes':
        return 'Number of nodes'
    elif xlabel == 'capacity':
        return 'Capacity per node (100 nodes in total)'


def compare_algo_runtime(data, xlabel):
    x, y = [], {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        if d['cfg'][xlabel] not in x:
            x.append(d['cfg'][xlabel])
        master_type = d['cfg']['master_type']
        y_stat = y.setdefault(master_type, {})
        y_stat.setdefault('insert', []).append((
            np.mean([i['master']['insert_time'][4] for i in d['data']]),
            np.std([i['master']['insert_time'][4] for i in d['data']])
        ))
        y_stat.setdefault('evict', []).append((
            np.mean([i['master']['evict_time'][4] for i in d['data']]),
            np.std([i['master']['evict_time'][4] for i in d['data']])
        ))
        y_stat.setdefault('retrieve', []).append((
            np.mean([i['master']['retrieve_time'][4] for i in d['data']]),
            np.std([i['master']['retrieve_time'][4] for i in d['data']])
        ))

    plt.clf()
    ls, colors = ('-.', '-'), ('#035a9a', '#40830c', '#ff5921')
    for i, master_type in enumerate(y):
        for j, action in enumerate(y[master_type]):
            plt.errorbar(x, [t[0] for t in y[master_type][action]],
                         yerr=[t[1] for t in y[master_type][action]],
                         label='%s-%s'%(master_type, action),
                         ls=ls[i], color=colors[j],
                         capsize=3)
            plt.xticks(x, x)
            plt.xlabel(get_xlabel(xlabel))
            plt.ylabel('Avg. algorithm runtime per invocation (seconds)')
            lgd = plt.legend(ncol=2)
            plt.tight_layout()
            plt.savefig(
                'experiment/figures/algo-runtime-comp-%s.pdf' % xlabel.replace('_', '-'),
                bbox_extra_artists=(lgd,),
                bbox_inches='tight',
                format='pdf')


def plot_lp_invocations_varying_node(data):
    xticks, y = [], []
    for d in sorted(data, key=lambda x: x['cfg']['n_nodes']):
        master_type = d['cfg']['master_type']
        if master_type == 'lfu':
            continue
        xticks.append(d['cfg']['n_nodes'])
        y.append(np.mean([i['master']['solver_time'][0] for i in d['data']]))
    plt.clf()
    plt.plot(xticks, y)
    plt.xlabel('Number of nodes')
    plt.ylabel('Number of LP invocations')
    plt.xticks(xticks, xticks)
    plt.tight_layout()
    plt.savefig('experiment/figures/lp-invoke-nodes.pdf',
                bbox_inches='tight',
                format='pdf')


def plot_lp_invocations_varying_capacity(data):
    xticks, y = [], []
    for d in sorted(data, key=lambda x: x['cfg']['capacity']):
        master_type = d['cfg']['master_type']
        if master_type == 'lfu':
            continue
        xticks.append(d['cfg']['capacity'])
        y.append(np.mean([i['master']['solver_time'][0] for i in d['data']]))
    plt.clf()
    plt.plot(xticks, y)
    plt.xlabel('Capacity per node (10 nodes in total)')
    plt.ylabel('Number of LP invocations')
    plt.xticks(xticks, xticks)
    plt.tight_layout()
    plt.savefig('experiment/figures/lp-invoke-cap.pdf',
                bbox_inches='tight',
                format='pdf')


def compare_hit_rate(data, xlabel):
    xticks = []
    mean, yerr = {}, {}
    for d in sorted(data,
                    key=lambda x: x['cfg'][xlabel] if xlabel != 'cache_bw' else x['cfg'][xlabel][1]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in xticks:
            xticks.append(xtick)
        algo = d['cfg']['slave_type']
        mean.setdefault(algo, []).append(np.mean([np.sum([j[1]
                                                                 for j in i['slave']['num_hits']])
                                             for i in d['data']]))
        yerr.setdefault(algo, []).append(np.mean([np.std([j[1]
                                                                 for j in i['slave']['num_hits']])
                                                         for i in d['data']]))
    for k in dict(mean):
        mean[k] = np.array(mean[k]) * 100/d['cfg']['load_cfg'][0]
        yerr[k] = np.array(yerr[k]) * 100/d['cfg']['load_cfg'][0]
    plot_error_bar(xticks, mean, yerr,
                   markersize=2,
                   capsize=3,
                   ylim=(0, 100),
                   xlabel=get_xlabel(xlabel),
                   ylabel='Hit rate (%)',
                   save_path='experiment/figures/hitrate-comp-%s.pdf'%xlabel.replace('_', '-'))


def compare_hit_rate_varying_capacity(data):
    xticks = []
    mean, yerr = {}, {}
    for d in sorted(data, key=lambda x: x['cfg']['capacity']):
        if d['cfg']['capacity'] not in xticks:
            xticks.append(d['cfg']['capacity'])
        master_type = d['cfg']['master_type']
        mean.setdefault(master_type, []).append(np.mean([np.sum([j[1]
                                                                 for j in i['slave']['num_hits']])
                                                         for i in d['data']]))
        yerr.setdefault(master_type, []).append(np.mean([np.std([j[1]
                                                                 for j in i['slave']['num_hits']])
                                                         for i in d['data']]))
    for k in dict(mean):
        mean[k] = np.array(mean[k]) * 100/d['cfg']['load_cfg'][0]
        yerr[k] = np.array(yerr[k]) * 100/d['cfg']['load_cfg'][0]
    plot_error_bar(xticks, mean, yerr,
                   markersize=2,
                   capsize=3,
                   ylim=(0, 100),
                   xlabel='Capacity per node (10 nodes in total)',
                   ylabel='Hit rate (%)',
                   save_path='experiment/figures/hitrate-comp-cap.pdf')


def compare_wait_time_varying_node(data):
    xticks = []
    mean, yerr = {}, {}
    for d in sorted(data, key=lambda x: x['cfg']['n_nodes']):
        if d['cfg']['n_nodes'] not in xticks:
            xticks.append(d['cfg']['n_nodes'])
        master_type = d['cfg']['master_type']
        mean.setdefault(master_type, []).append(np.mean([np.mean([j[5]
                                                                  for j in i['wait_time']])
                                                         for i in d['data']]))
        yerr.setdefault(master_type, []).append(np.mean([np.std([j[5]
                                                                 for j in i['wait_time']])
                                                         for i in d['data']]))
    plot_error_bar(xticks, mean, yerr,
                   xlabel='Number of nodes',
                   ylabel='Avg. wait time per object',
                   save_path='experiment/figures/waittime-comp-node.pdf')


def compare_wait_time_varying_capacity(data):
    xticks = []
    mean, yerr = {}, {}
    for d in sorted(data, key=lambda x: x['cfg']['capacity']):
        if d['cfg']['capacity'] not in xticks:
            xticks.append(d['cfg']['capacity'])
        master_type = d['cfg']['master_type']
        mean.setdefault(master_type, []).append(np.mean([np.mean([j[5]
                                                                  for j in i['wait_time']])
                                                         for i in d['data']]))
        yerr.setdefault(master_type, []).append(np.mean([np.std([j[5]
                                                                 for j in i['wait_time']])
                                                         for i in d['data']]))
    plot_error_bar(xticks, mean, yerr,
                   xlabel='Capacity per node (10 nodes in total)',
                   ylabel='Avg. wait time per object',
                   save_path='experiment/figures/waittime-comp-cap.pdf')


def compare_rx_dropped(data, xlabel):
    xticks = []
    mean = {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        if d['cfg'][xlabel] not in xticks:
            xticks.append(d['cfg'][xlabel])
        algo = d['cfg']['slave_type']
        num_dropped = np.mean([np.sum([j[1] for j in i['slave']['rx_dropped']])
                               for i in d['data']])
        num_inserted = np.mean([i['master']['insert_time'][0] for i in d['data']])
        mean.setdefault(algo, []).append(num_dropped/num_inserted * 100)
    plot_line(xticks, mean,
              xlabel=get_xlabel(xlabel),
              ylabel='Percentage of dropped objects (%)',
              save_path='experiment/figures/dropped-comp-%s.pdf'%xlabel.replace('_', '-'))


def plot_num_evicted_and_transferred_varying_node(data):
    xticks = []
    n_evicted, n_transfer = [], []
    for d in sorted(data, key=lambda x: x['cfg']['n_nodes']):
        if d['cfg']['n_nodes'] not in xticks:
            xticks.append(d['cfg']['n_nodes'])
        master_type = d['cfg']['master_type']
        if master_type == 'lfu':
            continue
        num_evicted = np.mean([i['master']['num_evicted'][1] for i in d['data']])
        num_transferred = np.mean([i['master']['num_transferred'][1] for i in d['data']])
        n_evicted.append(num_evicted)
        n_transfer.append(num_transferred)
    plt.clf()
    plt.plot(xticks, n_evicted, label='evicted')
    plt.plot(xticks, n_transfer, label='transferred')
    plt.xticks(xticks)
    plt.xlabel('Number of nodes')
    plt.ylabel('Number of objects')
    plt.legend()
    plt.tight_layout()
    plt.savefig('experiment/figures/lp-evict-transfer-node.pdf',
                bbox_inches='tight',
                format='pdf')


def plot_num_evicted_and_transferred_varying_capacity(data):
    xticks = []
    n_evicted, n_transfer = [], []
    for d in sorted(data, key=lambda x: x['cfg']['capacity']):
        if d['cfg']['capacity'] not in xticks:
            xticks.append(d['cfg']['capacity'])
        master_type = d['cfg']['master_type']
        if master_type == 'lfu':
            continue
        num_evicted = np.mean([i['master']['num_evicted'][1] for i in d['data']])
        num_transferred = np.mean([i['master']['num_transferred'][1] for i in d['data']])
        n_evicted.append(num_evicted)
        n_transfer.append(num_transferred)
    plt.clf()
    plt.plot(xticks, n_evicted, label='evicted')
    plt.plot(xticks, n_transfer, label='transferred')
    plt.xticks(xticks)
    plt.xlabel('Capacity per node (10 nodes in total)')
    plt.ylabel('Number of objects')
    plt.legend()
    plt.tight_layout()
    plt.savefig('experiment/figures/lp-evict-transfer-cap.pdf',
                bbox_inches='tight',
                format='pdf')


def compare_tx_data_varying_node(data):
    xticks = []
    mean, yerr = {}, {}
    for d in sorted(data, key=lambda x: x['cfg']['n_nodes']):
        if d['cfg']['n_nodes'] not in xticks:
            xticks.append(d['cfg']['n_nodes'])
        master_type = d['cfg']['master_type']
        mean.setdefault(master_type, []).append(np.mean([np.sum([j[1]
                                                                 for j in i['slave']['tx_raw']])
                                                         for i in d['data']]))
        yerr.setdefault(master_type, []).append(np.mean([np.std([j[1]
                                                                 for j in i['slave']['tx_raw']])
                                                         for i in d['data']]))
    plot_error_bar(xticks, mean, yerr,
                   xlabel='Number of nodes',
                   ylabel='Number of objects transferred',
                   markersize=2,
                   capsize=3,
                   save_path='experiment/figures/transfer-comp-node.pdf')


def compare_tx_data_varying_capacity(data):
    xticks = []
    mean, yerr = {}, {}
    for d in sorted(data, key=lambda x: x['cfg']['capacity']):
        if d['cfg']['capacity'] not in xticks:
            xticks.append(d['cfg']['capacity'])
        master_type = d['cfg']['master_type']
        mean.setdefault(master_type, []).append(np.mean([np.sum([j[1]
                                                                  for j in i['slave']['tx_raw']])
                                                         for i in d['data']]))
        yerr.setdefault(master_type, []).append(np.mean([np.std([j[1]
                                                                 for j in i['slave']['tx_raw']])
                                                         for i in d['data']]))
    plot_error_bar(xticks, mean, yerr,
                   xlabel='Capacity per node (10 nodes in total)',
                   ylabel='Number of objects transferred',
                   markersize=2,
                   capsize=3,
                   save_path='experiment/figures/transfer-comp-cap.pdf')


def compare_num_evicted_varying_node(data):
    xticks = []
    mean = {}
    for d in sorted(data, key=lambda x: x['cfg']['n_nodes']):
        if d['cfg']['n_nodes'] not in xticks:
            xticks.append(d['cfg']['n_nodes'])
        master_type = d['cfg']['master_type']
        if master_type == 'lfu':
            mean.setdefault(master_type, []).append(np.mean([i['master']['evict_time'][0]
                                                             for i in d['data']]))
        else:
            mean.setdefault(master_type, []).append(np.mean([i['master']['num_evicted'][1]
                                                            for i in d['data']]))
    plot_line(xticks, mean,
              xlabel='Number of nodes',
              ylabel='Number of objects evicted',
              save_path='experiment/figures/evicted-comp-node.pdf')


def compare_num_evicted_varying_capacity(data):
    xticks = []
    mean = {}
    for d in sorted(data, key=lambda x: x['cfg']['capacity']):
        if d['cfg']['capacity'] not in xticks:
            xticks.append(d['cfg']['capacity'])
        master_type = d['cfg']['master_type']
        if master_type == 'lfu':
            mean.setdefault(master_type, []).append(np.mean([i['master']['evict_time'][0]
                                                             for i in d['data']]))
        else:
            mean.setdefault(master_type, []).append(np.mean([i['master']['num_evicted'][1]
                                                            for i in d['data']]))
    plot_line(xticks, mean,
              xlabel='Capacity per node (10 nodes in total)',
              ylabel='Number of objects evicted',
              save_path='experiment/figures/evicted-comp-cap.pdf')


def plot_line(xticks, y, xlabel, ylabel, save_path, ylim=()):
    plt.clf()
    for k in y:
        plt.plot(xticks, y[k], label=k,)
    plt.xticks(xticks)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if ylim:
        plt.ylim(ylim)
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, bbox_inches='tight', format='pdf')


def plot_error_bar(xticks, y, yerr, xlabel, ylabel, save_path,
                   capsize=5, markersize=3, ylim=()):
    plt.clf()
    for k in y:
        print(k, y[k], yerr[k])
        plt.errorbar(xticks, y[k], yerr=yerr[k], label=k,
                     fmt='-o', markersize=markersize, capsize=capsize)
    plt.xticks(xticks)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if ylim:
        plt.ylim(ylim)
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, bbox_inches='tight', format='pdf')


if __name__ == '__main__':
    data = find_data(lambda x: x['n_nodes'] * x['capacity'] == 100
                               and x['cache_bw'][1] == 10
                               #and x['load_cfg'][-3]['a'] == 0.8
                     )
    # plot_num_evicted_and_transferred_varying_node(data)
    # plot_lp_invocations_varying_node(data)
    #compare_algo_runtime(data, 'n_nodes')
    #compare_hit_rate(data, 'n_nodes')
    # compare_rx_dropped(data, 'n_nodes')
    # compare_hit_rate_varying_node(data)
    # compare_wait_time_varying_node(data)
    # compare_rx_dropped_varying_node(data)

    # compare_tx_data_varying_node(data)
    # compare_rx_data_varying_node(data)
    # compare_num_evicted_varying_node(data)
    #
    data = find_data(lambda x: x['n_nodes'] == 100
                           and x['cache_bw'][1] == 10
                           #and x['load_cfg'][-3]['a'] == 0.8
                 )
    #compare_algo_runtime(data, 'capacity')
    compare_hit_rate(data, 'capacity')
    # compare_rx_dropped(data, 'capacity')
    # plot_lp_invocations_varying_capacity(data)

    # compare_algo_runtime_varying_capacity(data)
    # compare_hit_rate_varying_capacity(data)
    # compare_wait_time_varying_node(data)

    # compare_rx_dropped_varying_capacity(data)
    # compare_rx_time_varying_capacity(data)
    # compare_wait_time_varying_capacity(data)
    # compare_tx_data_varying_capacity(data)

    # plot_num_evicted_and_transferred_varying_capacity(data)
    # compare_num_evicted_varying_capacity(data)
    # compare_wait_time_varying_capacity(data)

    data = find_data(lambda x: x['n_nodes'] == 20
                       and x['capacity'] == 5
                       and x['cache_bw'][1] in range(1, 10, 2)
                       #and x['load_cfg'][-3]['a'] == 0.8
             )
    #compare_hit_rate(data, 'cache_bw')



