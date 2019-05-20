import os
import json
import numpy as np
import matplotlib.pyplot as plt


def get_xlabel(xlabel):
    if xlabel == 'n_nodes':
        return 'Number of nodes'
    elif xlabel == 'capacity':
        return 'Capacity'
    elif xlabel == 'cache_bw':
        return 'Bandwidth range'


def plot_line(x, y, xlabel, ylabel, save_path, ylim=()):
    plt.clf()
    for k in y:
        plt.plot(x, y[k], label=k, )
    plt.xticks(np.arange(0, len(x), len(x)/10))
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if ylim:
        plt.ylim(ylim)
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, bbox_inches='tight', format='pdf')


def plot_error_bar(x, y, yerr, xlabel, ylabel, save_path,
                   capsize=5, markersize=3, ylim=()):
    plt.clf()
    for k in y:
        print(k, y[k], yerr[k])
        plt.errorbar(x, y[k], yerr=yerr[k], label=k,
                     fmt='-o', markersize=markersize, capsize=capsize)
    plt.xticks(x)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if ylim:
        plt.ylim(ylim)
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, bbox_inches='tight', format='pdf')


def read_data():
    data = []
    for run_dir in os.listdir('results'):
        run_dir = 'results/%s'%run_dir
        cfg = json.load(open('%s/cfg.json'%run_dir, 'r'))
        data.append(dict(cfg=cfg,
                         data=[json.load(open('%s/%s'%(run_dir, fn)))
                               for fn in os.listdir(run_dir)
                               if fn.startswith('result')]))
    return data


def compare_algo_runtime(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        for action in ('insert', 'evict', 'retrieve'):
            vals = [np.mean([j[1][4] for j in i['slave']['%s_runtime'%action]])
                    for i in d['data']]
            y.setdefault(algo, {}).setdefault(action, []).append(np.mean(vals) * 1e3)
            yerr.setdefault(algo, {}).setdefault(action, []).append(np.std(vals) * 1e3)
    ls, colors = ('-.', '-'), ('#035a9a', '#40830c', '#ff5921')
    plt.clf()
    for i, algo in enumerate(y):
        for j, action in enumerate(y[algo]):
            plt.errorbar(x, y[algo][action], yerr=yerr[algo][action],
                         label='%s %s'%(algo, action),
                         ls=ls[i], color=colors[j],
                         capsize=3)
    plt.xticks(x, x)
    plt.xlabel(get_xlabel(xlabel))
    plt.ylabel('Avg. algorithm runtime per invocation (milliseconds)')
    lgd = plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, bbox_extra_artists=(lgd,), bbox_inches='tight', format='pdf')


def compare_hit_rate(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [np.sum([j[1] for j in i['slave']['num_hits']]) for i in d['data']]
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    for k in dict(y):
        y[k] = np.array(y[k]) * 100/d['cfg']['load'][0]
        yerr[k] = np.array(yerr[k]) * 100/d['cfg']['load'][0]
    plot_error_bar(x, y, yerr,
                   markersize=2, capsize=3, ylim=(0, 100),
                   xlabel=get_xlabel(xlabel),
                   ylabel='Hit rate (%)',
                   save_path=save_path)


def compare_bytes_transferred(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [np.sum([j[1][1] for j in i['slave']['tx_data']]) for i in d['data']]
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    plot_error_bar(x, y, yerr,
                   xlabel=get_xlabel(xlabel),
                   ylabel='Total number of objects transferred',
                   markersize=2, capsize=3, save_path=save_path)


def compare_bytes_executed(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [np.sum([j[1][1] for j in i['slave']['exec_data']]) for i in d['data']]
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    plot_error_bar(x, y, yerr,
                   xlabel=get_xlabel(xlabel),
                   ylabel='Total number of objects executed',
                   markersize=2, capsize=3, save_path=save_path)


def compare_execute_time(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [np.sum([j[1][1] for j in i['slave']['exec_time']]) /
                d['cfg']['load'][0]
                for i in d['data']]
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    plot_error_bar(x, y, yerr,
                   xlabel=get_xlabel(xlabel),
                   ylabel='Avg. execution time per object',
                   markersize=2, capsize=3, save_path=save_path)


def compare_transfer_time(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    t_pct, e_pct, l_pct = [], [], []
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [np.sum([j[1][1] for j in i['slave']['tx_transfer_time']]) /
                d['cfg']['load'][0]
                for i in d['data']]
        transfer_pct = np.mean([np.sum([j[1][0] for j in i['slave']['tx_data']])
                                /d['cfg']['load'][0]
                for i in d['data']])
        execute_pct = np.mean(
            [np.sum([j[1][0] for j in i['slave']['exec_time']]) /
                d['cfg']['load'][0]
                for i in d['data']]
        )
        local_pct = 1 - transfer_pct - execute_pct
        t_pct.append(transfer_pct)
        e_pct.append(execute_pct)
        l_pct.append(local_pct)
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    print('Transfer: %s'%t_pct)
    print('Execute: %s'%e_pct)
    print('Local: %s'%l_pct)
    plot_error_bar(x, y, yerr,
                   xlabel=get_xlabel(xlabel),
                   ylabel='Avg. transfer time per object',
                   markersize=2, capsize=3, save_path=save_path)


def compare_total_time(data, xlabel, save_path):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [(np.sum([j[1][1] for j in i['slave']['tx_transfer_time']]) +
                 np.sum([j[1][1] for j in i['slave']['exec_time']])) / \
                ((np.sum([j[1][0] for j in i['slave']['tx_transfer_time']]) +
                 np.sum([j[1][0] for j in i['slave']['exec_time']])))
                for i in d['data']]
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    plot_error_bar(x, y, yerr,
                   xlabel=get_xlabel(xlabel),
                   ylabel='Avg. wait time per object',
                   markersize=2, capsize=3, save_path=save_path)


def compare_wait_time(data, xlabel, save_path, ylim=None):
    x, y, yerr = [], {}, {}
    for d in sorted(data, key=lambda x: x['cfg'][xlabel]):
        xtick = d['cfg'][xlabel]
        if xlabel == 'cache_bw':
            xtick = xtick[1]
        if xtick not in x:
            x.append(xtick)
        algo = d['cfg']['slave_type']
        vals = [np.sum(j[2] for j in i['wait_time']) /
                np.sum([j[1] for j in i['wait_time']])
                for i in d['data']]
        print(np.mean([np.sum([j[1] for j in i['wait_time']])
                for i in d['data']]))
        y.setdefault(algo, []).append(np.mean(vals))
        yerr.setdefault(algo, []).append(np.std(vals))
    plot_error_bar(x, y, yerr,
                   xlabel=get_xlabel(xlabel),
                   ylabel='Avg. wait time per object',
                   markersize=2, capsize=3, save_path=save_path,
                   ylim=ylim)


def plot_waittime_timeline(data, xlabel, x_val, seq_no, save_path):
    x, y = [], {}
    for d in data:
        algo = d['cfg']['slave_type']
        if d['cfg'][xlabel] != x_val:
            continue
        z = [sorted(i['wait_time'], key=lambda x: x[0])[seq_no][-1] for i in d['data']]
        y[algo] = [np.mean([j[i] for j in z]) for i in range(len(z[0]))]
        x = np.arange(len(y[algo]))
    plot_line(x, y,
              xlabel='Object sequence no.',
              ylabel='Wait time',
              save_path=save_path)


def plot_avg_waittime_per_key(data, xlabel, x_val, algo, save_path, top_n=1000):
    x, y = set(), []
    for d in data:
        if d['cfg']['slave_type'] != algo or d['cfg'][xlabel] != x_val:
            continue
        for i in d['data']:
            x.update([int(j) for j in i['wait_time_per_key'].keys()])
        x = sorted(x)
        y = [np.mean([j['wait_time_per_key'][str(i)][4]
                      if str(i) in j['wait_time_per_key'] else 0
                      for j in d['data']]) for i in x]
    total_time, n_obj = [], []
    for i in x[:top_n]:
        n_obj.append(np.mean([j['wait_time_per_key'][str(i)][0] for j in d['data']]))
        total_time.append(np.mean([j['wait_time_per_key'][str(i)][1] for j in d['data']]))
    print(np.sum(total_time)/np.sum(n_obj))
    # print(y)
    plt.bar(x[:top_n], y[:top_n], width=1)
    plt.xlabel('Key')
    plt.ylabel('Avg. wait time')
    plt.legend()
    plt.savefig(save_path, format='pdf', bbox_inches='tight')


def plot_task_distribution(data, save_path, top_n=1000):
    x, y = set(), []
    d = data[0]['data']
    for i in d:
        i['task_dist'] = dict(i['task_dist'])
        x.update([j for j in i['task_dist'].keys()])
    x = sorted(x)
    y = [np.mean([j['task_dist'][i] if i in j['task_dist'] else 0 for j in d]) for i in x]
    print(np.sum(y[:top_n])/np.sum(y))
    print(y)
    plt.bar(x[:top_n], y[:top_n], width=1)
    plt.xlabel('Key')
    plt.ylabel('Frequency')
    plt.legend()
    plt.savefig(save_path, format='pdf', bbox_inches='tight')



