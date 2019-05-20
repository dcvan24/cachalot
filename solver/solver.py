#!/usr/bin/env sage --python
"""

"""
import sys
import json
import numpy as np

from networkx.readwrite import json_graph
from collections import namedtuple

from sage.all import *
from sage.numerical.mip import MixedIntegerLinearProgram

CacheClass = namedtuple('CacheClass', 'id size util')
CacheNode = namedtuple('CacheNode', 'id capacity')


def generate_random_graph(n, p, scale_factor):
    print('Generating graph with {} vertices'.format(n))
    g = graphs.RandomGNP(n, p)
    cache_nodes = dict([(v, CacheNode(v, 10 ** scale_factor))
                        for v in g.vertices()])
    for u, v, _ in g.edges():
        g.set_edge_label(u, v, np.random.uniform(0, 1))
    return g, cache_nodes


def generate_cache_classes(n, g):
    print('Generating {} classes'.format(n))
    size = np.random.uniform(1, 10)
    util_total = np.random.uniform(10, 50)
    size, util = distribute_cache_classes(g, size, util_total)
    return [CacheClass(id=k, size=size, util=util) for k in range(n)]


def distribute_cache_classes(g, size, util):
    a = np.random.uniform(0, 10, len(g.vertices()))
    a /= np.sum(a)
    return (dict((v, a[i] * size) for i, v in enumerate(g.vertices())),
            dict((v, a[i] * util) for i, v in enumerate(g.vertices())))


def adjust_cache_size(nodes, classes):
    for i, n in nodes.items():
        total_size = sum(c.size[i] for c in classes)
        if total_size > n.capacity:
            for c in classes:
                c.size[i] *= n.capacity/total_size


def parse_graph(g_data, thres):
    nx_g = json_graph.node_link_graph(g_data)
    g = Graph()
    for n, d in nx_g.nodes_iter(data=True):
        g.add_vertex(str(n))
        g.set_vertex(str(n), CacheNode(str(n), d['capacity'] * thres))
    for u, v, d in nx_g.edges_iter(data=True):
        g.add_edge(str(u), str(v), d['cost'])
    return g


def parse_classes(class_data):
    return dict((c['id'], CacheClass(c['id'], c['size'], c['util'])) for c in class_data)


class CacheOptimizer(object):

    def __init__(self, g):
        self._problem = MixedIntegerLinearProgram(maximization=True)
        # topology of the caching system
        self._g = g
        self._t = self._problem.new_variable(nonnegative=True)
        self._e = self._problem.new_variable(nonnegative=True)

    def optimize(self, classes):
        g, p = self._g, self._problem
        t, e = self._t, self._e

        objective = 0
        for i, c in classes.items():
            util_delta = p.sum(t[i, u, v] * ((c.util[v] if v in c.util else 0)
                                             - (c.util[u] if u in c.util else 0))
                               + t[i, v, u] * ((c.util[u] if u in c.util else 0)
                                               - (c.util[v] if v in c.util else 0))
                               for u, v, _ in g.edges())
            transfer_cost = p.sum(d * (t[i, u, v] + t[i, v, u]) for u, v, d in g.edges())
            evict_cost = p.sum(e[i, v] * (c.util[v] if v in c.util else 0)
                               for v in g.vertices())
            objective += util_delta - 10 * transfer_cost - .01 * evict_cost
        p.set_objective(objective)

        for i, c in classes.items():
            for v in g.vertices():
                evicted = e[i, v]
                moved = p.sum(t[i, v, u] for u in g.vertices()
                              if g.has_edge(u, v) or g.has_edge(v, u))
                p.add_constraint(evicted + moved, max=c.size[v] if v in c.size else 0)

        for n in self._g.get_vertices().values():
            space_used = sum(c.size[n.id] if n.id in c.size else 0
                             for c in classes.values())
            for v in g.vertices():
                if n.id != v and not g.has_edge(n.id, v) and not g.has_edge(v, n.id):
                    continue
                space_used += p.sum((t[c.id, v, n.id] if v == n.id
                                     else t[c.id, v, n.id] - t[c.id, n.id, v])
                                    for c in classes.values())
            space_used -= sum(e[c.id, n.id] for c in classes.values())
            p.add_constraint(space_used, max=n.capacity)

        # p.show()
        p.solve()
        t, e = p.get_values([t, e])
        util_delta = 0
        class_total = {}
        output = dict(transfer=[], evict=[])
        for (i, u, v), amt in t.items():
            if amt > 0:
                c = classes[i]
                if amt > c.size[u] and abs(amt - c.size[u]) > 1e-6:
                    raise ValueError('[Wrong] %f %f'%(amt, c.size[u]))
                if not g.has_edge(u, v):
                    print('False: %s, %s, %s, %f'%(i, u, v, amt))
                    continue
                util_v = c.util[v] if v in c.util else 0
                util_u = c.util[u] if u in c.util else 0
                delta = amt * (util_v - util_u)
                # print('%f of class %s moved from %s to %s (%f)'%(amt, i, u, v, delta))
                if i not in class_total:
                    class_total[i] = 0
                class_total[i] += amt
                util_delta += delta
                output['transfer'].append(dict(class_id=int(i), src=int(u), dst=int(v), amount=amt))
        total_evicted = 0
        for (i, j), amt in e.items():
            if amt > 0:
                total_evicted += amt
                class_total.setdefault(i, 0)
                class_total[i] += amt
                #util_delta -= classes[i].util[j]
                output['evict'].append(dict(class_id=int(i), node_id=int(j), amount=amt))
                # print('%f of class %s are evicted from node %s'%(amt, i, j))
        for i, total in class_total.items():
            actual_class_total = sum(classes[i].size.values())
            if total > actual_class_total and abs(total - actual_class_total) > 1e-6:
                # print('[Wrong] %f %f'%(total, actual_class_total))
                raise ValueError('')
        # print('Total evicted: %f'%total_evicted)
        # print('Utility delta: %f'%util_delta)
        return output

if __name__ == '__main__':
    sage_fn = sys.argv[1]
    data = json.load(open(sage_fn, 'r'))
    g = parse_graph(data['g'], data['threshold'])
    classes = parse_classes(data['classes'])
    optimizer = CacheOptimizer(g)
    output = optimizer.optimize(classes)
    print(json.dumps(output))



# import datetime
# for scale_factor in range(2, 6):
#     start_time = datetime.datetime.now()
#     n_site = 10
#     g, cache_nodes = generate_random_graph(n_site, .5, scale_factor)
#     classes = generate_cache_classes(10 ** scale_factor, g)
#     adjust_cache_size(cache_nodes, classes)
#
#     for n in cache_nodes.values():
#         total_size = sum(c.size[n.id] for c in classes)
#         print('%d - space used: %f, capacity: %f'%(n.id, total_size, n.capacity))
#
#     # print('Nodes:')
#     # for id, n in cache_nodes.items():
#     #     print('\t%d: %f'%(id, n.capacity))
#     #
#     # print('Classes:')
#     # for c in classes:
#     #     for v in g.vertices():
#     #         print('\t%d, %d: %f'%(c.id, v, c.size[v] * c.util[v]))
#
#     print('Total capacity: %f'%sum(n.capacity for n in cache_nodes.values()))
#     print('Total class size: %f'%(sum(sum(c.size.values()) for c in classes)))
#     optimizer = CacheOptimizer(g, cache_nodes)
#     optimizer.optimize(classes)
#     print('Time spent: %s'%(datetime.datetime.now() - start_time))






