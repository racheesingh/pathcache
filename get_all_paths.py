#!/usr/bin/python
import settings
import os
from graph_tool.all import *
import pdb

def load_dest_graphs():
    files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
              if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
    files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]
    all_graphs = {}
    for f in files:
        asn_to_id = {}
        asn = f.split( '/' )[ -1 ].split('.')[0]
        gr = load_graph(f, fmt="gt")
        all_graphs[int(asn)] = gr
    return all_graphs

all_graphs = load_dest_graphs()
print "loaded all graphs.."

def all_paths(gr, src_node, dst_node, memo_dict = None):
    src_asn = gr.vp.asn[src_node]
    dst_asn = gr.vp.asn[dst_node]
    print src_asn, dst_asn
    if memo_dict is None:
        memo_dict = dict()
    if gr.vp.asn[src_node] == gr.vp.asn[dst_node]:
        return frozenset([(gr.vp.asn[src_node],)])
    pair = (gr.vp.asn[src_node], gr.vp.asn[dst_node])
    if pair in memo_dict: # Is answer memoized already?
        return memo_dict[pair]
    result = set()
    for new_source in src_node.out_neighbours():
        #pdb.set_trace()
        paths = all_paths(gr, new_source, dst_node, memo_dict=memo_dict)
        for path in paths:
            #path = [src_asn] + path
            path = (src_asn,) + path
            result.add(path)
    result = frozenset(result)
    memo_dict[(src_asn, dst_node)] = result
    return result

def all_paths_new(gr, src_node, dst_node):
    result = []
    for edge in dfs_iterator(gr, src_node):
        if gr.vp.asn[edge.source] == gr.vp.asn[src_node]:
            path = []
        path.append(gr.vp.asn[edge.source])
        if gr.vp.asn[edge.target] == gr.vp.asn[dst_node]:
            path.append(gr.vp.asn[edge.target])
            print path
            result.append([x for x in path])
    return frozenset(result)

def dfs_paths(gr, src_node, dst_node):
    stack = [(src_node, [src_node])]
    while stack:
        (vertex, path) = stack.pop()
        for next in set(vertex.out_neighbours()) - set(path):
            if next == dst_node:
                yield path + [next]
            else:
                stack.append((next, path + [next]))

def get_path( src, dst):
    if dst in all_graphs:
        gr = all_graphs[dst]
        # Find all paths from src to dst in this graph
        src_node = find_vertex(gr, gr.vp.asn, int(src))
        if not src_node:
            return None
        src_node = src_node[0]
        dst_node = find_vertex(gr, gr.vp.asn, int(dst))
        assert dst_node
        dst_node = dst_node[0]
        src_dst_paths = dfs_paths(gr, src_node, dst_node)
        paths = []
        for p in src_dst_paths:
            paths.append([gr.vp.asn[x] for x in p])
        return paths
    else:
        return None

pdb.set_trace()
paths = get_path(5719,3333)
print paths
paths = get_path(2119, 4608)
paths = get_path(3265, 3333)
paths = get_path(5719, 8075)
