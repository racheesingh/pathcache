#!/usr/bin/python
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings

TOTAL_MMT_PER_SRC = 20
all_graphs_ripe = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_RIPE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_RIPE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_RIPE, f ) for f in files ]
ripe_src_dst_pairs = 0
ripe_violations = []
ripe_all = []
for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    overall_origins = {}
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        for src in origin_dict:
            if src not in overall_origins:
                overall_origins[src] = 0
            overall_origins[src] += origin_dict[src]
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        origin_dict_new = {}
        for src in origin_dict:
            origin_dict_new[src] = \
                                   (float(origin_dict[src])*TOTAL_MMT_PER_SRC)/overall_origins[src]
        gr.ep.origin[e] = origin_dict_new
    eprop_prob = gr.new_edge_property("float")
    gr.ep.prob = eprop_prob
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        prob_src = 1.0/len(overall_origins)
        e_prob = 0
        for src in origin_dict:
            e_prob += (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC
        gr.ep.prob[e] = e_prob
    all_graphs_ripe[asn] = gr

all_graphs_caida = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_CAIDA ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_CAIDA, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_CAIDA, f ) for f in files ]
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing CAIDA graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    overall_origins = {}
    for edge in gr.edges_iter(data=True):
        origin_dict = edge[-1]['origin']
        for src in origin_dict:
            if src not in overall_origins:
                overall_origins[src] = 0
            overall_origins[src] += origin_dict[src]
        for edge in gr.edges_iter(data=True):
            origin_dict = edge[-1]['origin']
            origin_dict_new ={}
            for src in origin_dict:
                origin_dict_new[src] = (float(origin_dict[src])*TOTAL_MMT_PER_SRC)/overall_origins[src]
            gr[edge[0]][edge[1]]['origin'] = origin_dict_new
        prob_src = 1.0/len(overall_origins)
        for edge in gr.edges_iter():
            origin_dict = edge[-1]['origin']
            e_prob = 0
            for src in in origin_dict:
                e_prob += = (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC)
            gr[edge[0]][edge[1]]['prob'] = e_prob
    all_graphs_caida[asn] = gr
                                   
all_graphs_iplane = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_IPLANE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_IPLANE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_IPLANE, f ) for f in files ]
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing Iplane graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    overall_origins = {}
    for edge in gr.edges_iter(data=True):
        origin_dict = edge[-1]['origin']
        for src in origin_dict:
            if src not in overall_origins:
                overall_origins[src] = 0
            overall_origins[src] += origin_dict[src]
        for edge in gr.edges_iter(data=True):
            origin_dict = edge[-1]['origin']
            origin_dict_new ={}
            for src in origin_dict:
                origin_dict_new[src] = (float(origin_dict[src])*TOTAL_MMT_PER_SRC)/overall_origins[src]
            gr[edge[0]][edge[1]]['origin'] = origin_dict_new
        prob_src = 1.0/len(overall_origins)
        for edge in gr.edges_iter():
            origin_dict = edge[-1]['origin']
            e_prob = 0
            for src in in origin_dict:
                e_prob += = (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC)
            gr[edge[0]][edge[1]]['prob'] = e_prob
    all_graphs_iplane[asn] = gr
'''
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]
all_graphs = {}

for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    overall_origins = {}
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        for src in origin_dict:
            if src not in overall_origins:
                overall_origins[src] = 0
            overall_origins[src] += origin_dict[src]
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        origin_dict_new = {}
        for src in origin_dict:
            origin_dict_new[src] = \
                                   (float(origin_dict[src])*TOTAL_MMT_PER_SRC)/overall_origins[src]
        gr.ep.origin[e] = origin_dict_new
    eprop_prob = gr.new_edge_property("float")
    gr.ep.prob = eprop_prob
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        prob_src = 1.0/len(overall_origins)
        e_prob = 0
        for src in origin_dict:
            e_prob += (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC
        gr.ep.prob[e] = e_prob
    all_graphs[int(asn)] = gr
'''

def dfs_paths(gr, src_node, dst_node):
    visited = set()
    stack = [(src_node, [src_node])]
    while stack:
        (vertex, path) = stack.pop()
        visited.add(vertex)
        for next in set(vertex.out_neighbours()):
            if next in visited:
                continue
            if next == dst_node:
                yield path + [next]
            else:
                stack.append((next, path + [next]))

def get_path_prob(p, gr):
    prob = 0
    for first, second in zip(p, p[1:]):
        e = gr.edge(first, second)
        prob += math.log(gr.ep.prob[e])
    return prob

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
        #first = next(src_dst_paths, None)
        #if not first:
        #    print "Could not find a path from data plane measurements."
        #    src_dst_paths = dfs_paths(gr, src_node, dst_node)
        paths = []
        count = 0
        for p in src_dst_paths:
            count += 1
            if count > 10000:
                break
            pr = get_path_prob(p, gr)
            paths.append((pr, p))
        paths_sorted = sorted(paths, key=lambda x:x[0], reverse=True)
        paths_sorted = paths_sorted[:10]
        final_paths = []
        for prob, p in paths_sorted:
            path_ases = [gr.vp.asn[x] for x in p]
            final_paths.append((prob, path_ases))
        return final_paths
    else:
        return []

def get_most_probable_path(src, dst, threshold=2):
    paths = get_path(src, dst)
    paths = sorted(paths, key=lambda x: x[0], reverse=True)
    paths = paths[:threshold]
    final_paths = []
    for p in paths:
        final_paths.append(p[1])
    return final_paths

combined_plens = []
for asn, gr in all_graphs.iteritems():
    for v in gr.vertices():
        src_asn = gr.vp.asn[v]
        if src_asn == asn: continue
        path = get_most_probable_path(src_asn, int(asn))
        combined_plens.append(len(path))

print "COMBINED percentage of violations", percent_violations_combined
