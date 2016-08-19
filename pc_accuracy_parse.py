#!/usr/bin/python
import socket
import mkit.ripeatlas.probes as ripeprobes
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import pdb
import urllib
import urllib2
import mkit.inference.ip_to_asn as ip2asn
import json
import datetime
import settings
import os
import math
from graph_tool.all import *
import os
import settings

API_HOST = 'https://atlas.ripe.net'
API_MMT_URI = 'api/v1/measurement'

def fetch_json( offset=0 ):
    data = []
    timestamp  = int( (datetime.datetime.utcnow() - \
                       datetime.datetime( 1970, 1, 1 ) ).total_seconds() ) - ( 60 * 60 * 24 * 2)
    
    api_args = dict( offset=offset, use_iso_time="true",
                     description__startswith="ACCURACY_STATIC_PC_POSTER3",
                     start_time__gt="%s" % timestamp, type="traceroute" )
    url = "%s/%s/?%s" % ( API_HOST, API_MMT_URI, urllib.urlencode( api_args ) )
    print url
    response = urllib2.urlopen( url )
    data = json.load( response )
    return data

count = 0
msms = []
while( 1 ):
    try:
        data = fetch_json( offset=count*100 )
    except urllib2.HTTPError:
        break
    if not data[ 'objects' ]:
        break
    for d in data[ 'objects' ]:
        if d[ 'dst_asn' ]:
            msms.append(d[ 'msm_id' ])
        else:
            try:
                msms.append( d[ 'msm_id' ] )
            except:
                pass
    count += 1

print len(msms)
all_graphs = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]

TOTAL_MMT_PER_SRC = 20
for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "Graph for ASN", asn
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
        
    '''
    eprop_prob = gr.new_edge_property("float")
    gr.ep.prob = eprop_prob
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        prob_src = 1.0/len(overall_origins)
        e_prob = 0
        for src in origin_dict:
            e_prob += (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC
        gr.ep.prob[e] = e_prob
    '''
    all_graphs[int(asn)] = gr

def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
    return False

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

def get_edge_probs(paths, gr):
    overall_origins = {}
    edge_origins = {}
    for path in paths:
        for first, second in zip(path, path[1:]):
            e = gr.edge(first, second)
            origin_dict = gr.ep.origin[e]
            for src in origin_dict:
                if src not in overall_origins:
                    overall_origins[src] = 0
                overall_origins[src] += origin_dict[src]
    for path in paths:
        for first, second in zip(path, path[1:]):
            e = gr.edge(first, second)
            origin_dict = gr.ep.origin[e]
            origin_dict_new = {}
            for src in origin_dict:
                origin_dict_new[src] = \
                (float(origin_dict[src])*TOTAL_MMT_PER_SRC)/overall_origins[src]
            edge_origins[e] = origin_dict_new
    edge_probs = {}
    for path in paths:
        for first, second in zip(path, path[1:]):
            e = gr.edge(first, second)
            origin_dict = edge_origins[e]
            prob_src = 1.0/len(overall_origins)
            e_prob = 0
            for src in origin_dict:
                e_prob += (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC
            edge_probs[e] = e_prob

    return edge_probs

def get_path_prob_new(edge_probs, p, gr):
    prob = 0
    for first, second in zip(p, p[1:]):
        e = gr.edge(first, second)
        prob += math.log(edge_probs[e])
    return prob

def get_path_prob_brand_new(gr, paths):
    nodes = []
    for path in paths:
        nodes.extend(path)
    nodes = list(frozenset(nodes))
    
    vfilt = gr.new_vertex_property('bool')
    for node in nodes:
        vfilt[node] = True
    sub_gr = GraphView(gr, vfilt)
    edge_probs = {}
    for node in nodes:
        nbrs = node.out_neighbours()
        total_trcrts = 0
        edges = []
        for nbr in nbrs:
            if nbr not in nodes: continue
            e = sub_gr.edge(node, nbr)
            edges.append(e)
            origin = gr.ep.origin[e]
            if e in edge_probs: pdb.set_trace()
            edge_probs[e] = 0
            for src in origin:
                total_trcrts += origin[src]
                edge_probs[e] += origin[src]
        for ed in edges:
            edge_probs[ed] = edge_probs[ed]/float(total_trcrts)
    path_probs = []
    for path in paths:
        prob = 1
        for first, second in zip(path, path[1:]):
            e = gr.edge(first, second)
            #prob += math.log(edge_probs[e])
            prob *= edge_probs[e]
        path_probs.append((prob, path))
    print "Prob of all paths", sum(x[0] for x in path_probs)
    return path_probs

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
        src_dst_paths_gen = dfs_paths(gr, src_node, dst_node)
        src_dst_paths = []
        for path in src_dst_paths_gen:
            src_dst_paths.append(path)
        # edge_probs = get_edge_probs(src_dst_paths, gr)
        paths = []
        count = 0
        #for p in src_dst_paths:
        #    count += 1
        #    if count > 50000:
        #        break
        #    pr = get_path_prob_brand_new(edge_probs, p, gr)
        #    paths.append((pr, p))
        #paths_sorted = sorted(paths, key=lambda x:x[0], reverse=True)
        #paths_sorted = paths_sorted[:10]

        path_probs = get_path_prob_brand_new(gr, src_dst_paths)
        #paths_sorted = sorted(path_probs, key=lambda x: x[0], reverse=True)
        final_paths = []
        for prob, p in path_probs:
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

path_comp = []
path_comp_dict = []
not_in_pc = []
for msm in msms:
    print "Parsing MSM", msm
    try:
        data = parse.parse_msm_trcrt(msm)
    except:
        pdb.set_trace()
        continue
    for d in data:
        src_asn = int(ripeprobes.get_probe_asn(d['prb_id']))
        dst_asn = int(ip2asn.ip2asn_bgp(d['dst_addr']))
        if not path_in_cache(src_asn, dst_asn): continue
            
        aslinks = asp.traceroute_to_aspath(d)
        if not aslinks['_links']: continue
        aslinks = ixp.remove_ixps(aslinks)
        hops = []
        for link in aslinks:
            hops.append(int(link['src']))
        hops.append(int(link['dst']))
        if hops[0] != src_asn or hops[-1] != dst_asn:
            print "Traceroute hops don't start from src or don't end in dest, skipping"
            continue
        pc_path = get_most_probable_path(src_asn, dst_asn, threshold=10)
        all_pc_paths = get_path(src_asn, dst_asn)
        if hops not in all_pc_paths:
            not_in_pc.append(hops)
        path_comp.append([(src_asn, dst_asn),pc_path, hops])
        path_comp_dict.append([(src_asn, dst_asn), pc_path, hops])
        
pdb.set_trace()        
with open("cipollino-verify/path_comp_accuracy_june23_new_500", "w") as fi:
    json.dump(path_comp, fi)
with open("cipollino-verify/path_comp_accuracy_dict_june23_new_500", "w") as fi:
    json.dump(path_comp_dict, fi)
with open("cipollino-verify/path_comp_accuracy_dict_not_found_june23_new_500", "w") as fi:
    json.dump(not_in_pc, fi)
