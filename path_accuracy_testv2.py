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

def fetch_json( offset=0 ):
    data = []
    timestamp  = int( (datetime.datetime.utcnow() - \
                       datetime.datetime( 1970, 1, 1 ) ).total_seconds() ) - ( 60 * 60 * 24 * 8)
    
    #api_args = dict( offset=offset, use_iso_time="true", description__startswith="PCExitAccuracy",
    #                 start_time__gt="%s" % timestamp, type="traceroute" )
    api_args = dict( offset=offset, use_iso_time="true", description__startswith="ACCURACY_PC",
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

def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
    return False

def path_cache(src, dst, threshold=2):
    if not path_in_cache(src, dst):
        return []
    paths = get_most_probable_path(src, dst, threshold=threshold)
    #hops = []
    #for p in paths:
    #    hops.extend(p)
    #hops = list(frozenset(hops))
    #return hops
    return paths

def get_paths_bgp_sim( pairs):
    destinations = list()
    query_1, query_2 = "", "-q "
    for pair in pairs:
        src, dst = (pair.split("-")[0]).split("AS")[1], (pair.split("-")[1]).split("AS")[1]
        if src not in destinations:
            destinations.append(src)
            query_1 += src + " "
        if dst not in destinations:
            destinations.append(dst)
            query_1 += dst + " "
        query_2 += src + " " + dst + " "
    ip = '127.0.0.1'
    port = 11000
    buffer_size = 1000000
    query = query_1 + query_2 + "<EOFc>"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(query)
    data = ""
    result = dict()
    while True:
        d = s.recv(buffer_size)
        data += d
        if len(d) == 0:
            break
        if "<EOFs>" in d:
            break
    s.close()
    data = data.split("-\n<EOFs>")[0]
    arr = data.split("-\n")
    arr = data.split("-\n")
    return arr

overestimate_list_pc = []
underestimate_list_pc = []
overestimate_list_bgp = []
underestimate_list_bgp = []

threshold=5
total = 0
exact=0
same = 0
shorter = 0
longer = 0
for msm in msms:
    data = parse.parse_msm_trcrt(msm)
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
        #real_path = set(hops)
        if not hops:
            continue
        pc_paths = path_cache(int(src_asn), int(dst_asn), threshold)
        #arr = get_paths_bgp_sim(['AS%s-AS%s' % (src_asn, dst_asn)])
        #bgpsim_path = arr[0].split(':')[1].strip().split('\n')
        #try:
        #    bgpsim_path = set([int(x) for x in bgpsim_path])
        #except ValueError:
        #    pdb.set_trace()
        #    continue
        #overestimate = len(bgpsim_path - real_path)
        #underestimate = len(real_path - bgpsim_path)
        #overestimate_list_bgp.append(overestimate)
        #underestimate_list_bgp.append(underestimate)
        #total += 1
        #if real_path == pc_path:
        #    exact += 1
        #elif len(real_path) == len(pc_path):
        #    same += 1
        #elif len(real_path) > len(pc_path):
        #    shorter += 1
        #else:
        #    longer += 1
        #overestimate = len(pc_path - real_path)
        #underestimate = len(real_path - pc_path)
        #overestimate_list_pc.append(overestimate)
        #underestimate_list_pc.append(underestimate)
        print pc_paths, hops 

print total, exact, same, shorter, longer
pdb.set_trace()
'''with open("cipollino-verify/over_estimate_exit_dest_pc", "w") as fi:
    json.dump(overestimate_list_pc, fi)

with open("cipollino-verify/under_estimate_exit_dest_pc", "w") as fi:
    json.dump(underestimate_list_pc, fi)

with open("cipollino-verify/over_estimate_exit_dest_bgp", "w") as fi:
    json.dump(overestimate_list_bgp, fi)

with open("cipollino-verify/under_estimate_exit_dest_bgp", "w") as fi:
    json.dump(underestimate_list_bgp, fi)
'''
