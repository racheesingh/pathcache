#!/usr/bin/python
from graph_tool.all import *
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import settings
from networkx.readwrite import json_graph
import sys
import traceback
import pickle
import pygeoip
import multiprocessing as mp
import networkx as nx
import urllib2
import json
import pdb
import urllib
import datetime
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import logging

settings.GRAPH_DIR = settings.GRAPH_DIR_RIPE
now = '-'.join( str( datetime.datetime.now() ).split() )

def are_siblings( as1, as2 ):
    print "Checking if siblings:", (as1, as2)
    print orgs[ as1 ], orgs[ as2 ]
    if as1 not in orgs or as2 not in orgs:
        return False
    return orgs[ as1 ] == orgs[ as2 ]

with open(settings.RIPE_MSMS) as fi:
    msms = json.load(fi)
msms = list(frozenset(msms))

def compute_dest_based_graphs(msms):
    dest_based_graphs = {}
    for msm in msms:
        info = parse.mmt_info(msm)
        # Start time should not be < Jan 1st 2016
        if info['start_time'] < 1451606400: continue
        if info['type']['af'] != 4: continue
        dst_asn = info['dst_asn']
        if not info['is_oneoff']:
            data = parse.parse_msm_trcrt(msm, count=3000)
        data = parse.parse_msm_trcrt(msm)
        if dst_asn in dest_based_graphs:
            G = dest_based_graphs[dst_asn][0]
            asn_to_id = dest_based_graphs[dst_asn][1]
        else:
            G = Graph()
            vprop_asn = G.new_vertex_property("int")
            G.vp.asn = vprop_asn
            eprop_type = G.new_edge_property("short")
            G.ep.type = eprop_type
            eprop_msm = G.new_edge_property("long")
            G.ep.msm = eprop_msm
            eprop_probe = G.new_edge_property("int")
            G.ep.probe = eprop_probe
            asn_to_id = {}
        for d in data:
            aslinks = asp.traceroute_to_aspath(d)
            if not aslinks['_links']: continue
            aslinks = ixp.remove_ixps(aslinks)
            for link in aslinks:
                if link['src'] == dst_asn:
                    break
                if link['src'] in asn_to_id:
                    v1 = G.vertex(asn_to_id[link['src']])
                else:
                    v1 = G.add_vertex()
                    G.vp.asn[v1] = int(link['src'])
                    asn_to_id[link['src']] = int(v1)
                if link['dst'] in asn_to_id:
                    v2 = G.vertex(asn_to_id[link['dst']])
                else:
                    v2 = G.add_vertex()
                    G.vp.asn[v2] = int(link['dst'])
                    asn_to_id[link['dst']] = int(v2)
                assert G.vp.asn[v1] == int(link['src'])
                assert G.vp.asn[v2] == int(link['dst'])
                e = G.add_edge(v1, v2)
                if link['type'] == 'i':
                    G.ep.type[e] = 1
                else:
                    G.ep.type[e] = 0
                G.ep.msm[e] = msm
                G.ep.probe[e] = int(d['prb_id'])
        dest_based_graphs[dst_asn] = (G, asn_to_id)
    print dest_based_graphs.keys()
    dest_b_gr = {}
    for asn, tup in dest_based_graphs.iteritems():
        dest_b_gr[asn] = tup[0]
    return dest_b_gr

def wrap_function( msms ):
    try:
        return compute_dest_based_graphs( msms )
    except:
        logging.warning( "".join(traceback.format_exception(*sys.exc_info())) )
    
logging.debug( "Number of measurements in the time frame: %d" % len(msms) )

num_msm_per_process = 5
num_chunks = len( msms )/num_msm_per_process + 1
pool = mp.Pool(processes=32)
results = []
for x in range( num_chunks ):
    start = x * num_msm_per_process
    end = start + num_msm_per_process
    if end > len( msms ) - 1:
        end = len( msms )
    print start, end
    results.append(pool.apply_async(wrap_function, args=(msms[ start: end ],)))
    #compute_dest_based_graphs(msms[start:end])

pool.close()
pool.join()

output = [ p.get() for p in results ]
del results

def combine_graphs(G, H):
    F = Graph()
    asn_to_id = {}
    all_edges = []
    all_nodes = []
    for edge in G.edges():
        v1 = edge.source()
        v2 = edge.target()
        src = G.vp.asn[v1]
        dst = G.vp.asn[v2]
        asn_to_id[src] = int(v1)
        asn_to_id[dst] = int(v2)
        if src not in all_nodes:
            all_nodes.append(src)
        if dst not in all_nodes:
            all_nodes.append(dst)
        if (src, dst) not in all_edges:
            all_edges.append((src, dst))

    for edge in H.edges():
        v1 = edge.source()
        v2 = edge.target()
        src = H.vp.asn[v1]
        dst = H.vp.asn[v2]
        if src not in all_nodes:
            all_nodes.append(src)
            new_v1 = G.add_vertex()
            G.vp.asn[new_v1] = src
            assert src not in asn_to_id
            asn_to_id[src] = int(new_v1)
        else:
            new_v1 = G.vertex(asn_to_id[src])
        if dst not in all_nodes:
            all_nodes.append(dst)
            new_v2 = G.add_vertex()
            G.vp.asn[new_v2] = dst
            assert dst not in asn_to_id
            asn_to_id[dst] = int(new_v2)
        else:
            new_v2 = G.vertex(asn_to_id[dst])
        if (src, dst) not in all_edges:
            ed = G.add_edge(new_v1, new_v2)
            all_edges.append((src, dst))
            G.ep.type[ed] = H.ep.type[edge]
            G.ep.msm[ed] = H.ep.msm[edge]
            G.ep.probe[ed] = H.ep.probe[edge]
    return G
                    
all_dst_based_graphs = {}
for res in output:
    print "evaluating result", res
    if res and not res.values():
        logging.warning( "No graph constructed for asn %s" % res.keys() )
    elif not res:
        continue
    for asn, gr in res.iteritems():
        if asn in all_dst_based_graphs and all_dst_based_graphs[asn] and gr:
            print "Combining graphs for AS", asn
            all_dst_based_graphs[asn] = combine_graphs(all_dst_based_graphs[asn], gr)
        elif gr and asn:
            all_dst_based_graphs[ asn ] = gr
            
print len(all_dst_based_graphs.keys())
for asn, gr in all_dst_based_graphs.iteritems():
    if not gr: continue
    try:
        gr.save(settings.GRAPH_DIR + '%s.gt' % asn)
    except:
        pdb.set_trace()
