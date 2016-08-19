#!/usr/bin/python
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings

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
    all_graphs_ripe[asn] = gr
    for v in gr.vertices():
        src_asn = gr.vp.asn[v]
        ripe_all.append((src_asn, asn))
        if v.out_degree() > 1:
            ripe_violations.append((src_asn, asn))

size_d = {}
for asn, gr in all_graphs_ripe.iteritems():
    size_d[asn] = gr.num_vertices()
pdb.set_trace()
sorted_size = sorted(size_d.items(), key=lambda x: x[1], reverse=True)

'''ripe_all = list(set(ripe_all))
ripe_violations = list(set(ripe_violations))

print "RIPE total src dst pairs", len(ripe_all)
print "RIPE total violations", len(ripe_violations)
percent_violations_ripe = round(float(len(ripe_violations))/len(ripe_all), 3)
print "RIPE percentage of violations", percent_violations_ripe

all_graphs_caida = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_CAIDA ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_CAIDA, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_CAIDA, f ) for f in files ]

caida_all = []
caida_violations = []
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing CAIDA graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    all_graphs_caida[asn] = gr
    for v in gr.nodes():
        caida_all.append((v, asn))
        if gr.out_degree(v) > 1:
            caida_violations.append((v, asn))
                                   
caida_all = list(set(caida_all))
caida_violations = list(set(caida_violations))

print "CAIDA total src dst pairs", len(caida_all)
print "CAIDA total violations", len(caida_violations)
percent_violations_caida = round(float(len(caida_violations))/len(caida_all), 3)
print "CAIDA percentage of violations", percent_violations_caida

all_graphs_iplane = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_IPLANE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_IPLANE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_IPLANE, f ) for f in files ]
iplane_all = []
iplane_violations = []
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing Iplane graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    all_graphs_iplane[asn] = gr
    for v in gr.nodes():
        iplane_all.append((v, asn))
        if gr.out_degree(v) > 1:
            iplane_violations.append((v, asn))

iplane_all = list(set(iplane_all))
iplane_violations = list(set(iplane_violations))

print "IPLANE total src dst pairs", len(iplane_all)
print "IPLANE total violations", len(iplane_violations)
percent_violations_iplane = round(float(len(iplane_violations))/len(iplane_all), 3)
print "IPLANE percentage of violations", percent_violations_iplane

all_graphs_bgp = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_BGP ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_BGP, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_BGP, f ) for f in files ]

bgp_all = []
bgp_violations = []
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing BGP graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    all_graphs_bgp[asn] = gr
    for v in gr.nodes():
        bgp_all.append((v, asn))
        if gr.out_degree(v) >1:
            bgp_violations.append((v, asn))

bgp_all = list(set(bgp_all))
bgp_violations = list(set(bgp_violations))

print "BGP total src dst pairs", len(bgp_all)
print "BGP total violations", len(bgp_violations)
percent_violations_bgp = round(float(len(bgp_violations))/len(bgp_all), 3)
print "BGP percentage of violations", percent_violations_bgp

'''
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]
all_graphs = {}
combined_all = []
combined_violations = []
for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    all_graphs[int(asn)] = gr
    for v in gr.vertices():
        src_asn = gr.vp.asn[v]
        combined_all.append((src_asn, asn))
        if v.out_degree() > 1:
            combined_violations.append((src_asn, asn))

combined_all = list(set(combined_all))
combined_violations = list(set(combined_violations))

print "COMBINED total src dst pairs", len(combined_all)
print "COMBINED total violations", len(combined_violations)
percent_violations_combined = round(float(len(combined_violations))/len(combined_all), 3)
print "COMBINED percentage of violations", percent_violations_combined
