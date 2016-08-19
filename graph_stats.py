#!/usr/bin/python
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings

# Source values
RIPE = 1
CAIDA = 2
IPLANE = 3
BGP = 4

all_graphs_ripe = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_RIPE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_RIPE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_RIPE, f ) for f in files ]
ripe_src_dst_pairs = 0
for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    all_graphs_ripe[asn] = gr
    ripe_src_dst_pairs += gr.num_vertices() - 1

print "RIPE src dst pairs", ripe_src_dst_pairs

all_graphs_caida = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_CAIDA ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_CAIDA, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_CAIDA, f ) for f in files ]
caida_src_dst_pairs = 0
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing CAIDA graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    all_graphs_caida[asn] = gr
    caida_src_dst_pairs += gr.number_of_nodes() -1

print "CAIDA src dst pairs", caida_src_dst_pairs

all_graphs_iplane = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_IPLANE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_IPLANE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_IPLANE, f ) for f in files ]
iplane_src_dst_pairs = 0
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing Iplane graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    all_graphs_iplane[asn] = gr
    iplane_src_dst_pairs += gr.number_of_nodes() - 1

print "Iplane src dst pairs", iplane_src_dst_pairs

all_graphs_bgp = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_BGP ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_BGP, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_BGP, f ) for f in files ]
bgp_src_dst_pairs = 0
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing BGP graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    all_graphs_bgp[asn] = gr
    bgp_src_dst_pairs += gr.number_of_nodes() - 1

print "BGP src dst pairs", bgp_src_dst_pairs

pdb.set_trace()
pdb.set_trace()
