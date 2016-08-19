#!/usr/bin/python
import math
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings

all_graphs = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]
ripe_files = [x for x in os.listdir( settings.GRAPH_DIR_RIPE)]
caida_files = [x for x in os.listdir( settings.GRAPH_DIR_CAIDA)]
iplane_files = [x for x in os.listdir( settings.GRAPH_DIR_IPLANE)]
bgp_files = [x for x in os.listdir( settings.GRAPH_DIR_BGP)]

all_asns = []
for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    all_asns.append(asn)
asn_counts = {}
for asn in all_asns:
    print "SN", asn
    if "%s.gt" % asn in ripe_files:
        fname =  os.path.join(settings.GRAPH_DIR_RIPE, "%s.gt" % asn)
        ripe_gr =load_graph(fname, fmt="gt")
    else:
        ripe_gr = None
    if str(asn) in caida_files:
        fname = os.path.join(settings.GRAPH_DIR_CAIDA, str(asn))
        with open(fname) as fi:
            caida_gr = json_graph.node_link_graph(json.load(fi))
    else:
        caida_gr = None
    if str(asn) in iplane_files:
        fname = os.path.join(settings.GRAPH_DIR_IPLANE, str(asn))
        with open(fname) as fi:
            iplane_gr = json_graph.node_link_graph(json.load(fi))
    else:
        iplane_gr = None
    if str(asn) in bgp_files:
        fname = os.path.join(settings.GRAPH_DIR_BGP, str(asn))
        with open(fname) as fi:
            bgp_gr = json_graph.node_link_graph(json.load(fi))            
    else:
        bgp_gr = None

    fname = os.path.join(settings.GRAPH_DIR_FINAL, "%s.gt" % asn)
    complete_gr = load_graph(fname, fmt="gt")
    all_vertices = complete_gr.vertices()
    all_vertices_count = len([x for x in all_vertices])

    if ripe_gr:
        ripe_nodes = ripe_gr.vertices()
    else:
        ripe_nodes = set()
    ripe_asns = frozenset([ripe_gr.vp.asn[v] for v in ripe_nodes])
    if caida_gr:
        caida_asns = frozenset(caida_gr.nodes())
    else:
        caida_asns = set()
    if iplane_gr:
        iplane_asns = frozenset(iplane_gr.nodes())
    else:
        iplane_asns = set()
    if bgp_gr:
        bgp_asns = frozenset(bgp_gr.nodes())
    else:
        bgp_asns = set()
    asn_counts[asn] = {}
    asn_counts[asn]['ripe'] = len(ripe_asns - caida_asns - iplane_asns)
    asn_counts[asn]['caida'] = len(caida_asns - ripe_asns - iplane_asns)
    asn_counts[asn]['iplane'] = len(iplane_asns - caida_asns - ripe_asns)
    asn_counts[asn]['multiple'] = all_vertices_count - asn_counts[asn]['ripe'] -\
                                 asn_counts[asn]['caida'] - asn_counts[asn]['iplane']
pdb.set_trace()
with open("per_graph_stats_new_method", "w") as fi:
    json.dump(asn_counts, fi)
