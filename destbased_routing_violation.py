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
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    '''
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
    '''
    all_graphs[int(asn)] = gr

percent_violation_per_dest = {}
for asn, gr in all_graphs.iteritems():
    violation_count = 0
    for v in gr.vertices():
        if v.out_degree() > 1:
            violation_count += 1
    total_sources = gr.num_vertices() - 1
    if total_sources > 0:
        percent_violation_per_dest[asn] = float(violation_count)/total_sources

with open("cipollino-verify/violations_june24", "w") as fi:
    json.dump(percent_violation_per_dest, fi)

all_graphs_pref = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL_PREF ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL_PREF, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL_PREF, f ) for f in files ]

TOTAL_MMT_PER_SRC = 20
for f in files:
    pref = f.split( '/' )[ -1 ].split('.gt')[0]
    print "RIPE graph for PREF", pref
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    all_graphs_pref[pref] = gr

percent_violation_per_dest_pref = {}
for pref, gr in all_graphs_pref.iteritems():
    violation_count = 0
    for v in gr.vertices():
        if v.out_degree() > 1:
            violation_count += 1
    total_sources = gr.num_vertices() - 1
    if total_sources > 1:
        percent_violation_per_dest_pref[pref] = float(violation_count)/total_sources

with open("cipollino-verify/violations_pref_june24", "w") as fi:
    json.dump(percent_violation_per_dest_pref, fi)
pdb.set_trace()
violation_count_per_as = {}
occurence_src_per_as = {}
for dst_asn, gr in all_graphs.iteritems():
    for v in gr.vertices():
        asn = gr.vp.asn[v]
        if asn not in occurence_src_per_as:
            occurence_src_per_as[asn] = 0
        occurence_src_per_as[asn] += 1
        if v.out_degree() > 1:
            if asn not in violation_count_per_as:
                violation_count_per_as[asn] = 0
            violation_count_per_as[asn] += 1

violation_percent_per_src_as = {}
for asn, count in violation_count_per_as.iteritems():
    violation_percent_per_src_as[asn] = round(float(count)/occurence_src_per_as[asn], 3)

with open("cipollino-verify/violations", "w") as fi:
    json.dump(percent_violation_per_dest, fi)

with open("cipollino-verify/violation_count", "w") as fi:
    json.dump(violation_count_per_as, fi)

with open("cipollino-verify/violation_percent_src", "w") as fi:
    json.dump(violation_percent_per_src_as, fi)

