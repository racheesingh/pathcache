#!/usr/bin/python
#from Atlas import Measure
from ripe.atlas.cousteau import Probe
import random
from collections import defaultdict
from itertools import groupby
import time
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from networkx.readwrite import json_graph
import networkx as nx
import json
from random import shuffle
import pycountry
from ripe.atlas.cousteau import ProbeRequest
import subprocess
import pdb
import os
import settings
from graph_tool.all import *

destinations = []
country_codes = [ 'BR', 'CN', 'DE', 'ES', 'FR', 'GB', 'IT', 'RU', 'UA', 'US' ]

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

global_tr_hit_count = 0
global_total = 0
def path_in_cache( src, dst ):
    global global_total
    global global_tr_hit_count
    global_total += 1
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            global_tr_hit_count += 1
            return True
    return False

with open(settings.TOR_SRC_ASES_FILE) as fi:
    srcStr = fi.read()

src_ases_by_country = {}
for line in srcStr.split('\n'):
    if line:
        _, asn, country_code = line.split()
        if country_code in src_ases_by_country:
            src_ases_by_country[country_code].append(int(asn))
        else:
            src_ases_by_country[country_code] = [int(asn)]

with open(settings.TOR_DST_ASES_FILE) as fi:
    dstStr = fi.read()

dest_ases = []
for line in dstStr.split('\n'):
    if line:
        asn, _ = line.split()
        dest_ases.append(int(asn))

def get_cov_numbers(cov_file, percent, entry_ases, exit_ases):
    print percent, len(entry_ases), len(exit_ases)
    print "Looking at new set of exits and destinations"
    src_entry_forward_hit = {}
    src_entry_forward_miss = {}
    src_entry_reverse_hit = {}
    src_entry_reverse_miss = {}

    # Checking PathCache for src/entry pairs
    for code, src_asns in src_ases_by_country.iteritems():
        src_entry_forward_hit[ code ] = 0
        src_entry_forward_miss[ code ] = 0
        src_entry_reverse_hit[ code ] = 0
        src_entry_reverse_miss[ code ] = 0
        for src_asn in src_asns:
            for entry_as in entry_ases:
                if path_in_cache( src_asn, entry_as ):
                    src_entry_forward_hit[ code ] += 1
                else:
                    src_entry_forward_miss[ code ] += 1
                    # Checking for reverse path
                if path_in_cache( entry_as, src_asn ):
                    src_entry_reverse_hit[ code ] += 1
                else:
                    src_entry_reverse_miss[ code ] += 1
        percentF = float(src_entry_forward_hit[code] * 100)/\
                   float(src_entry_forward_hit[code] + src_entry_forward_miss[code])
        percentR = float(src_entry_reverse_hit[code] * 100)/\
                   float(src_entry_reverse_hit[code] + src_entry_reverse_miss[code])
        cov_file.write("%s %d %f %f\n" % (code, percent, percentF, percentR))
        print "%s's src-to-entry forward path hit percentage: %f" % (code, percentF)
        print "%s's src-to-entry reverse path hit percentage: %f" % (code, percentR)

    exit_dst_forward_hit = 0
    exit_dst_forward_miss = 0
    exit_dst_reverse_hit = 0
    exit_dst_reverse_miss = 0

    # Checking PathCache for src/entry pairs
    for dst_asn in dest_ases:
        for exit_as in exit_ases:
            if path_in_cache( exit_as, dst_asn ):
                exit_dst_forward_hit += 1
            else:
                exit_dst_forward_miss += 1
            if path_in_cache(dst_asn, exit_as):
                exit_dst_reverse_hit += 1
            else:
                exit_dst_reverse_miss += 1

    percentF =  (exit_dst_forward_hit*100.0)/(exit_dst_forward_hit + exit_dst_forward_miss)
    percentR =  (exit_dst_reverse_hit*100.0)/(exit_dst_reverse_hit + exit_dst_reverse_miss)
    print "Exit-to-dst forward path hit percentage: %f" % percentF
    print "Exit-to-dst reverse path hit percentage: %f" % percentR

    cov_file.write("%s %d %f %f\n" % ("*", percent, percentF, percentR))
    tr_hit_rate = (float(global_tr_hit_count)*100)/float(global_total)
    print "TR hit rate:", tr_hit_rate, " Total requests:", global_total
    print "TR hit number:", global_tr_hit_count

cov_file = open("cipollino-verify/coverage_num", "w")
for percent_relays in [10, 25, 50, 75, 100]:
    if percent_relays == 100:
        entries_fname = settings.TOR_ENTRIES_FILE % ""
        exits_fname = settings.TOR_EXITS_FILE % ""
    else:
        st = "-%d" % percent_relays
        entries_fname = settings.TOR_ENTRIES_FILE % st
        exits_fname = settings.TOR_EXITS_FILE % st

    with open(entries_fname) as fi:
        entriesStr = fi.read()
    entry_ases = list( frozenset( [ int(x.split()[ 0 ]) for x in entriesStr.split( '\n' ) if x ] ) )
        
    with open(exits_fname ) as fi:
        exitsStr = fi.read()
    exit_ases = list( frozenset( [ int(x.split()[ 0 ]) for x in exitsStr.split( '\n' ) if x ] ) )
    get_cov_numbers(cov_file, percent_relays, entry_ases, exit_ases)

fi.close()
