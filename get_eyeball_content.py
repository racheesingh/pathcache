#!/usr/bin/python
from Atlas import Measure
import random
from collections import defaultdict
from itertools import groupby
import time
from networkx.readwrite import json_graph
import networkx as nx
import json
from random import shuffle
import pycountry
import subprocess
import pdb
import os
import settings
from graph_tool.all import *
import socket
import mkit.inference.ip_to_asn as ip2asn
import alexa
import pdb
import time

f = open("data/aspop")
entries = list()
for table in f:
    records = table.split("[")
    for record in records:
        record = record.split("]")[0]
        entry = dict()
        try:
            entry["rank"] = record.split(",")[0]
            entry["as"] = record.split(",")[1].strip("\"")
            entry["country"] = ((record.split(",")[3]).split("=")[2]).split("\\")[0]
            entry["ip"] = None
            entries.append(entry)
        except IndexError:
            continue
f.close()

with open("cipollino-verify/measured_path_list_caida") as fi:
    mmt_paths = json.load(fi)
with open("cipollino-verify/measured_path_list_ripe") as fi:
    mmt_paths.extend(json.load(fi))
with open("cipollino-verify/measured_path_list_iplane") as fi:
    mmt_paths.extend(json.load(fi))

websites = alexa.top_list(100)
content_asns = []
content_ips = {}
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
        content_ips[asn] = socket.gethostbyname(w[1])
    except:
        print w
        continue

content_asns = list(set(content_asns))
eyeballs = [int(e["as"].split("AS")[-1]) for e in entries]
eyeballs = list(set(eyeballs))

all_probes = []
with open("cipollino-verify/20160223.json") as fd:
    jsonStr = json.load(fd)
    
    total_count = jsonStr['meta']['total_count']
    for probeCount in range(total_count):
        probe = jsonStr['objects'][probeCount]
        all_probes.append(probe)

def get_probes_in_asn(asn):
    asn = int(asn)
    candidate_set = []
    for pr in all_probes:
        if 'system-ipv6-works' in pr['tags'] and pr['status_name'] == 'Connected':
            if pr['asn_v4'] == asn:
                candidate_set.append(pr)
    return candidate_set

all_graphs = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]

for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    all_graphs[int(asn)] = gr

def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
    return False

asn_to_prefix = {}
with open("cipollino-verify/routeviews-rv2-20160221-1200.pfx2as") as fi:
    for line in fi:
        ip, preflen, asn = line.split()
        if ',' in asn:
            tokens = asn.split(',')
            asn = tokens[0]
        if '_' in asn:
            tokens = asn.split('_')
            asn = tokens[0]
        if asn in asn_to_prefix:
            asn_to_prefix[asn].append(ip)
        else:
            asn_to_prefix[asn] = [ip]

def run_oneofftrace(src, dst_prefix):
    src_probe = get_probes_in_asn(src)
    if not src_probe:
        return None
    print "Launching traceroute:", len(src_probe), dst_prefix
    src_probe = src_probe[0]['id']
    try:
        msm_id = Measure.oneofftrace(
        src_probe, dst_prefix, af=4, paris=1,
            description="UNSEEN_ACCURACY_PC traceroute to %s" % dst_prefix )
        return msm_id
    except:
        pdb.set_trace()
        return None

msms_for_eval_dest = []
for dst_asn in content_asns:
    if len(msms_for_eval_dest) > 1000:
        break
    dst_count = 0
    for src_asn in eyeballs:
        if dst_count >=5:
            break
        if (src_asn, dst_asn) not in mmt_paths and path_in_cache(src_asn, dst_asn):
            dst_prefix = content_ips[dst_asn]
            if not dst_prefix and not src_asn: continue
            msm_id = run_oneofftrace(src_asn, dst_prefix)
            if msm_id:
                dst_count += 1
                msms_for_eval_dest.append((src_asn, dst_asn, msm_id))
                print len(msms_for_eval_dest)
pdb.set_trace()
pdb.set_trace()
print msms_for_eval_dest
with open("cipollino-verify/pc_correctness_content_eyeball_unseen", "w") as fi:
    json.dump(msms_for_eval_dest, fi)

pdb.set_trace()
