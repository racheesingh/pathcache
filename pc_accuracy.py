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
import time
import socket
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings

files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]

all_graphs = {}

for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "COMBINEND graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    overall_origins = {}
    all_graphs[int(asn)] = gr

files = [ x for x in os.listdir( settings.GRAPH_DIR_BGP ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_BGP, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_BGP, f ) for f in files ]

all_graphs_bgp = {}
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing BGP graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    all_graphs_bgp[asn] = gr


def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
        else:
            if dst in all_graphs_bgp:
                gr = all_graphs_bgp[asn]
                if src in gr.nodes:
                    return True
    return False

NUM_CONTENT = 500
websites = alexa.top_list(NUM_CONTENT)
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

f = open("data/aspop")
entries = list()
for table in f:
    records = table.split("[")
    for record in records:
        record = record.split("]")[0]
        entry = dict()
        try:
            entry["rank"] = int(record.split(",")[0])
            entry["as"] = record.split(",")[1].strip("\"")
            entry["country"] = ((record.split(",")[3]).split("=")[2]).split("\\")[0]
            entry["users"] = int(record.split(",")[4])
            if entry["rank"] > 5000:
                continue
            entries.append(entry)
        except (IndexError, ValueError):
            continue
f.close()

top_eyeballs = []
NUM_EYEBALL = 500
for entry in entries:
    top_eyeballs.append(int(entry['as'].split('AS')[-1]))
top_eyeballs = top_eyeballs[:200]
def run_oneofftrace(src, dst_prefix):
    src_probe = get_probes_in_asn(src)
    if not src_probe:
        return None
    print "Launching traceroute:", len(src_probe), dst_prefix
    src_probe = src_probe[0]['id']
    try:
        msm_id = Measure.oneofftrace(
        src_probe, dst_prefix, af=4, paris=1,
            description="ACCURACY_STATIC_PC_POSTER3 traceroute to %s" % dst_prefix )
        return msm_id
    except:
        time.sleep(150)
        return None

all_probes = []
with open("data/20160501.json") as fd:
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

with open("evaluation/evaluation_queries", "w") as fi:
    for src_eyeball in top_eyeballs:
        for dst_content in content_asns:
            fi.write("%s %s\n" % (src_eyeball, dst_content))
fi.close()
print "getting overall coverage"
overall_msms = []
for src_eyeball in top_eyeballs:
    for dst_content in content_asns:
        dst_prefix = content_ips[dst_content]
        if not dst_prefix and not src_eyeball: continue
        
        if not path_in_cache(src_eyeball, dst_content): continue
        print "Traceroute from ASN", src_eyeball, "to content", dst_content
        msm_id = run_oneofftrace(src_eyeball, dst_prefix)
        if msm_id:
            overall_msms.append((src_eyeball, dst_content, msm_id))

print overall_msms


'''
country_msms = {}
pdb.set_trace()
for code, eyeballs in src_ases_by_country.iteritems():
    print code
    country_msms[code] = {}
    eyeballs = eyeballs[:NUM_EYEBALL]
    for src in eyeballs:
        for dst in content_asns:
            if not path_in_cache(src, dst): continue
            dst_prefix = content_ips[dst]
            if not dst_prefix and not src: continue
            msm_id = run_oneofftrace(src, dst_prefix)
            if msm_id:
                country_msms[code][(src, dst)] = msm_id'''

#with open("cipollino-verify/pc_accuracy_country_mmt", "w") as fi:
#    json.dump(country_msms, fi)

with open("cipollino-verify/pc_accuracy_mmt_june23", "w") as fi:
    json.dump(overall_msms, fi)

pdb.set_trace()
