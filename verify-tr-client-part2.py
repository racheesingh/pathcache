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

print "Measuring path correctness"

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
dest_as_ips = {}
for line in dstStr.split('\n'):
    if line:
        asn, ip = line.split()
        dest_ases.append(int(asn))
        dest_as_ips[int(asn)] = ip

with open(settings.TOR_ENTRIES_FILE % "") as fi:
    entriesStr = fi.read()
entry_ases = list( frozenset( [ int(x.split()[ 0 ]) for x in entriesStr.split( '\n' ) if x ] ) )

with open(settings.TOR_ENTRIES_FILE % "") as fi:
    entriesStr = fi.read()
entry_ases_ips = dict( [ (int(x.split()[ 0 ]), x.split()[ 1 ].split('/')[0]) for x in entriesStr.split( '\n' ) if x ] )

with open(settings.TOR_EXITS_FILE % "") as fi:
    exitsStr = fi.read()
exit_ases = list( frozenset( [ int(x.split()[ 0 ]) for x in exitsStr.split( '\n' ) if x ] ) )

def dfs_paths(gr, src_node, dst_node):
    stack = [(src_node, [src_node])]
    while stack:
        (vertex, path) = stack.pop()
        for next in set(vertex.out_neighbours()) - set(path):
            if next == dst_node:
                yield path + [next]
                #return path + [next]
            else:
                stack.append((next, path + [next]))

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
        rp = gr.ep.RIPE
        cp = gr.ep.CAIDA
        ip = gr.ep.IPLANE
        GV = GraphView(gr, efilt=lambda e:rp[e] == 1 or cp[e] == 1 or ip[e] == 1)
        src_dst_paths = dfs_paths(GV, src_node, dst_node)
        first = next(src_dst_paths, None)
        if not first:
            print "Could not find a path from data plane measurements."
            src_dst_paths = dfs_paths(gr, src_node, dst_node)
        hops = []
        for p in src_dst_paths:
            hops.extend(p)
        return hops
    else:
        return []

def path_cache(src, dst):
    if not path_in_cache(src, dst):
        return None
    hops = list(frozenset(get_path(src, dst)))
    return hops
'''
destination_ases = set(all_ripe_graphs.keys()).union(set(all_iplane_graphs.keys())).union(set(all_bgp_graphs.keys()))
destination_ases = list(destination_ases)

with open("probe_to_asn_updated") as fi:
    ptoasnStr = fi.read()
probes_to_asn = json.loads(ptoasnStr)
asn_to_probe = {}
for k, v in probes_to_asn.iteritems():
    asn_to_probe[v] = asn_to_probe.get(v, [])
    asn_to_probe[v].append(k)

def get_probe_in_asn(asn):
    if int(asn) in asn_to_probe and asn_to_probe[int(asn)]:
        probe_id= asn_to_probe[int(asn)][0]
    else:
        return None
    return probe_id
'''
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

with open("cipollino-verify/measured_path_list_caida") as fi:
    mmt_paths = json.load(fi)
with open("cipollino-verify/measured_path_list_ripe") as fi:
    mmt_paths.extend(json.load(fi))
with open("cipollino-verify/measured_path_list_iplane") as fi:
    mmt_paths.extend(json.load(fi))

def run_oneofftrace(src, dst_prefix):
    src_probe = get_probes_in_asn(src)
    if not src_probe:
        return None
    print "Launching traceroute:", len(src_probe), dst_prefix
    src_probe = src_probe[0]['id']
    try:
        msm_id = Measure.oneofftrace(
        src_probe, dst_prefix, af=4, paris=1,
            description="ACCURACY_PC: traceroute to %s" % dst_prefix )
        return msm_id
    except:
        pdb.set_trace()
        return None
'''
mmt_paths = [tuple(x) for x in mmt_paths]
mmt_paths = list(frozenset(mmt_paths))
msms_for_eval = {}
for code, src_asns in src_ases_by_country.iteritems():
    msms_for_eval[code] = []
    count = 0
    print "For country", code, len(src_asns), len(entry_ases)
    for src_asn in src_asns:
        if count > 20:
            break
        for entry_as in entry_ases:
            if count > 20:
                break
            if (src_asn, entry_as) not in mmt_paths and path_in_cache(src_asn, entry_as):
                dst_prefix = entry_ases_ips[int(entry_as)]
                if not dst_prefix and not src_asn: continue
                msm_id = run_oneofftrace(src_asn, dst_prefix)
                if msm_id:
                    count += 1
                    msms_for_eval[code].append((src_asn, entry_as, msm_id))

with open("cipollino-verify/correctness1", "w") as fi:
    json.dump(msms_for_eval, fi)
'''
count = 0
msms_for_eval_dest = []
for dst_asn in dest_ases:
    if count > 200:
        break
    dst_count = 0
    for exit_as in exit_ases:
        if dst_count >=5:
            break
        if (exit_as, dst_asn) not in mmt_paths and path_in_cache(exit_as, dst_asn):
            dst_prefix = dest_as_ips[int(dst_asn)]
            if not dst_prefix and not exit_as: continue
            msm_id = run_oneofftrace(exit_as, dst_prefix)
            if msm_id:
                count += 1
                dst_count += 1
                msms_for_eval_dest.append((exit_as, dst_asn, msm_id))
pdb.set_trace()

with open("cipollino-verify/correctness2", "w") as fi:
    json.dump(msms_for_eval_dest, fi)

'''
src_dst_from_ripe = set()
with open('logs/mmt-fectch-2016-02-17-17:45:14.136466.log') as fi:
    for line in fi:
        if not 'RACHEE:' in line:
            continue
        tokens = line.split('RACHEE:')[-1].strip().split('\t')
        if len(tokens) < 2:
            tokens = line.split('RACHEE:')[-1].strip().split(' ')
        src, dst = tokens
        src_dst_from_ripe.add((src, dst))


print "Parsed measurements from %d unique src-dest pairs" % len(src_dst_from_ripe)
src_mmts = [str(x[0]) for x in src_dst_from_ripe if x]
src_mmts = list(frozenset(src_mmts))
asns_with_probes = asn_to_probe.keys()
asns_with_probes = [str(x) for x in asns_with_probes if x]
asns_with_probes = list(frozenset(asns_with_probes))

ases_without_mmts = list(set(asns_with_probes).difference(set(src_mmts)))
src_dst_not_in_mmt = []
count = 0
for dst, gr in all_ripe_graphs.iteritems():
    for src in ases_without_mmts:
        if src in gr.nodes():
            # these nodes should not be leaves
            #assert gr.in_degree(src) > 0
            if gr.in_degree(src) == 0:
                continue
            else:
                count += 1
                src_dst_not_in_mmt.append((src, dst))

pdb.set_trace()
count_new_paths = 0
traceroute_meta = []
for as_tuple in src_dst_not_in_mmt:
    print "evaluating", as_tuple
    src = str(as_tuple[0])
    dst = str(as_tuple[1])
    prb_id = get_probe_in_asn(src)
    if not prb_id:
        continue
    if dst not in all_ripe_graphs:
        continue
    if src not in all_ripe_graphs[dst].nodes():
        continue
    try:
        pc_path = path_cache(src, dst)
    except:
        continue
    if not pc_path:
        continue
    count_new_paths += 1
    dst_prefix = asn_to_prefix[dst]
    if not dst_prefix:
        continue
    else:
        dst_prefix = dst_prefix[0]
    print "adding to list"
    traceroute_meta.append((prb_id, dst_prefix))


with open("traceroute_meta") as fi:
    traceroute_meta = json.load(fi)

print len(traceroute_meta)
pdb.set_trace()        

msms = []
actual_mmts_count = 0
for tup in traceroute_meta:
    src_probe = int(tup[0])
    dst_prefix = tup[1]
    probe = Probe(id=src_probe)
    if probe.status != 'Connected':
        continue
    actual_mmts_count += 1
    msm_id = Measure.oneofftrace(
        src_probe, dst_prefix, af=4, paris=1,
        description="PCAccuracy Test: traceroute to %s" % dst_prefix )
    msms.append({(src_probe, dst_prefix): msm_id})
    time.sleep(1)
print msms
'''
pdb.set_trace()
