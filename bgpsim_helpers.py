import multiprocessing as mp
import signal
import copy
import socket
from ripe.atlas.cousteau import ProbeRequest
from ripe.atlas.cousteau import (
    Ping,
    Traceroute,
    AtlasSource,
    AtlasCreateRequest
)
import json
import os
import csv
import sys
import subprocess
import pdb
from subprocess import Popen
import time
import settings
import mkit.inference.ixp as ixp
from networkx.readwrite import json_graph
import networkx as nx
import mkit.inference.ip_to_asn as ip2asn
TCP_IP = '127.0.0.1'
TCP_PORT = 11002

response = ProbeRequest(tags="system-ipv4-works", status=1)
all_ripe_probes = [pr for pr in response]
per_asn_probes = {}

for pr in all_ripe_probes:
    if 'asn_v4' not in pr: continue
    if not pr['asn_v4']: continue
    pr_asn = pr['asn_v4']
    if pr_asn in per_asn_probes:
        per_asn_probes[pr_asn].append(pr)
    else:
        per_asn_probes[pr_asn] = [pr]

print "Total %d RIPE probes, in %d ASNs" % (len(all_ripe_probes), len(per_asn_probes))

print "Get all single homed ASes"
# providers of an asn
provider_asns = {}
# customers of an asn
customer_asns = {}
with open("20170201.as-rel.txt") as f:
    for line in f:
        if line.startswith('#'): continue
        prov, cust, typ = line.split('|')
        if typ == '-1\n':
            if cust in provider_asns:
                provider_asns[cust].append(prov)
            else:
                provider_asns[cust] = [prov]
            if prov in customer_asns:
                customer_asns[prov].append(cust)
            else:
                customer_asns[prov] = [cust]
        else:
            # Treating p2p as provider links that go both
            # ways. Since I want to find ASNs that have *only*
            # one way to go outside of their network
            if cust in provider_asns:
                provider_asns[cust].append(prov)
            else:
                provider_asns[cust] = [prov]
            if prov in provider_asns:
                provider_asns[prov].append(cust)
            else:
                provider_asns[prov] = [cust]
                
single_homed_asns = [x[0] for x in provider_asns.items() if len(x[1]) == 1]

def get_single_homed_customers(asn):
    if str(asn) not in customer_asns:
        return []
    single_homed = set()
    for cust in customer_asns[str(asn)]:
        if cust in single_homed_asns:
            single_homed.add(cust)
    return list(single_homed)

def make_bgp_gr(arr_list, asn_src):
    as_graph = nx.DiGraph()
    for arr in arr_list:
        arr = arr.split(':\n')[-1]
        hops = arr.split('\n')
        hops = [x for x in hops if x]
        if not hops: continue
        if asn_src not in hops:
            print "Source not in path!", hops, asn_src
            continue
        path_hops = hops[:hops.index(asn_src) + 1]
        new_hops = []
        for hop in path_hops:
            if hop in ixp.IXPs:
                continue
            new_hops.append(hop)
        if len(new_hops) <= 1:
            continue
        for i in range(0,len(new_hops)-1):
            as_graph.add_edge(new_hops[i],new_hops[i+1])
    data = json_graph.node_link_data(as_graph)
    s = json.dumps( data )
    with open(settings.GRAPH_SIM_BGP + '%s' % asn_src, "w") as f:
        f.write(s)
    return as_graph

asns = set()
with open("20170201.as-rel.txt") as f:
    for line in f:
        if line.startswith('#'): continue
        src, dst, typ = line.split('|')
        asns.add(src)
        asns.add(dst)
asns = list(asns)
asn_srcs = asns

print "Totally %d ASNs on the Internet, %d of them are single homed stubs" % (len(asn_srcs), len(single_homed_asns))

def run_sim_get_bgp_graphs(dest_asn):
    dest_asn = str(dest_asn)
    FNULL = open(os.devnull, 'w')
    proc = Popen(['mono',
                  '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/TestingApplication/bin/Release/TestingApplication.exe',
                  '-server11002 ', '20170201-cyclops',
                  '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/precomp/US-precomp367.txt',
                  '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/cache/exit_asns.txt' ],
                 stderr=subprocess.STDOUT, stdout=FNULL, stdin=subprocess.PIPE)
    time.sleep(10)
    pid = proc.pid
    print "Getting all paths towards", dest_asn
    MESSAGE = dest_asn + " -q"
    count = 0 
    for asn_src in asn_srcs:
        if asn_src == dest_asn: continue
        MESSAGE += " " + asn_src + " " + dest_asn
        count += 1
    MESSAGE += " <EOFc> "
    print "Sending message to BGPSim to get %d paths towards %s.." % (count, dest_asn)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP, TCP_PORT))
    s.send(MESSAGE)
    data = ""
    result = dict()
    buffer_size = 10000000
    while True:
        d = s.recv(buffer_size)
        data += d
        if len(d) == 0:
            break
        if "<EOFs>" in d:
            break

    s.close()
    proc.kill()
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        pass
    print "Killed BGPSim"
    arr = data.split("-\n")
    gr = make_bgp_gr(arr, dest_asn)
    return gr, pid

def greedy_max_coverage(subsets, k, superset):
    # superset is the set of all ASNs discoverable by running
    # measurements.
    superset_paths = copy.deepcopy(superset)

    # subsets is a dictionary, where key is the vantage point ASN
    # and value is a list of ASes discovered while running a measurement
    # from an AS.
    subsets_copy = copy.deepcopy(subsets)
    nodes_covered = []
    mmts_ordered = []
    for i in range(k):
        if not superset_paths:
            print "Finished early!", i, k
            return mmts_ordered, nodes_covered
        max_gain = None
        max_gain_mmt = []
        for mmt in subsets_copy:
            if not max_gain:
                max_gain_mmt = mmt
                max_gain = set(subsets_copy[mmt]).intersection(superset_paths)
            if len(set(subsets_copy[mmt]).intersection(superset_paths)) > len(max_gain):
                max_gain = set(subsets_copy[mmt]).intersection(superset_paths)
                max_gain_mmt = mmt
        if not max_gain : pdb.set_trace()
        assert max_gain_mmt not in mmts_ordered
        superset_paths = superset_paths.difference(max_gain)
        nodes_covered.append(list(max_gain))
        if len(nodes_covered) > 1:
            #print len(nodes_covered[-1]), len(nodes_covered[-2])
            assert len(nodes_covered[-1]) <= len(nodes_covered[-2])
        subsets_copy.pop(max_gain_mmt)
        mmts_ordered.append(max_gain_mmt)
    return mmts_ordered, nodes_covered

def wrap_function(asn, per_asn_probes):
    print "Building BGPSIM graph for AS", asn
    gr, pid = run_sim_get_bgp_graphs(asn)
    #os.system("sudo kill -9 %s" % pid)
    try:
        data = json_graph.node_link_data(gr)
        s = json.dumps(data)
        with open( settings.GRAPH_SIM_BGP + '%s' % asn, "w" ) as f:
            f.write( s )
        print "Wrote graph for %s to disk" % asn
    except:
        print "failed to write"

    init_utility_per_mmt = {}
    superset = set() # contains all the nodes that can be covered if we were to run measurements from
    # all ripe nodes.
    for vp_asn in per_asn_probes:
        single_homed_gain = get_single_homed_customers(vp_asn)
        try:
            sp = nx.shortest_path(gr, str(vp_asn), str(asn))
        except nx.exception.NetworkXError, e:
            print "In the graph of", asn, e
            continue
        utility = set(single_homed_gain).union(set(sp))
        init_utility_per_mmt[str(vp_asn)] = utility
        superset = superset.union(set(single_homed_gain).union(set(sp)))
    #max_measurable[asn] = list(superset)
    mmt_subset, coverage = greedy_max_coverage(init_utility_per_mmt,
                                               len(per_asn_probes),
                                               superset)
    return [asn, mmt_subset, coverage, list(superset)]

max_measurable = {}
overall_mmt_gain = {}
results = []
pool = mp.Pool(processes=25, maxtasksperchild=1)
for asn in asn_srcs:
    # print "Building BGPSIM graph for AS", asn
    # gr = run_sim_get_bgp_graphs(asn)
    # try:
    #     data = json_graph.node_link_data(gr)
    #     s = json.dumps(data)
    #     with open( settings.GRAPH_SIM_BGP + '%s' % asn, "w" ) as f:
    #         f.write( s )
    # except:
    #     pdb.set_trace()

    # init_utility_per_mmt = {}
    # superset = set() # contains all the nodes that can be covered if we were to run measurements from
    # # all ripe nodes.
    # for vp_asn in per_asn_probes:
    #     single_homed_gain = get_single_homed_customers(vp_asn)
    #     try:
    #         sp = nx.shortest_path(gr, str(vp_asn), str(asn))
    #     except nx.exception.NetworkXError, e:
    #         print "In the graph of", asn, e
    #         continue
    #     utility = set(single_homed_gain).union(set(sp))
    #     init_utility_per_mmt[str(vp_asn)] = utility
    #     superset = superset.union(set(single_homed_gain).union(set(sp)))
    # max_measurable[asn] = list(superset)
    # mmt_subset, coverage = greedy_max_coverage(init_utility_per_mmt,
    #                                            len(per_asn_probes),
    #                                            superset)
    results.append(pool.apply_async(wrap_function, args=(asn, per_asn_probes)))
    #wrap_function(asn, per_asn_probes)
pool.close()
pool.join()

output = [ p.get() for p in results ]
del results

#overall_mmt_gain[asn] = [mmt_subset, coverage]
pdb.set_trace()
with open("all_ases_greedy_mmts_ripe.json", "w") as fi:
    json.dump(overall_mmt_gain, fi)
with open("max_measurable.json", "w") as fi:
    json.dump(max_measurable, fi)
