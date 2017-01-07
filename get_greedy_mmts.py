import json
import random
import sys
import dill
import copy
import pdb
import os
import mkit.inference.ixp as ixp
from networkx.readwrite import json_graph
import networkx as nx
import mkit.inference.ip_to_asn as ip2asn
import csv
from mkit.ripeatlas import probes
import settings

with open("../asgraphs/analysis/asn_to_cc") as fi:
    asn_to_cc = json.load(fi)

country_codes_shuffled = list(set(asn_to_cc.values()))
random.shuffle(country_codes_shuffled)
all_probes = probes.all_probes

if sys.argv[1] == 'ripe':
    print "Getting source probes from RIPE"
    type_probes = 'ripe'
    probes_per_asn = {}
    for pr in all_probes:
        if 'system-ipv4-works' in pr['tags'] and pr['status_name'] == 'Connected':
            asn = pr['asn_v4']
        if not asn: continue
        if asn in probes_per_asn:
            probes_per_asn[asn].append(pr)
        else:
            probes_per_asn[asn] = [pr]
            
if sys.argv[1] == 'pl':
    print "Getting source probes from PL"
    type_probes = 'pl'
    with open("pl_probe_asns.json") as fi:
        probes_per_asn = json.load(fi)

if sys.argv[1] == 'ark':
    print "Getting source probes form CAIDA Ark"
    type_probes = 'ark'
    with open("ark_probe_asns.json") as fi:
        probes_per_asn = json.load(fi)

files = [ x for x in os.listdir( settings.GRAPH_SIM_BGP ) \
          if os.path.isfile( os.path.join( settings.GRAPH_SIM_BGP, x ) ) ]
files = [ os.path.join( settings.GRAPH_SIM_BGP, f ) for f in files ]
asn_graphs = {}
for f in files:
    print "Loading the graph", f
    asn = f.split( '/' )[ -1 ].split('.')[0]
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    asn_graphs[asn] = gr

top_content_prefs = []
with open("per_prefix_count.csv") as fi:
    reader = csv.reader(fi)
    for row in reader:
        if row[0] == 'pref': continue
        if int(row[1]) < 72: break
        top_content_prefs.append(row[0])

with open("random_ips.json") as fi:
    random_prefs = json.load(fi)

with open("top_cust_cone_ips.json") as fi:
    top_cust_cone_prefs = json.load(fi)

with open("top_eyeball_prefs.json") as fi:
    top_eyeball_prefs = json.load(fi)

print "Get all single homed ASes"
# providers of an asn
provider_asns = {}
# customers of an asn
customer_asns = {}
with open("../asgraphs/data2/caida/20161201.as-rel.txt") as f:
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
    print "FOR THE TIME BEING, NO SINGLE_HOMED GAINS"
    return []
    if str(asn) not in customer_asns:
        return []
    single_homed = set()
    for cust in customer_asns[str(asn)]:
        if cust in single_homed_asns:
            single_homed.add(cust)
    return list(single_homed)

def geo_distributed_coverage(subsets, k, superset):
    print "Geodistributed selection"
    superset_paths = copy.deepcopy(superset)
    subsets_copy = copy.deepcopy(subsets)
    nodes_covered = []
    mmts_ordered = []
    countries_covered = []
    measurements = subsets.keys()
    country_codes_shuffled_copy = copy.deepcopy(country_codes_shuffled)
    measurements = subsets.keys()
    random.shuffle(measurements)
    for mmt in measurements:
        if not superset_paths:
	    print "Finished Early!"
	    return mmts_ordered, nodes_covered
        if len(mmts_ordered) < len(country_codes_shuffled):
            if mmt in asn_to_cc and asn_to_cc[mmt] in countries_covered:
                continue
            if mmt not in asn_to_cc: continue
    	    gain = set(subsets_copy[mmt]).intersection(superset_paths)
	    nodes_covered.append(list(gain))
	    mmts_ordered.append(mmt)
	    subsets_copy.pop(mmt)
	    superset_paths = superset_paths.difference(gain)
            countries_covered.append(asn_to_cc[mmt])
        else:
            break
    # These are the left over measurements
    measurements = list(set(measurements).difference(set(mmts_ordered)))
    mmts_per_country = {}
    for mmt in measurements:
        if mmt not in asn_to_cc:
            print "ASN", mmt, "not in mapping"
            asn_to_cc[mmt] = 'XX'
        cc = asn_to_cc[mmt]
        if cc not in mmts_per_country:
            mmts_per_country[cc] = [mmt]
        else:
            mmts_per_country[cc].append(mmt)
        
    while superset_paths:
        random_cc = random.choice(mmts_per_country.keys())
        possible_mmts = mmts_per_country[random_cc]
        mmt = random.choice(possible_mmts)
        mmts_per_country[random_cc].remove(mmt)
        if not mmts_per_country[random_cc]:
            mmts_per_country.pop(random_cc)
        assert mmt not in mmts_ordered
    	gain = set(subsets_copy[mmt]).intersection(superset_paths)
        nodes_covered.append(list(gain))
        mmts_ordered.append(mmt)
	subsets_copy.pop(mmt)
	superset_paths = superset_paths.difference(gain)
    return mmts_ordered, nodes_covered
                        
def random_coverage(subsets, k, superset):
    print "Random selection"
    superset_paths = copy.deepcopy(superset)
    subsets_copy = copy.deepcopy(subsets)
    nodes_covered = []
    mmts_ordered = []
    measurements = subsets.keys()
    random.shuffle(measurements)
    for mmt in measurements:
    	if not superset_paths:
	   print "Finished Early!"
	   return mmts_ordered, nodes_covered
    	gain = set(subsets_copy[mmt]).intersection(superset_paths)
	nodes_covered.append(list(gain))
	mmts_ordered.append(mmt)
	subsets_copy.pop(mmt)
	superset_paths = superset_paths.difference(gain)
    return mmts_ordered, nodes_covered

def greedy_max_coverage(subsets, k, superset):
    superset_paths = copy.deepcopy(superset)
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
            print len(nodes_covered[-1]), len(nodes_covered[-2])
            assert len(nodes_covered[-1]) <= len(nodes_covered[-2])
        subsets_copy.pop(max_gain_mmt)
        mmts_ordered.append(max_gain_mmt)
    return mmts_ordered, nodes_covered

if len(sys.argv) > 2:
    if sys.argv[2] == 'random':
        greedy_max_coverage = random_coverage
        strategy = "random"
    elif sys.argv[2] == 'geod':
        greedy_max_coverage = geo_distributed_coverage
        strategy = "geod"
else:
    strategy="greedy"
        
overall_mmt_gain = {}
overall_single_homed_gain_content = set()
for top_content_pref in top_content_prefs:
    if top_content_pref == '0.0.0.0': continue
    print "Top content prefix", top_content_pref
    asn = ip2asn.ip2asn_bgp(top_content_pref)
    if asn not in asn_graphs:
        print "DO NOT HAVE ASN GRAPH OF PREF", top_content_pref
        continue
    print "Prefix belongs to asn", asn
    print "Getting the ASN graph for", top_content_pref
    print "Graph is a tree?", nx.is_tree(gr)
    gr = asn_graphs[asn]
    print "Greedily define the utilities of all paths from RIPE probes toward", top_content_pref
    
    init_utility_per_mmt = {}
    superset = set() # contains all the nodes that can be covered if we were to run measurements from
    # all ripe nodes.
    for ripe_asn in probes_per_asn:
        single_homed_gain = get_single_homed_customers(ripe_asn)
        overall_single_homed_gain_content = overall_single_homed_gain_content.union(set(single_homed_gain))
        try:
            sp = nx.shortest_path(gr, str(ripe_asn), str(asn))
        except nx.exception.NetworkXError, e:
            print "In the graph of", asn, top_content_pref, e
            continue
        utility = set(single_homed_gain).union(set(sp))
        init_utility_per_mmt[str(ripe_asn)] = utility
        superset = superset.union(set(single_homed_gain).union(set(sp)))
        
    measurement_gain = []
    #for k in range(1, len(init_utility_per_mmt) + 1):
    mmt_subset, coverage = greedy_max_coverage(init_utility_per_mmt,
                                               len(init_utility_per_mmt) + 1,
                                               superset)
    measurement_gain.append([mmt_subset, coverage])
    overall_mmt_gain[top_content_pref] = measurement_gain

with open("top_content_coverage-%s-%s-nosh.json" % (type_probes, strategy), "w") as fi:
    json.dump(overall_mmt_gain, fi)

overall_mmt_gain = {}
for asn, random_pref in random_prefs.iteritems():
    gr = asn_graphs[asn]
    print "Greedily define the utilities of all paths from RIPE probes toward", random_pref
    
    init_utility_per_mmt = {}
    superset = set() # contains all the nodes that can be covered if we were to run measurements from
    # all ripe nodes.
    for ripe_asn in probes_per_asn:
        single_homed_gain = get_single_homed_customers(ripe_asn)
        try:
            sp = nx.shortest_path(gr, str(ripe_asn), str(asn))
        except nx.exception.NetworkXError, e:
            print "In the graph of", asn, random_pref, e
            continue
        init_utility_per_mmt[str(ripe_asn)] = set(single_homed_gain).union(set(sp))
        superset = superset.union(set(single_homed_gain).union(set(sp)))
        
    measurement_gain = []
    mmt_subset, coverage = greedy_max_coverage(init_utility_per_mmt,
                                               len(init_utility_per_mmt) + 1,
                                               superset)
    measurement_gain.append([mmt_subset, coverage])
    overall_mmt_gain[random_pref] = measurement_gain
    
with open("random_coverage-%s-%s-nosh.json" % (type_probes, strategy), "w") as fi:
    json.dump(overall_mmt_gain, fi)

overall_mmt_gain = {}
for asn, top_cust_cone_pref in top_cust_cone_prefs.iteritems():
    gr = asn_graphs[asn]
    print "Greedily define the utilities of all paths from RIPE probes towards top_cust_cone_pref", \
        top_cust_cone_pref
    
    init_utility_per_mmt = {}
    superset = set() # contains all the nodes that can be covered if we were to run measurements from
    # all ripe nodes.
    for ripe_asn in probes_per_asn:
        single_homed_gain = get_single_homed_customers(ripe_asn)
        try:
            sp = nx.shortest_path(gr, str(ripe_asn), str(asn))
        except nx.exception.NetworkXError, e:
            print "In the graph of", asn, top_cust_cone_pref, e
            continue
        init_utility_per_mmt[str(ripe_asn)] = set(single_homed_gain).union(set(sp))
        superset = superset.union(set(single_homed_gain).union(set(sp)))
        
    measurement_gain = []
    mmt_subset, coverage = greedy_max_coverage(init_utility_per_mmt,
                                               len(init_utility_per_mmt) + 1,
                                               superset)
    measurement_gain.append([mmt_subset, coverage])
    overall_mmt_gain[top_cust_cone_pref] = measurement_gain

with open("top_cust_cone_coverage-%s-%s-nosh.json" % (type_probes, strategy), "w") as fi:
    json.dump(overall_mmt_gain, fi)

overall_mmt_gain = {}
for asn, top_eyeball_pref in top_eyeball_prefs.iteritems():
    gr = asn_graphs[asn]
    print "Greedily define the utilities of all paths from RIPE probes towards top_eyeball_pref", \
        top_eyeball_pref
    
    init_utility_per_mmt = {}
    superset = set() # contains all the nodes that can be covered if we were to run measurements from
    # all ripe nodes.
    for ripe_asn in probes_per_asn:
        single_homed_gain = get_single_homed_customers(ripe_asn)
        try:
            sp = nx.shortest_path(gr, str(ripe_asn), str(asn))
        except nx.exception.NetworkXError, e:
            print "In the graph of", asn, top_cust_cone_pref, e
            continue
        init_utility_per_mmt[str(ripe_asn)] = set(single_homed_gain).union(set(sp))
        superset = superset.union(set(single_homed_gain).union(set(sp)))
        
    measurement_gain = []
    mmt_subset, coverage = greedy_max_coverage(init_utility_per_mmt,
                                               len(init_utility_per_mmt) + 1,
                                               superset)
    measurement_gain.append([mmt_subset, coverage])
    overall_mmt_gain[top_eyeball_pref] = measurement_gain

with open("top_eyeball_coverage-%s-%s-nosh.json" % (type_probes, strategy), "w") as fi:
    json.dump(overall_mmt_gain, fi)
