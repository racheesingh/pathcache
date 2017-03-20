#!/usr/bin/python
from itertools import groupby
import networkx as nx
from networkx.readwrite import json_graph
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import os
import pdb
import settings
import json
import glob
msms = []
num_mmts = 0
def parse_caida_json_streaming(fname):
    global num_mmts
    print num_mmts
    num_trcrts = 0
    with open(fname) as fi:
        dest_based_aspaths = {}
        for line in fi:
            num_trcrts += 1
            try:
                trcrt = json.loads(line)
            except:
                continue
            if trcrt['stop_reason'] != 'COMPLETED':
                continue
            num_mmts += 1
            src = trcrt['src']
            dst = trcrt['dst']
            rnode = ip2asn.ip_to_pref(dst)
            if rnode:
                dst_prefix = rnode.prefix.replace('/', '_')
            else:
                continue
            ixp_match = ixp.ixp_radix.search_best(dst)
            if ixp_match:
                continue
            dst_asn = int(ip2asn.ip2asn_bgp(dst))
            src_asn = ip2asn.ip2asn_bgp(src)
            if not dst_asn:
                continue
            if (dst_asn, dst_prefix) not in dest_based_aspaths:
                dest_based_aspaths[(dst_asn, dst_prefix)] = []
            aslinks = []
            last_hop_nr = None
            this_hop_nr = None
            this_hop_asn = None
            last_hop_asn = None
            for hop in trcrt['hops']:
                addr = hop['addr']
                ixp_match = ixp.ixp_radix.search_best(addr)
                if ixp_match:
                    continue
                asn = ip2asn.ip2asn_bgp(addr)
                if not asn: continue
                if asn in ixp.IXPs:
                    continue
                last_hop_nr = this_hop_nr
                this_hop_nr = hop['probe_ttl']
                last_hop_asn = this_hop_asn
                this_hop_asn = asn
                if this_hop_asn and last_hop_asn:
                    if last_hop_asn != this_hop_asn:
                        if (this_hop_nr - last_hop_nr) == 1:
                            link_type = 'd'
                        else:
                            link_type = 'i'
                        link = (int(last_hop_asn), int(this_hop_asn), link_type)
                        aslinks.append(link)
            if aslinks:
                dest_based_aspaths[(int(dst_asn), dst_prefix)].append(aslinks)
    #print len(dest_based_aspaths)
    #print num_trcrts
    return dest_based_aspaths

dest_based_aspaths = []
files = filter(os.path.isfile, glob.glob(settings.CAIDA_DATA + "*"))
print files
for fname in files:
    print "Converting %s to JSON" % fname
    convert_to_json_cmd = "sc_warts2json %s > %s" % (fname, fname+".json")
    os.system(convert_to_json_cmd)
    dest_based_aspaths.append(parse_caida_json_streaming(fname+'.json'))
    print "Removing the JSON file to save space"
    os.system("rm %s" % fname+".json")

dest_based_graphs_overall = {}
for asp_list in dest_based_aspaths:
    for tup, aspaths in asp_list.iteritems():
        dst_asn = int(tup[0])
        dst_prefix = tup[1]
        if dst_prefix in dest_based_graphs_overall:
            G = dest_based_graphs_overall[dst_prefix]
        else:
            G = nx.DiGraph()
            G.add_node(dst_asn, prefix=dst_prefix)
        for aspath in aspaths:
            src_asn = aspath[0][0]
            if dst_prefix == '216.58.219.0_24' and int(src_asn) == 16735: pdb.set_trace()
            prev_edge = None
            str_path = " ".join([str(x[0]) for x in aspath])
            str_path += " " + str(x[1])
            for link in aspath:
                if G.has_edge(link[0], link[1]):
                    edge_data = G.get_edge_data(link[0], link[1])
                else:
                    edge_data = {}

                if not prev_edge:
                    assert str(link[0]) == str(src_asn)
                    key = "gen"
                    if G.has_node(link[0]) and 'generated' in G.node[link[0]]:
                        node_data = G.node[link[0]]
                    else:
                        node_data = {'generated':0}
                    G.add_node(link[0], generated=node_data['generated']+1, str_path=str_path)
                else:
                    key = prev_edge

                if key != "gen":
                    if str(link[0]) not in key.split('-')[-1]:
                        pdb.set_trace()
                if 'origin' in edge_data:
                    origin = edge_data['origin']
                else:
                    origin = {}
                if key not in origin:
                    count = 1
                else:
                    count = origin[key] + 1
                origin[key] = count
                prev_edge = "%s-%s" % (link[0], link[1])
                G.add_edge(link[0], link[1], type=link[2], origin=origin)
        dest_based_graphs_overall[dst_prefix] = G
        #if dst_asn not in G.node:
        #    print "Didnt find dst asn in graph", dst_asn
        #    continue
        #G.node[dst_asn] = {'prefix': dst_prefix}

for dst_prefix, gr in dest_based_graphs_overall.iteritems():
    if not gr: continue
    try:
        data = json_graph.node_link_data(gr)
        s = json.dumps(data)
        if str(dst_prefix) == 'None': pdb.set_trace()
        with open(settings.GRAPH_DIR_CAIDA_PREF + str(dst_prefix), "w") as f:
            f.write( s )
    except:
        pdb.set_trace()
