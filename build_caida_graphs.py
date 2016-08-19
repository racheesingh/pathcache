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

def parse_caida_json_streaming(fname):
    num_trcrts = 0
    with open(fname) as fi:
        dest_based_aspaths = {}
        for line in fi:
            num_trcrts += 1
            try:
                trcrt = json.loads(line)
            except ValueError:
                continue
            if trcrt['stop_reason'] != 'COMPLETED':
                continue
            src = trcrt['src']
            dst = trcrt['dst']
            ixp_match = ixp.ixp_radix.search_best(dst)
            if ixp_match:
                continue
            dst_asn = ip2asn.ip2asn_bgp(dst)
            src_asn = ip2asn.ip2asn_bgp(src)
            if not dst_asn:
                continue
            if dst_asn not in dest_based_aspaths:
                dest_based_aspaths[dst_asn] = []
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
                dest_based_aspaths[dst_asn].append(aslinks)
    print len(dest_based_aspaths)
    print num_trcrts
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
    for asn, aspaths in asp_list.iteritems():
        if asn in dest_based_graphs_overall:
            G = dest_based_graphs_overall[asn]
        else:
            G = nx.DiGraph()
        for aspath in aspaths:
            #for first, second in zip(aspath, aspath[1:]):
            #    G.add_edge(first, second)
            src_asn = aspath[0][0]
            for link in aspath:
                if G.has_edge(link[0], link[1]):
                    edge_data = G.get_edge_data(link[0], link[1])
                else:
                    edge_data = {}
                if 'origin' in edge_data:
                    origin = edge_data['origin']
                else:
                    origin = {}
                if src_asn not in origin:
                    count = 1
                else:
                    count = origin[src_asn] + 1
                origin[src_asn] = count
                G.add_edge(link[0], link[1], type=link[2], origin=origin)
        dest_based_graphs_overall[asn] = G

edges = []
for asn, gr in dest_based_graphs_overall.iteritems():
    if not gr: continue
    edges.append(len(gr.edges()))
    try:
        data = json_graph.node_link_data( gr )
        s = json.dumps( data )
        if str(asn) == 'None': pdb.set_trace()
        with open(settings.GRAPH_DIR_CAIDA + str(asn), "w") as f:
            f.write( s )
    except:
        pdb.set_trace()

print "total edges %d" % sum(edges)
