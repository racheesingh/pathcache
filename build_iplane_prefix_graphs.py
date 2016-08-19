#!/usr/bin/python
import mkit.iplane.parse as iparse
import networkx as nx
from networkx.readwrite import json_graph
import json
import settings
import pdb

dest_aspaths = iparse.get_iplane_prefix_graphs('2016_05_30')

dest_based_graphs = {}
for tup, aspaths in dest_aspaths.iteritems():
    asn = tup[0]
    dst = tup[1]
    assert dst not in dest_based_graphs
    G = nx.DiGraph()
    G.add_node(asn, prefix=dst)
    for aspath in aspaths:
        if asn not in aspath[-1]: continue
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
    dest_based_graphs[dst] = G

pdb.set_trace()
for asn, gr in dest_based_graphs.iteritems():
    if not gr: continue
    try:
        data = json_graph.node_link_data( gr )
        s = json.dumps( data )
        with open(settings.GRAPH_DIR_IPLANE_PREF + str(asn), "w") as f:
            f.write( s )
    except:
        pdb.set_trace()

