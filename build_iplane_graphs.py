#!/usr/bin/python
import mkit.iplane.parse as iparse
import networkx as nx
from networkx.readwrite import json_graph
import json
import settings
import pdb

dest_aspaths = iparse.get_iplane_graphs('2016_08_15')

dest_based_graphs = {}
for dst, aspaths in dest_aspaths.iteritems():
    assert dst not in dest_based_graphs
    dst = int(dst)
    G = nx.DiGraph()
    for aspath in aspaths:
        # Incomplete seeming path, does not make it to the dest
        # don't want dangling edges, so skipping these
        if dst not in aspath[-1]: continue
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

for asn, gr in dest_based_graphs.iteritems():
    if not gr: continue
    try:
        data = json_graph.node_link_data( gr )
        s = json.dumps( data )
        with open(settings.GRAPH_DIR_IPLANE + str(asn), "w") as f:
            f.write( s )
    except:
        pdb.set_trace()

