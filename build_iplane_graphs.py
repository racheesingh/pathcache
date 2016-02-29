#!/usr/bin/python
import mkit.iplane.parse as iparse
import networkx as nx
from networkx.readwrite import json_graph
import json
import settings
import pdb

dest_aspaths = iparse.get_iplane_graphs('2016_02_27')

dest_based_graphs = {}
for dst, aspaths in dest_aspaths.iteritems():
    assert dst not in dest_based_graphs
    G = nx.DiGraph()
    for aspath in aspaths:
        for first, second in zip(aspath, aspath[1:]):
            G.add_edge(first, second)
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

