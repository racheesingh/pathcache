import settings
import json
import os
from networkx.readwrite import json_graph
import networkx as nx
import pdb

all_graphs_bgp = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_BGP ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_BGP, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_BGP, f ) for f in files ]
bgp_src_dst_pairs = 0
graph_sizes = []
for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing BGP graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    all_graphs_bgp[asn] = gr
    graph_sizes.append(gr.number_of_edges())

with open("bgp_graph_sizes", "w") as fi:
    json.dump(graph_sizes, fi)
