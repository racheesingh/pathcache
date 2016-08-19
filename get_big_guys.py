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


files = [ x for x in os.listdir( settings.GRAPH_DIR_RIPE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_RIPE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_RIPE, f ) for f in files ]

all_graphs = {}

for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    overall_origins = {}
    all_graphs[int(asn)] = gr

pdb.set_trace()
edges_dict = {}
nodes_dict = {}
for asn, gr in all_graphs.iteritems():
    edges_dict[asn] = gr.num_edges()
    nodes_dict[asn] = gr.num_vertices()

pdb.set_trace()
edges_sorted = sorted(edges_dict.iteritems(), key=lambda x: x[1], reverse=True)
nodes_sorted = sorted(nodes_dict.iteritems(), key=lambda x: x[1], reverse=True)
