#!/usr/bin/python
import numpy as np
#from Atlas import Measure
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import seaborn as sns
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

destinations = []

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

num_nodes = {}
num_nodes_list = []
num_edges = {}
num_edges_list =[]
for asn, gr in all_graphs.iteritems():
    total_nodes = gr.num_vertices()
    total_edges = gr.num_edges()
    num_ripe = 0
    num_caida = 0
    num_iplane = 0
    num_bgp = 0
    for edge in gr.edges():
        if gr.ep.RIPE[edge] == 1:
            num_ripe += 1
        elif gr.ep.IPLANE[edge] == 1:
            num_iplane += 1
        elif gr.ep.CAIDA[edge] == 1:
            num_caida += 1
        elif gr.ep.BGP[edge] == 1:
            num_bgp += 1
            
    num_edges[asn] = (num_ripe, num_iplane, num_caida, num_bgp, total_edges)
    num_nodes[asn] = total_nodes
    num_nodes_list.append(total_nodes)
    num_edges_list.append(total_edges)

pdb.set_trace()
num_edges_items = num_edges.items()
num_edges_items.sort(key=lambda x:x[1][4], reverse=True)
top_edges = num_edges_items[:10]
fi = open("edges_stacked_bar", "w")
fi.write(','.join([str(x[0]) for x in top_edges]))
fi.write('\n')
fi.write(','.join([str(x[1][0]) for x in top_edges]))
fi.write('\n')
fi.write(','.join([str(x[1][1]) for x in top_edges]))
fi.write('\n')
fi.write(','.join([str(x[1][2]) for x in top_edges]))
fi.write('\n')
fi.write(','.join([str(x[1][3]) for x in top_edges]))
fi.write('\n')
fi.close()

with open("num_edges_cdf", "w") as fi:
    fi.write(','.join([str(x) for x in num_edges]))
    
with open("num_nodes_cdf", "w") as fi:
    fi.write(','.join([str(x) for x in num_nodes]))

