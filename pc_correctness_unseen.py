 #!/usr/bin/python
import socket
import mkit.ripeatlas.probes as ripeprobes
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import pdb
import urllib
import urllib2
import mkit.inference.ip_to_asn as ip2asn
import json
import datetime
import settings
import os
import math
from graph_tool.all import *
import os
import settings
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

all_graphs = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]

TOTAL_MMT_PER_SRC = 20
for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "Graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    overall_origins = {}
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        for src in origin_dict:
            if src not in overall_origins:
                overall_origins[src] = 0
            overall_origins[src] += origin_dict[src]
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        origin_dict_new = {}
        for src in origin_dict:
            origin_dict_new[src] = \
                                   (float(origin_dict[src])*TOTAL_MMT_PER_SRC)/overall_origins[src]
        gr.ep.origin[e] = origin_dict_new
    eprop_prob = gr.new_edge_property("float")
    gr.ep.prob = eprop_prob
    for e in gr.edges():
        origin_dict = gr.ep.origin[e]
        prob_src = 1.0/len(overall_origins)
        e_prob = 0
        for src in origin_dict:
            e_prob += (prob_src * origin_dict[src])/TOTAL_MMT_PER_SRC
        gr.ep.prob[e] = e_prob
    all_graphs[int(asn)] = gr

def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
    return False

def dfs_paths(gr, src_node, dst_node):
    visited = set()
    stack = [(src_node, [src_node])]
    while stack:
        (vertex, path) = stack.pop()
        visited.add(vertex)
        for next in set(vertex.out_neighbours()):
            if next in visited:
                continue
            if next == dst_node:
                yield path + [next]
            else:
                stack.append((next, path + [next]))

def get_path_prob(p, gr):
    prob = 0
    for first, second in zip(p, p[1:]):
        e = gr.edge(first, second)
        prob += math.log(gr.ep.prob[e])
    return prob

def get_path( src, dst):
    if dst in all_graphs:
        gr = all_graphs[dst]
        # Find all paths from src to dst in this graph
        src_node = find_vertex(gr, gr.vp.asn, int(src))
        if not src_node:
            return None
        src_node = src_node[0]
        dst_node = find_vertex(gr, gr.vp.asn, int(dst))
        assert dst_node
        dst_node = dst_node[0]
        src_dst_paths = dfs_paths(gr, src_node, dst_node)
        #first = next(src_dst_paths, None)
        #if not first:
        #    print "Could not find a path from data plane measurements."
        #    src_dst_paths = dfs_paths(gr, src_node, dst_node)
        paths = []
        count = 0
        for p in src_dst_paths:
            count += 1
            if count > 10000:
                break
            pr = get_path_prob(p, gr)
            paths.append((pr, p))
        paths_sorted = sorted(paths, key=lambda x:x[0], reverse=True)
        paths_sorted = paths_sorted[:10]
        final_paths = []
        for prob, p in paths_sorted:
            path_ases = [gr.vp.asn[x] for x in p]
            final_paths.append((prob, path_ases))
        return final_paths
    else:
        return []

def get_most_probable_path(src, dst, threshold=2):
    paths = get_path(src, dst)
    paths = sorted(paths, key=lambda x: x[0], reverse=True)
    paths = paths[:threshold]
    final_paths = []
    for p in paths:
        final_paths.append(p[1])
    return final_paths


msms = [(59, 8075, 3793363), (73, 8075, 3793364), (262438, 8075, 3793366), (553, 8075, 3793367), (681, 8075, 3793368), (59, 23820, 3793369), (73, 23820, 3793370), (553, 23820, 3793371), (681, 23820, 3793372), (271, 23820, 3793375), (59, 22414, 3793376), (73, 22414, 3793377), (553, 22414, 3793378), (681, 22414, 3793380), (271, 22414, 3793381), (59, 62787, 3793382), (73, 62787, 3793383), (553, 62787, 3793386), (681, 62787, 3793387), (271, 62787, 3793388), (59, 47764, 3793389), (327750, 47764, 3793390), (73, 47764, 3793391), (196705, 47764, 3793392), (239, 47764, 3793393), (59, 13335, 3793394), (73, 13335, 3793395), (553, 13335, 3793409), (681, 13335, 3793410), (33480, 13335, 3793411), (59, 23576, 3793412), (73, 23576, 3793413), (553, 23576, 3793414), (681, 23576, 3793415), (271, 23576, 3793417), (59, 30361, 3793418), (73, 30361, 3793419), (553, 30361, 3793420), (681, 30361, 3793421), (197422, 30361, 3793422), (251, 14618, 3793423), (553, 14618, 3793425), (197422, 14618, 3793426), (1267, 14618, 3793427), (34177, 14618, 3793429), (59, 16798, 3793430), (73, 16798, 3793431), (553, 16798, 3793432), (681, 16798, 3793433), (1267, 16798, 3793434), (1267, 46489, 3793441), (1764, 46489, 3793442), (35244, 46489, 3793443), (35540, 46489, 3793444), (3242, 46489, 3793446), (59, 32934, 3793447), (73, 32934, 3793448), (251, 32934, 3793449), (262438, 32934, 3793450), (196965, 32934, 3793451), (59, 23724, 3793452), (327750, 23724, 3793453), (73, 23724, 3793454), (196705, 23724, 3793455), (251, 23724, 3793456), (59, 45102, 3793457), (73, 45102, 3793458), (553, 45102, 3793459), (681, 45102, 3793460), (1267, 45102, 3793463), (59, 10929, 3793465), (73, 10929, 3793466), (553, 10929, 3793467), (681, 10929, 3793468), (197422, 10929, 3793469), (59, 47541, 3793470), (73, 47541, 3793471), (553, 47541, 3793472), (681, 47541, 3793473), (197422, 47541, 3793474), (59, 13238, 3793475), (73, 13238, 3793476), (553, 13238, 3793477), (681, 13238, 3793478), (271, 13238, 3793480), (197422, 5662, 3793481), (1267, 5662, 3793482), (1764, 5662, 3793483), (35244, 5662, 3793484), (35366, 5662, 3793485), (59, 14907, 3793486), (327750, 14907, 3793487), (73, 14907, 3793488), (196705, 14907, 3793489), (239, 14907, 3793490), (59, 46652, 3793491), (327750, 46652, 3793492), (73, 46652, 3793493), (196705, 46652, 3793494), (251, 46652, 3793495), (73, 9802, 3793496), (553, 9802, 3793497), (681, 9802, 3793498), (271, 9802, 3793501), (1653, 9802, 3793502), (59, 15169, 3793503), (73, 15169, 3793504), (262438, 15169, 3793505), (196965, 15169, 3793506), (553, 15169, 3793507), (59, 37963, 3793508), (73, 37963, 3793509), (251, 37963, 3793510), (553, 37963, 3793511), (681, 37963, 3793512), (59, 9924, 3793513), (73, 9924, 3793514), (553, 9924, 3793517), (681, 9924, 3793518), (719, 9924, 3793519), (59, 4808, 3793520), (327750, 4808, 3793521), (73, 4808, 3793522), (196705, 4808, 3793523), (251, 4808, 3793524), (59, 33612, 3793525), (73, 33612, 3793526), (553, 33612, 3793527), (681, 33612, 3793528), (1267, 33612, 3793529), (59, 714, 3793530), (73, 714, 3793531), (553, 714, 3793532), (681, 714, 3793533), (197422, 714, 3793534), (59, 2635, 3793535), (73, 2635, 3793536), (553, 2635, 3793537), (681, 2635, 3793538), (1267, 2635, 3793539), (59, 4812, 3793540), (73, 4812, 3793541), (553, 4812, 3793542), (681, 4812, 3793543), (271, 4812, 3793545), (59, 14413, 3793546), (553, 14413, 3793549), (681, 14413, 3793550), (1267, 14413, 3793551), (271, 14413, 3793552), (73, 2510, 3793553), (553, 2510, 3793554), (681, 2510, 3793555), (1653, 2510, 3793560), (1887, 2510, 3793561), (59, 19024, 3793562), (327750, 19024, 3793563), (73, 19024, 3793564), (196705, 19024, 3793566), (251, 19024, 3793567), (59, 53334, 3793568), (73, 53334, 3793569), (553, 53334, 3793578), (681, 53334, 3793579), (197422, 53334, 3793580), (59, 35415, 3793581), (73, 35415, 3793582), (553, 35415, 3793583), (681, 35415, 3793584), (1267, 35415, 3793586), (59, 7643, 3793587), (73, 7643, 3793588), (553, 7643, 3793589), (681, 7643, 3793591), (271, 7643, 3793593), (59, 29789, 3793594), (73, 29789, 3793595), (553, 29789, 3793597), (681, 29789, 3793598), (197422, 29789, 3793600), (59, 19679, 3793601), (73, 19679, 3793602), (553, 19679, 3793604), (681, 19679, 3793605), (197422, 19679, 3793606), (59, 54113, 3793607), (73, 54113, 3793608), (196965, 54113, 3793611), (553, 54113, 3793613), (681, 54113, 3793624), (59, 4134, 3793639), (327750, 4134, 3793644), (73, 4134, 3793646), (196705, 4134, 3793654), (251, 4134, 3793676), (59, 13414, 3793682), (73, 13414, 3793685), (262438, 13414, 3793693), (553, 13414, 3793695), (681, 13414, 3793696), (59, 5719, 3793697), (73, 5719, 3793698), (553, 5719, 3793699), (681, 5719, 3793701), (271, 5719, 3793745), (197422, 36459, 3793757), (1267, 36459, 3793758), (35244, 36459, 3793763), (35366, 36459, 3793764), (35592, 36459, 3793765), (59, 4847, 3793766), (73, 4847, 3793767), (553, 4847, 3793770), (681, 4847, 3793771), (197422, 4847, 3793777), (59, 17012, 3793778), (73, 17012, 3793779), (251, 17012, 3793803), (553, 17012, 3793804), (681, 17012, 3793806), (59, 26101, 3793807), (73, 26101, 3793808), (251, 26101, 3793809), (262438, 26101, 3793810), (553, 26101, 3793811), (59, 39572, 3793812), (73, 39572, 3793813), (553, 39572, 3793815), (681, 39572, 3793820), (197422, 39572, 3793821), (197422, 11643, 3793823), (1267, 11643, 3793825), (35244, 11643, 3793831), (35366, 11643, 3793832), (35592, 11643, 3793833), (59, 24572, 3793834), (251, 24572, 3793835), (553, 24572, 3793836), (681, 24572, 3793837), (197422, 24572, 3793838), (59, 16509, 3793839), (327750, 16509, 3793840), (73, 16509, 3793841), (251, 16509, 3793842), (553, 16509, 3793843), (59, 36351, 3793845), (73, 36351, 3793846), (553, 36351, 3793849), (681, 36351, 3793851), (197422, 36351, 3793852)]

path_comp = []
for tup in msms:
    msm = tup[-1]
    src_asn = tup[0]
    dst_asn = tup[1]
    try:
        data = parse.parse_msm_trcrt(msm)
    except:
        pdb.set_trace()
        continue
    for d in data:
        src_asn = int(ripeprobes.get_probe_asn(d['prb_id']))
        dst_asn = int(ip2asn.ip2asn_bgp(d['dst_addr']))
        if not path_in_cache(src_asn, dst_asn): continue
            
        aslinks = asp.traceroute_to_aspath(d)
        if not aslinks['_links']: continue
        aslinks = ixp.remove_ixps(aslinks)
        hops = []
        for link in aslinks:
            hops.append(int(link['src']))
        hops.append(int(link['dst']))
        pc_path = get_most_probable_path(src_asn, dst_asn, threshold=5)
        path_comp.append([pc_path, hops])
pdb.set_trace()
accuracy = 0
for p in path_comp:
    if p[-1] in p[0]:
        accuracy += 1

print accuracy, len(path_comp)
with open("cipollino-verify/path_comp_unseen_accuracy", "w") as fi:
    json.dump(path_comp, fi)
