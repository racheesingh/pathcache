#!/usr/bin/python
import editdistance
import alexa
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

NUM_CONTENT = 1000
websites = alexa.top_list(NUM_CONTENT)
content_asns = []
content_ips = {}
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
        content_ips[asn] = socket.gethostbyname(w[1])
    except:
        print w
        continue

f = open("data/aspop")
entries = list()
for table in f:
    records = table.split("[")
    for record in records:
        record = record.split("]")[0]
        entry = dict()
        try:
            entry["rank"] = int(record.split(",")[0])
            entry["as"] = record.split(",")[1].strip("\"")
            entry["country"] = ((record.split(",")[3]).split("=")[2]).split("\\")[0]
            entry["users"] = int(record.split(",")[4])
            if entry["rank"] > 5000:
                continue
            entries.append(entry)
        except (IndexError, ValueError):
            continue
f.close()

top_eyeballs = []
for entry in entries:
    top_eyeballs.append(int(entry['as'].split('AS')[-1]))

count = 0
fw_len = []
bw_len = []
sym = 0
asym = 1
asym_num_hops = []

pdb.set_trace()
for eyeball in top_eyeballs:
    for content in content_asns:
        if path_in_cache(eyeball, content) and path_in_cache(content, eyeball):
            count += 1
            alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't']
            f_path = get_most_probable_path(eyeball, content, threshold=1)
            b_path = get_most_probable_path(content, eyeball, threshold=1)
            if len(f_path) < 1:
                continue
                count -= 1
            if len(b_path) < 1:
                continue
                count -= 1
            f_path = f_path[0]
            b_path = b_path[0]
            if f_path == b_path:
                sym += 1
                asym_num_hops.append(0)
            else:
                asym += 1
                path_mapping = {}
                try:
                    f_str = ''
                    b_str = ''
                    for hop in f_path:
                        map_char = alphabet.pop(0)
                        path_mapping[hop] = map_char
                        f_str = f_str + map_char
                    for hop in b_path:
                        if hop in path_mapping:
                            map_char = path_mapping[hop]
                        else:
                            map_char = alphabet.pop(0)
                        path_mapping[hop] = map_char
                        b_str = b_str + map_char
                    ed = int(editdistance.eval(f_str, b_str[::-1]))
                    #diff = len(set(f_path) - set(b_path))
                    #diff += len(set(b_path) - set(f_path))
                except:
                    pdb.set_trace()
                asym_num_hops.append(ed)
            fw_len.append(len(f_path))
            bw_len.append(len(b_path))

with open("cipollino-verify/asym_num_ed", "w") as fi:
    json.dump(asym_num_hops, fi)

print "print evaluated total paths:", count
print "sym", sym, "asym", asym
print fw_len
print bw_len
with open("cipollino-verify/fw_len", "w") as fi:
    json.dump(fw_len, fi)
with open("cipollino-verify/bw_len", "w") as fi:
    json.dump(bw_len, fi)
