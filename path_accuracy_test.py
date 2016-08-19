#!/usr/bin/python
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
from graph_tool.all import *

API_HOST = 'https://atlas.ripe.net'
API_MMT_URI = 'api/v1/measurement'

def load_dest_graphs():
    files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
              if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
    files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]
    all_graphs = {}
    for f in files:
        asn_to_id = {}
        asn = f.split( '/' )[ -1 ].split('.')[0]
        gr = load_graph(f, fmt="gt")
        remove_parallel_edges(gr)
        remove_self_loops(gr)
        all_graphs[int(asn)] = gr
    return all_graphs

def num_sources(edge, gr):
    rp = gr.ep.RIPE
    cp = gr.ep.CAIDA
    ip = gr.ep.IPLANE
    src_count = 0
    if rp[edge] == 1:
        src_count +=  1
    if cp[edge] == 1:
        src_count +=  1
    if ip[edge] == 1:
        src_count +=  1
    return src_count

all_graphs = load_dest_graphs()
per_graph_stats = {}
'''
for asn, gr in all_graphs.iteritems():
    rp = gr.ep.RIPE
    cp = gr.ep.CAIDA
    ip = gr.ep.IPLANE
    # only RIPE
    GV = GraphView(gr, efilt=lambda e:rp[e] == 1 and cp[e] != 1 and ip[e] != 1)
    num_ripe = GV.num_edges()

    GV = GraphView(gr, efilt=lambda e:rp[e] != 1 and cp[e] != 1 and ip[e] == 1)
    num_iplane = GV.num_edges()

    GV = GraphView(gr, efilt=lambda e:rp[e] != 1 and cp[e] == 1 and ip[e] != 1)
    num_caida = GV.num_edges()

    GV = GraphView(gr, efilt=lambda e:num_sources(e, gr) > 1)
    num_multiple = GV.num_edges()
    per_graph_stats[asn] = {'ripe': num_ripe, 'caida': num_caida, 'iplane':num_iplane, 'multiple':num_multiple}
'''
for asn, gr in all_graphs.iteritems():
    rp = gr.ep.RIPE
    cp = gr.ep.CAIDA
    ip = gr.ep.IPLANE
    num_types = {'ripe':0, 'caida':0, 'iplane':0, 'multiple':0}
    for e in gr.edges():
        if rp[e] == 1 and cp[e] != 1 and ip[e] != 1)
        
with open("per_graph_stats_june24", "w") as fi:
    json.dump(per_graph_stats, fi)

pdb.set_trace()

def fetch_json( offset=0 ):
    data = []
    timestamp  = int( (datetime.datetime.utcnow() - \
                       datetime.datetime( 1970, 1, 1 ) ).total_seconds() ) - ( 60 * 60 * 24 * 30)
    
    api_args = dict( offset=offset, use_iso_time="true", description__startswith="PCExitAccuracy",
                     start_time__gt="%s" % timestamp, type="traceroute" )
    url = "%s/%s/?%s" % ( API_HOST, API_MMT_URI, urllib.urlencode( api_args ) )
    print url
    response = urllib2.urlopen( url )
    data = json.load( response )
    return data

count = 0
msms = []
while( 1 ):
    try:
        data = fetch_json( offset=count*100 )
    except urllib2.HTTPError:
        break
    if not data[ 'objects' ]:
        break
    for d in data[ 'objects' ]:
        if d[ 'dst_asn' ]:
            msms.append(d[ 'msm_id' ])
        else:
            try:
                msms.append( d[ 'msm_id' ] )
            except:
                pass
    count += 1

print len(msms)
def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
    return False

def dfs_paths(gr, src_node, dst_node):
    stack = [(src_node, [src_node])]
    while stack:
        (vertex, path) = stack.pop()
        for next in set(vertex.out_neighbours()) - set(path):
            if next == dst_node:
                yield path + [next]
                #return path + [next]
            else:
                stack.append((next, path + [next]))

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
        rp = gr.ep.RIPE
        cp = gr.ep.CAIDA
        ip = gr.ep.IPLANE
        GV = GraphView(gr, efilt=lambda e:rp[e] == 1 or cp[e] == 1 or ip[e] == 1)
        src_dst_paths = dfs_paths(GV, src_node, dst_node)
        first = next(src_dst_paths, None)
        if not first:
            print "Could not find a path from data plane measurements."
            src_dst_paths = dfs_paths(gr, src_node, dst_node)
        paths = []
        count = 0
        if not src_dst_paths:
            return []
        for p in src_dst_paths:
            count += 1
            if count > 5: break
            if not p: continue
            path = [gr.vp.asn[x] for x in p]
            if path:
                paths.append(path)
        return paths
    else:
        return []

def path_cache(src, dst):
    if not path_in_cache(src, dst):
        return []
    paths = get_path(src, dst)
    hops = []
    for p in paths:
        hops.extend(p)
    hops = list(frozenset(hops))
    return hops

overestimate_list = []
underestimate_list = []
for msm in msms:
    data = parse.parse_msm_trcrt(msm)
    for d in data:
        src_asn = ripeprobes.get_probe_asn(d['prb_id'])
        dst_asn = ip2asn.ip2asn_bgp(d['dst_addr'])
        aslinks = asp.traceroute_to_aspath(d)
        if not aslinks['_links']: continue
        aslinks = ixp.remove_ixps(aslinks)
        hops = []
        for link in aslinks:
            hops.append(int(link['src']))
            hops.append(int(link['dst']))
        real_path = set(hops)
        if not real_path:
            continue
        pc_path = set(path_cache(int(src_asn), int(dst_asn)))
        overestimate = len(pc_path - real_path)
        underestimate = len(real_path - pc_path)
        overestimate_list.append(overestimate)
        underestimate_list.append(underestimate)

with open("cipollino-verify/over_estimate_exit_dest", "w") as fi:
    json.dump(overestimate_list, fi)

with open("cipollino-verify/under_estimate_exit_dest", "w") as fi:
    json.dump(underestimate_list, fi)

print overestimate_list
print underestimate_list
