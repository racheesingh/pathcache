#!/usr/bin/python
import os
from graph_tool.all import *
import mkit.ripeatlas.probes as prb
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import settings
import sys
import traceback
import multiprocessing as mp
import urllib2
import json
import pdb
import urllib
import datetime
import time
import logging
import multiprocessing
logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.INFO)
logger.setLevel(multiprocessing.SUBDEBUG)

now = '-'.join( str( datetime.datetime.now() ).split() )

def are_siblings( as1, as2 ):
    print "Checking if siblings:", (as1, as2)
    print orgs[ as1 ], orgs[ as2 ]
    if as1 not in orgs or as2 not in orgs:
        return False
    return orgs[ as1 ] == orgs[ as2 ]

with  open("data/ripe/meta-1473674448-1474884048") as fi:
    msms = json.load(fi)
msms_all = list(frozenset(msms))

# tot_len =  len(msms_all)
# chunk = tot_len/3
# print chunk
# #msms_all = msms_all[:chunk]
# #msms_all = msms_all[chunk:(2*chunk)]
# msms_all = msms_all[(2*chunk):]
print len(msms_all)
pdb.set_trace()

def compute_dest_based_graphs(msms):
    dest_based_graphs = {}
    for msm in msms:
        info = parse.mmt_info(msm)
        # Start time should not be < Jan 1st 2016
        if info['start_time'] < 1451606400: continue
        if info['type']['af'] != 4: continue
        dst_asn = info['dst_asn']
        if not dst_asn:
            continue
        if not info['is_oneoff']:
            period  = int(info['interval'])
            if info['stop_time']:
                stop_time = int(info['stop_time'])
            else:
                stop_time = int(time.time())
            start = stop_time - 5 * period
            end = stop_time
            try:
                data = parse.parse_msm_trcrt(msm, start=start, end=end, count=500)
            except urllib2.HTTPError:
                continue
        else:
            data = parse.parse_msm_trcrt(msm)
        if dst_asn in dest_based_graphs:
            G = dest_based_graphs[dst_asn]
        #elif graph_on_disk(dst_asn):
        #    G = get_graph_on_disk(dst_asn)
        else:
            G = Graph()
            vprop_asn = G.new_vertex_property("int")
            G.vp.asn = vprop_asn
            eprop_type = G.new_edge_property("short")
            G.ep.type = eprop_type
            eprop_msm = G.new_edge_property("long")
            G.ep.msm = eprop_msm
            eprop_probe = G.new_edge_property("int")
            G.ep.probe = eprop_probe
            eprop_dict = G.new_edge_property("object")   
            G.ep.origin = eprop_dict

        assert 'asn' in G.vp
        assert 'type' in G.ep
        assert 'msm' in G.ep
        assert 'probe' in G.ep
        assert 'origin' in G.ep
        for d in data:
            src_asn = prb.get_probe_asn(d['prb_id'])
            if not src_asn:
                continue
            aslinks = asp.traceroute_to_aspath(d)
            if not aslinks['_links']: continue
            aslinks = ixp.remove_ixps(aslinks)
            for link in aslinks:
                if link['src'] == dst_asn:
                    break
                v1 = find_vertex(G, G.vp.asn, link['src'])
                if not v1:
                    v1 = G.add_vertex()
                    G.vp.asn[v1] = int(link['src'])
                else:
                    assert len(v1) == 1
                    v1 = v1[0]
                    
                v2 = find_vertex(G, G.vp.asn, link['dst'])
                if not v2:
                    v2 = G.add_vertex()
                    G.vp.asn[v2] = int(link['dst'])
                else:
                    assert len(v2) == 1
                    v2 = v2[0]

                assert G.vp.asn[v1] == int(link['src'])
                assert G.vp.asn[v2] == int(link['dst'])
                ed = G.edge(v1,v2)
                if not ed:
                    ed = G.add_edge(v1, v2)
                if not G.ep.origin[ed]:
                    G.ep.origin[ed] = {src_asn: 1}
                elif src_asn in G.ep.origin[ed]:
                    G.ep.origin[ed][src_asn] += 1
                else:
                    G.ep.origin[ed][src_asn] = 1
                if link['type'] == 'i':
                    G.ep.type[ed] = 1
                else:
                    G.ep.type[ed] = 0
                G.ep.msm[ed] = msm
                G.ep.probe[ed] = int(d['prb_id'])
        dest_based_graphs[dst_asn] = G

    print dest_based_graphs.keys()
    return dest_based_graphs

def wrap_function( msms ):
    try:
        return compute_dest_based_graphs( msms )
    except:
        logging.warning( "".join(traceback.format_exception(*sys.exc_info())) )
    
logging.debug( "Number of measurements in the time frame: %d" % len(msms) )

num_msm_per_process = 5
num_chunks = len( msms_all )/num_msm_per_process + 1
pool = mp.Pool(processes=45, maxtasksperchild=30)
results = []
for x in range(num_chunks):
    start = x * num_msm_per_process
    end = start + num_msm_per_process
    if end > len( msms_all ) - 1:
        end = len( msms_all )
    print start, end
    results.append(pool.apply_async(wrap_function, args=(msms_all[ start: end ],)))

pool.close()
pool.join()

output = [ p.get() for p in results ]
del results

files = [ x for x in os.listdir( settings.GRAPH_DIR_RIPE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_RIPE, x ) ) ]

def graph_on_disk_new(dst_asn):
    if str(dst_asn) + ".gt" in files:
        return True
    else:
        return False
    
def get_graph_on_disk_new(dst_asn):
    fname = os.path.join( settings.GRAPH_DIR_RIPE, str(dst_asn) + ".gt")
    gr = load_graph(fname, fmt="gt")
    return gr

def combine_graphs(G, H):
    remove_parallel_edges(G)
    remove_self_loops(G)
    remove_parallel_edges(H)
    remove_self_loops(H)

    asn_to_id = {}
    all_edges = []
    all_nodes = []
    for edge in G.edges():
        v1 = edge.source()
        v2 = edge.target()
        src = G.vp.asn[v1]
        dst = G.vp.asn[v2]
        asn_to_id[src] = int(v1)
        asn_to_id[dst] = int(v2)
        if src not in all_nodes:
            all_nodes.append(src)
        if dst not in all_nodes:
            all_nodes.append(dst)
        if (src, dst) not in all_edges:
            all_edges.append((src, dst))

    for edge in H.edges():
        v1 = edge.source()
        v2 = edge.target()
        src = H.vp.asn[v1]
        dst = H.vp.asn[v2]
        if src not in all_nodes:
            all_nodes.append(src)
            new_v1 = G.add_vertex()
            G.vp.asn[new_v1] = src
            assert src not in asn_to_id
            asn_to_id[src] = int(new_v1)
        else:
            new_v1 = G.vertex(asn_to_id[src])
        if dst not in all_nodes:
            all_nodes.append(dst)
            new_v2 = G.add_vertex()
            G.vp.asn[new_v2] = dst
            assert dst not in asn_to_id
            asn_to_id[dst] = int(new_v2)
        else:
            new_v2 = G.vertex(asn_to_id[dst])
        if (src, dst) not in all_edges:
            ed = G.add_edge(new_v1, new_v2)
            all_edges.append((src, dst))
            G.ep.type[ed] = H.ep.type[edge]
            G.ep.msm[ed] = H.ep.msm[edge]
            G.ep.probe[ed] = H.ep.probe[edge]
            G.ep.origin[ed] = H.ep.origin[edge]
        else:
            edG = G.edge(new_v1, new_v2)
            assert G.vp.asn[edG.source()] == src
            assert G.vp.asn[edG.target()] == dst
            
            # Combine edge's properties
            if not G.ep.origin[edG]:
                G_edge_copy = {}
            else:
                G_edge_copy = G.ep.origin[edG].copy()
                
            if not H.ep.origin[edge]:
                H_edge_copy = {}
            else:
                H_edge_copy = H.ep.origin[edge].copy()

            for asn, count in H_edge_copy.iteritems():
                if asn in G_edge_copy:
                    G_edge_copy[asn] = G_edge_copy[asn] + count
                else:
                    G_edge_copy[asn] = count
            G.ep.origin[edG] = G_edge_copy
    return G
                    
all_dst_based_graphs = {}
for res in output:
    print "evaluating result"
    if res and not res.values():
        logging.warning( "No graph constructed for asn %s" % res.keys() )
    elif not res:
        continue
    for asn, gr in res.iteritems():
        if asn in all_dst_based_graphs and all_dst_based_graphs[asn] and gr:
            print "Combining graphs for AS", asn
            all_dst_based_graphs[asn] = combine_graphs(all_dst_based_graphs[asn], gr)
            print all_dst_based_graphs[asn].num_edges(), gr.num_edges()
        elif gr and asn:
            if graph_on_disk_new(asn):
                gr_disk = get_graph_on_disk_new(asn)
                combined_graph = combine_graphs(gr_disk, gr)
                all_dst_based_graphs[ asn ] = combined_graph
                print "Old %d, New %d" % (gr_disk.num_edges(), combined_graph.num_edges())
            else:
                all_dst_based_graphs[ asn ] = gr
            
print len(all_dst_based_graphs.keys())

for asn, gr in all_dst_based_graphs.iteritems():
    if not gr: continue
    try:
        gr.save(settings.GRAPH_DIR_RIPE + '%s.gt' % asn)
    except:
        pdb.set_trace()
