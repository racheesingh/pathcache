#!/usr/bin/python
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings

# Source values
RIPE = 1
CAIDA = 2
IPLANE = 3
BGP = 4

def get_new_gr(gr=None):
    if not gr:
        gr = Graph()
    if 'RIPE' not in gr.ep:
        tprop = gr.new_edge_property('bool')
        gr.edge_properties["RIPE"] = tprop
    if 'CAIDA' not in gr.ep:
        tprop = gr.new_edge_property('bool')
        gr.edge_properties["CAIDA"] = tprop
    if 'IPLANE' not in gr.ep:
        tprop = gr.new_edge_property('bool')
        gr.edge_properties["IPLANE"] = tprop
    if 'BGP' not in gr.ep:
        tprop = gr.new_edge_property('bool')
        gr.edge_properties["BGP"] = tprop
    if 'conf' not in gr.ep:
        tprop = gr.new_edge_property('int16_t')
        gr.edge_properties["conf"] = tprop
    if 'asn' not in gr.vp:
        vprop_asn = gr.new_vertex_property("int")
        gr.vp.asn = vprop_asn
    if 'type' not in gr.ep:
        eprop_type = gr.new_edge_property("short")
        gr.ep.type = eprop_type
    if 'origin' not in gr.ep:
        eprop_origin = gr.new_edge_property("object")
        gr.ep.origin = eprop_origin

    return gr

all_gr_counts = {}
all_graphs = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_RIPE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_RIPE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_RIPE, f ) for f in files ]

for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    gr = get_new_gr(gr)
    for edge in gr.edges():
        gr.ep["RIPE"][edge] = True
        gr.ep["conf"][edge] = 1
    #dst_vertex = find_vertex(gr, gr.vp.asn, int(asn))
    #if dst_vertex:
    #    dst_vertex = dst_vertex[0]
    #    graph_draw(gr, pos=sfdp_layout(gr), vertex_font_size=3, vertex_text=gr.vp.asn,
    #               output="graphs/viz/%s-sfdp.pdf" % asn)
    #    graph_draw(gr, pos=radial_tree_layout(gr, dst_vertex), weighted=True, r=1.5,
    #               vertex_font_size=3, vertex_text=gr.vp.asn, output="graphs/viz/%s-radial.pdf" % asn)
    all_graphs[asn] = gr
    if asn not in all_gr_counts:
        all_gr_counts[asn] = {}

print "Loaded Ripe graphs in memory"
print len( all_graphs.keys() )

files = [ x for x in os.listdir( settings.GRAPH_DIR_CAIDA ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_CAIDA, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_CAIDA, f ) for f in files ]

for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing CAIDA graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    if asn in all_graphs:
        gr_asn = all_graphs[asn]
    else:
        gr_asn = get_new_gr()

    for edge in gr.edges_iter(data=True):
        src = int(edge[0])
        dst = int(edge[1])
        try:
            new_src = find_vertex(gr_asn, gr_asn.vp.asn, src)
            if new_src:
                assert len(new_src) == 1
                new_src = new_src[0]
            else:
                new_src = gr_asn.add_vertex()
                gr_asn.vp.asn[new_src] = src

            new_dst = find_vertex(gr_asn, gr_asn.vp.asn, dst)
            if new_dst:
                assert len(new_dst) == 1
                new_dst = new_dst[0]
            else:
                new_dst = gr_asn.add_vertex()
                gr_asn.vp.asn[new_dst] = dst

            if gr_asn.edge(int(new_src), int(new_dst)):
                existing_edge = gr_asn.edge(int(new_src), int(new_dst))
                gr_asn.ep.conf[existing_edge] += 1
                gr_asn.ep.CAIDA[existing_edge] = True
                existing_origin = gr_asn.ep.origin[existing_edge]
                caida_origin = edge[-1]['origin']
                combined_origin = {}
                unique_origins = list(set(existing_origin.keys()).union(set(caida_origin.keys())))
                for sasn in unique_origins:
                    count = 0
                    if sasn in caida_origin:
                        count += caida_origin[sasn]
                    if sasn in existing_origin:
                        count += existing_origin[sasn]
                    combined_origin[sasn] = count
                gr_asn.ep.origin[existing_edge] = combined_origin
            else:
                new_edge = gr_asn.add_edge(new_src, new_dst)
                gr_asn.ep.conf[new_edge] = 1
                if edge[2]['type'] == 'i':
                    gr_asn.ep.type[new_edge] = 1
                else:
                    gr_asn.ep.type[new_edge] = 0
                gr_asn.ep.origin[new_edge] = edge[-1]['origin']
                gr_asn.ep.CAIDA[new_edge] = True
        except OverflowError:
            continue
    all_graphs[asn] = gr_asn

files = [ x for x in os.listdir( settings.GRAPH_DIR_IPLANE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_IPLANE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_IPLANE, f ) for f in files ]

for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing Iplane graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    if asn in all_graphs:
        gr_asn = all_graphs[asn]
    else:
        gr_asn = get_new_gr()
    for edge in gr.edges_iter(data=True):
        src = int(edge[0])
        dst = int(edge[1])
        try:
            new_src = find_vertex(gr_asn, gr_asn.vp.asn, src)
            if new_src:
                assert len(new_src) == 1
                new_src = new_src[0]
            else:
                new_src = gr_asn.add_vertex()
                gr_asn.vp.asn[new_src] = src

            new_dst = find_vertex(gr_asn, gr_asn.vp.asn, dst)
            if new_dst:
                assert len(new_dst) == 1
                new_dst = new_dst[0]
            else:
                new_dst = gr_asn.add_vertex()
                gr_asn.vp.asn[new_dst] = dst

            if gr_asn.edge(new_src, new_dst):
                existing_edge = gr_asn.edge(new_src, new_dst)
                gr_asn.ep.conf[existing_edge] += 1
                gr_asn.ep.IPLANE[existing_edge] = True
                existing_origin = gr_asn.ep.origin[existing_edge]
                iplane_origin = edge[-1]['origin']
                combined_origin = {}
                unique_origins = list(set(existing_origin.keys()).union(set(iplane_origin.keys())))
                for sasn in unique_origins:
                    count = 0
                    if sasn in iplane_origin:
                        count += iplane_origin[sasn]
                    if sasn in existing_origin:
                        count += existing_origin[sasn]
                    combined_origin[sasn] = count
                gr_asn.ep.origin[existing_edge] = combined_origin
            else:
                new_edge = gr_asn.add_edge(new_src, new_dst)
                gr_asn.ep.conf[new_edge] = 1
                gr_asn.ep.IPLANE[new_edge] = True
                if edge[2]['type'] == 'i':
                    gr_asn.ep.type[new_edge] = 1
                else:
                    gr_asn.ep.type[new_edge] = 0
                gr_asn.ep.origin[new_edge] = edge[-1]['origin']
        except OverflowError:
            continue
    all_graphs[asn] = gr_asn
'''
=======
                gr_asn.ep.type[new_edge] = 0
        except OverflowError:
            continue
    all_graphs[asn] = gr_asn

>>>>>>> fc3d2764437ed4fe5787beffcf0b395c6bf4dd9a
files = [ x for x in os.listdir( settings.GRAPH_DIR_BGP ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_BGP, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_BGP, f ) for f in files ]

for f in files:
    asn = f.split( '/' )[ -1 ]
    print "Parsing BGP graph for", asn
    with open( f ) as fi:
        jsonStr = json.load( fi )
    gr = json_graph.node_link_graph( jsonStr )
    del jsonStr
    if asn in all_graphs:
        gr_asn = all_graphs[asn]
    else:
        gr_asn = get_new_gr()

    for edge in gr.edges_iter(data=True):
        src = int(edge[0])
        dst = int(edge[1])
        try:
            new_src = find_vertex(gr_asn, gr_asn.vp.asn, src)
            if new_src:
                assert len(new_src) == 1
                new_src = new_src[0]
            else:
                new_src = gr_asn.add_vertex()
                gr_asn.vp.asn[new_src] = src

            new_dst = find_vertex(gr_asn, gr_asn.vp.asn, dst)
            if new_dst:
                assert len(new_dst) == 1
                new_dst = new_dst[0]
            else:
                new_dst = gr_asn.add_vertex()
                gr_asn.vp.asn[new_dst] = dst

            if gr_asn.edge(new_src, new_dst):
                existing_edge = gr_asn.edge(new_src, new_dst)
                gr_asn.ep.conf[existing_edge] += 1
                gr_asn.ep.BGP[existing_edge] = True
            else:
                new_edge = gr_asn.add_edge(new_src, new_dst)
                gr_asn.ep.conf[new_edge] = 1
                gr_asn.ep.BGP[new_edge] = True
                gr_asn.ep.type[new_edge] = 0
        except OverflowError:
            continue
    all_graphs[asn] = gr_asn
<<<<<<< HEAD
'''

for asn, gr in all_graphs.iteritems():
    print asn
    try:
        if not gr: continue
        if not find_vertex(gr, gr.vp.asn, asn):
            print "No root node in this graph!, skipping"
            continue
        gr.save(settings.GRAPH_DIR_FINAL + '%s.gt' % asn)
        print "Saved", asn
    except:
        pass
