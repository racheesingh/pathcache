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

all_graphs = {}
files = [ x for x in os.listdir( settings.GRAPH_DIR_RIPE ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_RIPE, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_RIPE, f ) for f in files ]

for f in files:
    asn_to_id = {}
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "RIPE graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    tprop = gr.new_edge_property('int16_t')
    gr.edge_properties["source"] = tprop
    cprop = gr.new_edge_property('int16_t')
    gr.edge_properties["conf"] = cprop
    for edge in gr.edges():
        gr.ep["source"][edge] = RIPE
        gr.ep["conf"][edge] = 1
    for vertex in gr.vertices():
        asn_to_id[gr.vp.asn[vertex]] = int(vertex)
    dst_vertex = find_vertex(gr, gr.vp.asn, int(asn))
    if dst_vertex:
        dst_vertex = dst_vertex[0]
        graph_draw(gr, pos=sfdp_layout(gr), vertex_font_size=3, vertex_text=gr.vp.asn,
                   output="graphs/viz/%s-sfdp.pdf" % asn)
        graph_draw(gr, pos=radial_tree_layout(gr, dst_vertex), weighted=True, r=1.5,
                   vertex_font_size=3, vertex_text=gr.vp.asn, output="graphs/viz/%s-radial.pdf" % asn)
    all_graphs[asn] = (gr, asn_to_id)

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
        gr_asn, asn_to_id = all_graphs[asn]
    else:
        gr_asn = Graph()
        asn_to_id = {}
        vprop_asn = gr_asn.new_vertex_property("int")
        gr_asn.vp.asn = vprop_asn
        eprop_type = gr_asn.new_edge_property("short")
        gr_asn.ep.type = eprop_type
        tprop = gr_asn.new_edge_property('int16_t')
        gr_asn.ep.source = tprop
        cprop = gr_asn.new_edge_property('int16_t')
        gr_asn.edge_properties["conf"] = cprop

    for edge in gr.edges_iter(data=True):
        src = int(edge[0])
        dst = int(edge[1])
        try:
            if src in asn_to_id:
                src_id = asn_to_id[src]
                new_src = gr_asn.vertex(src_id)
            else:
                new_src = gr_asn.add_vertex()
                src_id = int(new_src)
                gr_asn.vp.asn[new_src] = src
                asn_to_id[src] = int(new_src)
            if dst in asn_to_id:
                dst_id = asn_to_id[dst]
                new_dst = gr_asn.vertex(dst_id)
            else:
                new_dst = gr_asn.add_vertex()
                dst_id = int(new_dst)
                gr_asn.vp.asn[new_dst] = dst
                asn_to_id[dst] = int(new_dst)
            if gr_asn.edge(src_id, dst_id):
                existing_edge = gr_asn.edge(src_id, dst_id)
                gr_asn.ep.conf[existing_edge] += 1
            else:
                new_edge = gr_asn.add_edge(new_src, new_dst)
                gr_asn.ep.conf[new_edge] = 1
                if edge[2]['type'] == 'i':
                    gr_asn.ep.type[new_edge] = 1
                else:
                    gr_asn.ep.type[new_edge] = 0
                gr_asn.ep.source[new_edge] = CAIDA
        except OverflowError:
            continue
    all_graphs[asn] = (gr_asn, asn_to_id)

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
        gr_asn, asn_to_id = all_graphs[asn]
    else:
        gr_asn = Graph()
        asn_to_id = {}
        vprop_asn = gr_asn.new_vertex_property("int")
        gr_asn.vp.asn = vprop_asn
        eprop_type = gr_asn.new_edge_property("short")
        gr_asn.ep.type = eprop_type
        tprop = gr_asn.new_edge_property('int16_t')
        gr_asn.ep.source = tprop
        cprop = gr_asn.new_edge_property('int16_t')
        gr_asn.edge_properties["conf"] = cprop

    for edge in gr.edges_iter(data=True):
        src = int(edge[0])
        dst = int(edge[1])
        try:
            if src in asn_to_id:
                src_id = asn_to_id[src]
                new_src = gr_asn.vertex(src_id)
            else:
                new_src = gr_asn.add_vertex()
                src_id = int(new_src)
                gr_asn.vp.asn[new_src] = src
                asn_to_id[src] = int(new_src)
            if dst in asn_to_id:
                dst_id = asn_to_id[dst]
                print "dst_id", dst_id
                new_dst = gr_asn.vertex(dst_id)
            else:
                new_dst = gr_asn.add_vertex()
                dst_id = int(new_dst)
                gr_asn.vp.asn[new_dst] = dst
                asn_to_id[dst] = int(new_dst)
            if gr_asn.edge(src_id, dst_id):
                existing_edge = gr_asn.edge(src_id, dst_id)
                gr_asn.ep.conf[existing_edge] += 1
            else:
                new_edge = gr_asn.add_edge(new_src, new_dst)
                gr_asn.ep.conf[new_edge] = 1
                gr_asn.ep.source[new_edge] = IPLANE
        except OverflowError:
            continue
    all_graphs[asn] = (gr_asn, asn_to_id)

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
        gr_asn, asn_to_id = all_graphs[asn]
    else:
        gr_asn = Graph()
        asn_to_id = {}
        vprop_asn = gr_asn.new_vertex_property("int")
        gr_asn.vp.asn = vprop_asn
        eprop_type = gr_asn.new_edge_property("short")
        gr_asn.ep.type = eprop_type
        tprop = gr_asn.new_edge_property('int16_t')
        gr_asn.ep.source = tprop
        cprop = gr_asn.new_edge_property('int16_t')
        gr_asn.edge_properties["conf"] = cprop

    for edge in gr.edges_iter(data=True):
        src = int(edge[0])
        dst = int(edge[1])
        try:
            if src in asn_to_id:
                src_id = asn_to_id[src]
                new_src = gr_asn.vertex(src_id)
            else:
                new_src = gr_asn.add_vertex()
                src_id = int(new_src)
                gr_asn.vp.asn[new_src] = src
                asn_to_id[src] = int(new_src)
            if dst in asn_to_id:
                dst_id = asn_to_id[dst]
                print "dst_id", dst_id
                new_dst = gr_asn.vertex(dst_id)
            else:
                new_dst = gr_asn.add_vertex()
                dst_id = int(new_dst)
                gr_asn.vp.asn[new_dst] = dst
                asn_to_id[dst] = int(new_dst)
            if gr_asn.edge(src_id, dst_id):
                existing_edge = gr_asn.edge(src_id, dst_id)
                gr_asn.ep.conf[existing_edge] += 1
            else:
                new_edge = gr_asn.add_edge(new_src, new_dst)
                gr_asn.ep.conf[new_edge] = 1
                gr_asn.ep.source[new_edge] = BGP
        except OverflowError:
            continue
    all_graphs[asn] = (gr_asn, asn_to_id)

for asn, gr_tuple in all_graphs.iteritems():
    print asn
    try:
        gr = gr_tuple[0]
        if not gr: continue
        if not find_vertex(gr, gr.vp.asn, asn):
            print "No root node in this graph!, skipping"
            continue
        gr.save(settings.GRAPH_DIR_FINAL + '%s.gt' % asn)
    except:
        pass
