#!/usr/bin/python
import mkit.inference.ixp as ixp
from networkx.readwrite import json_graph
import networkx as nx
from collections import defaultdict
from itertools import groupby
import time
import json
import pdb
from _pybgpstream import BGPStream, BGPRecord, BGPElem
import settings
bgp_graphs = {}

def build_bgp_graph(project=None, collector=None):
    print project, collector
    global bgp_graphs
    stream = BGPStream()
    rec = BGPRecord()

    if collector:
        stream.add_filter('collector', collector)
    else:
        stream.add_filter('project', project)
                      
    # Consider RIBs dumps only
    stream.add_filter('record-type','ribs')
    
    # Consider this time interval:
    #cur_time = int( time.time() )
    cur_time  = 1471219200
    if collector and 'views' in collector:
        interval = 3
    else:
        interval = 10
        
    #prev_time = int(cur_time - (60 * 60 * interval))
    prev_time = 1471132800
    print "Starting stream with %d interval" % interval
    stream.add_interval_filter(prev_time, cur_time)
    stream.start()
    ribEntryCount = 0
    loopCount = 0
    while(stream.get_next_record(rec)):
        elem = rec.get_next_elem()
        while(elem):
            pdb.set_trace()
            ribEntryCount += 1
            peer = str(elem.peer_asn)
            hops = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            loops = [i for i, x in enumerate(hops) if hops.count(x) > 1]
            if loops:
                # print( "Routing loop! AS path %s" % elem.fields['as-path'] )
                loopCount += 1
                elem = rec.get_next_elem()
                continue
            if '{' in elem.fields['as-path']:
                elem = rec.get_next_elem()
                continue
            if len(hops) > 1 and hops[0] == peer:
                # Get the origin ASN, is the destination for traffic
                # toward this prefix
                origin = hops[-1]
                if origin in ixp.IXPs:
                    print "Origin", origin, "is an IXP, it announced prefix", elem.fields['prefix']
                    elem = rec.get_next_elem()
                    continue
                if origin in bgp_graphs:
                    as_graph = bgp_graphs[ origin ]
                else:
                    as_graph = nx.DiGraph()
                    bgp_graphs[ origin ] = as_graph
                # Add new edges to the NetworkX graph
                new_hops = []
                for hop in hops:
                    if hop in ixp.IXPs:
                        continue
                    new_hops.append(hop)
                if len(new_hops) <= 1:
                    elem = rec.get_next_elem()
                    continue
                for i in range(0,len(new_hops)-1):
                    as_graph.add_edge(new_hops[i],new_hops[i+1])
                # Making sure, root has 0 out degree    
                #if as_graph.out_degree( origin ) != 0:
                #    pdb.set_trace()
            elem = rec.get_next_elem()
    print "Total RIB entries parsed", ribEntryCount, "Number of AS paths with loops", loopCount

print "Getting RIB from all RIS"
build_bgp_graph(project='ris')
print len( bgp_graphs.keys() )
print "Getting RIB from route-views.linx"
build_bgp_graph(collector='route-views.linx')
print len( bgp_graphs.keys() )
print "Getting RIB from route-views.sydney"
build_bgp_graph(collector='route-views.sydney')
print len( bgp_graphs.keys() )
print "Getting RIB from route-views.saopaulo"
build_bgp_graph(collector='route-views.saopaulo')
print len( bgp_graphs.keys() )
print "Getting RIB from route-views.wide"
build_bgp_graph(collector='route-views.wide')
print len( bgp_graphs.keys() )
print "Getting RIB from route-views.route-views2"
build_bgp_graph(collector='route-views2')
print len( bgp_graphs.keys() )

for asn, gr in bgp_graphs.iteritems():
    if not gr: continue
    try:
        data = json_graph.node_link_data( gr )
        s = json.dumps( data )
        with open( settings.GRAPH_DIR_BGP + '%s' % asn, "w" ) as f:
            f.write( s )
    except:
        pdb.set_trace()
