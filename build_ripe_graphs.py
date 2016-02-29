#!/usr/bin/python
import radix
import os
import settings
from networkx.readwrite import json_graph
import sys
import traceback
import pickle
import pygeoip
import multiprocessing as mp
import networkx as nx
import urllib2
import json
import pdb
import urllib
import datetime
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from networkx import *
import logging

settings.GRAPH_DIR = settings.GRAPH_DIR_RIPE
now = '-'.join( str( datetime.datetime.now() ).split() )

logging.basicConfig( filename=settings.MMT_FETCH_LOG % now, level=logging.DEBUG )
orgs = {}
with open( settings.CAIDA_ORGS_FILE ) as f:
    for line in f:
        try:
            asn, org = line.split()
            orgs[ int( asn ) ] = org
        except:
            logging.warning( "Could not parse line in CAIDA Orgs file: " + line )

def are_siblings( as1, as2 ):
    print "Checking if siblings:", (as1, as2)
    print orgs[ as1 ], orgs[ as2 ]
    if as1 not in orgs or as2 not in orgs:
        return False
    return orgs[ as1 ] == orgs[ as2 ]
API_HOST = 'https://atlas.ripe.net'
API_MMT_URI = 'api/v1/measurement'
API_PRB_URI = '/api/v1/probe/'

ai = pygeoip.GeoIP( settings.MAXMIND_DB, pygeoip.MEMORY_CACHE)

IXPs = [ '1200',  '4635',  '5507', '6695', '7606', '8714', '9355', '9439', '9560',
         '9722', '9989', '11670', '17819', '18398', '21371', '24029', '24115',
         '24990', '35054', '40633', '42476', '43100', '47886', '48850', '55818' ]

timestamp  = int(time.time() - (60*60*24*60))
def fetch_json( offset=0 ):
    data = []
    api_args = dict( offset=offset, use_iso_time="true", \
                     start_time__gt="%s" % timestamp, type="traceroute" )

    #api_args = dict( offset=offset, use_iso_time="true", \
    #                 start_time__gt="%s" % timestamp, description__startswith="Cipollino", type="traceroute" )
    url = "%s/%s/?%s" % ( API_HOST, API_MMT_URI, urllib.urlencode( api_args ) )
    print url
    response = urllib2.urlopen( url )
    data = json.load( response )
    return data

def iptoasnglobal():
    rtree = radix.Radix()
    with open("routeviews-rv2-20160211-1200.pfx2as") as fi:
        for line in fi:
            ip, preflen, asn = line.split()
            if ',' in asn:
                tokens = asn.split(',')
                asn = tokens[0]
            if '_' in asn:
                tokens = asn.split('_')
                asn = tokens[0]
            rnode = rtree.add(network=ip, masklen=int(preflen))
            rnode.data["asn"] = asn
    return rtree
global_pref_tree = iptoasnglobal()
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
            msms.append( ( d[ 'msm_id' ], str( d[ 'dst_asn' ] ) ) )
        else:
            try:
                node = global_pref_tree.search_best(d['dst_addr'])
                asn = node.data['asn']
                msms.append( ( d[ 'msm_id' ], asn ) )
            except:
                logging.debug( "Cannot find destination AS for measurement " + str(d[ 'msm_id' ]) )
                pass
    count += 1

def compute_dest_based_graphs( msms ):
    def filter_cruft( data ):
        if 'result' in data:
            res = data['result']
            for hop_idx, hop in enumerate( res ):
                if 'result' in hop:
                    hop['result'] = [hr for hr in hop['result'] if 'edst' not in hr]
        return data
    
    def iptoasn():
        rtree = radix.Radix()
        with open("routeviews-rv2-20160211-1200.pfx2as") as fi:
            for line in fi:
                ip, preflen, asn = line.split()
                if ',' in asn:
                    tokens = asn.split(',')
                    asn = tokens[0]
                if '_' in asn:
                    tokens = asn.split('_')
                    asn = tokens[0]
                rnode = rtree.add(network=ip, masklen=int(preflen))
                rnode.data["asn"] = asn
        return rtree
    
    pref_tree = iptoasn()
    
    def remove_ixps( data, msm_id ):
        nodes = data['nodes']
        links = data['_links']
        # First node is an IXP (don't know how and why), cut that edge out
        if links[0]['src'] in IXPs:
            links = links[1:]
            nodes.remove(links[0]['src'])
        if set( nodes ).isdisjoint( set( IXPs ) ):
            return links

        logging.warning( "Found IXP in a traceroute. Msm ID: %d" % msm_id )
        ixps = list( set( IXPs ).intersection( set( nodes ) ) )
        new_links = []
        connecting_link = {}
        for link in links:
            if link['dst'] in ixps:
                assert 'src' not in connecting_link
                connecting_link['src'] = link['src']
            elif link['src'] in ixps:
                assert 'dst' not in connecting_link
                connecting_link['dst'] = link['dst']
            else:
                new_links.append(link)
            if 'src' in connecting_link and 'dst' in connecting_link:
                connecting_link['type'] = 'i'
                new_links.append(connecting_link)
                connecting_link = {}
        return new_links
    '''
    def get_graph_from_disk( asn ):
        # Get the most recent graph for asn on disk
        files = [x for x in os.listdir(settings.GRAPH_DIR) if
                 os.path.isfile(os.path.join(settings.GRAPH_DIR, x))]
        files = [os.path.join(settings.GRAPH_DIR, f) for f in files]
        # Sort files such that the most recent is the first in the list
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        graph = None
        for f in files:
            if asn + '-' in f:
                with open(f) as fi:
                    json_str = json.load(fi)
                graph = json_graph.node_link_graph(json_str)
        return graph
    '''
    def getlinktype( numASHop1, numASHop2 ):
        if numASHop2 - numASHop1 > 1:
            return 'i'
        return 'd'
    
    def traceroute_to_aspath(data, src_asn):
        aslinks = {'_nodes': set(), '_links': [] }
                    
        if 'result' in data:
            res = data['result']
            last_resp_hop_nr = None
            last_resp_hop_ases = set()
            last_to_last_resp_hop_nr = None
            last_to_last_asn = None
            this_hop_ases = None

            for hop in res:
                if 'hop' not in hop:
                    print "No hop info in", hop
                    
                if this_hop_ases and len(this_hop_ases) > 0:
                    last_resp_hop_ases = this_hop_ases
                    last_resp_hop_nr = this_resp_hop_nr
                    
                this_resp_hop_nr = hop['hop']
                ips = set()
                if 'result' in hop:
                    for hr in hop['result']:
                        if 'from' in hr:
                            if hr['from'] not in ips:
                                ips.add(hr['from'])
                this_hop_ases = set()
                for ip in ips:
                    node =  pref_tree.search_best(ip)
                    if node:
                        asn = node.data['asn']
                        this_hop_ases.add(asn)
                        
                if len(this_hop_ases) == 1 and len(last_resp_hop_ases) == 1:
                    this_asn = list(this_hop_ases)[0]
                    last_asn = list(last_resp_hop_ases)[0]
                    if this_asn != last_asn:
                        link_type = getlinktype(last_resp_hop_nr, this_resp_hop_nr)
                        link = { 'src': last_asn,
                                 'dst': this_asn, 'type': link_type }
                        aslinks['_nodes'].add( this_asn )
                        aslinks['_nodes'].add( last_asn )
                        aslinks['_links'].append( link )

                elif len(this_hop_ases) == 0 or len(last_resp_hop_ases) == 0:
                    pass #uninteresting
                else:
                    print "Uncaught situation at hop no %s->%s: %s->: %s" % \
                        ( last_resp_hop_nr, this_resp_hop_nr , last_resp_hop_ases, this_hop_ases )
                    continue
        aslinks['nodes'] = []
        for asn in aslinks['_nodes']:
            aslinks['nodes'].append( str(asn) )

        if not aslinks['_links']:
            return aslinks
        # Many times, the first hop address is a local (non-routable) prefix, so
        # prepending src_asn to the AS level path since we know for sure that the traceroute
        # originated from src_asn (good for coverage)
        if aslinks['_links'][0]['src'] != str(src_asn):
            aslinks['_links'] = [{'src':src_asn, 'dst':aslinks['_links'][0]['src'], 'type':'i'}] + \
                                aslinks['_links']

        # This code block short circuits paths like A->B->C->B->D to A->B->D
        # Also A->B->A->C->D should become A->C->D.
        linkssane = []
        delnext = False
        for index in range(len(aslinks['_links'])):
            if delnext:
                delnext = False
                continue
            if (index + 1) < len(aslinks['_links']):
                if aslinks['_links'][index]['src'] == aslinks['_links'][index+1]['dst']:
                    delnext = True
                else:
                    linkssane.append(aslinks['_links'][index])
            else:
                 linkssane.append(aslinks['_links'][index])
            
        loopdetect = []
        for link in linkssane:
            loopdetect.append(link['src'])
        loopdetect.append(link['dst'])
        loops = [i for i,x in enumerate(loopdetect) if loopdetect.count(x) > 1]
        if loops:
            # Cannot trust this traceroute, it has loops
            aslinks['_links'] = []
            return aslinks
        
        aslinks['_links'] = linkssane
        return aslinks

    with open("probe_to_asn_updated") as fi:
        probe_to_asn_map = json.load(fi)
    
    dest_based_graphs = {}
    for msm_tuple in msms:
        msm = msm_tuple[ 0 ]
        print "Evaluating MSM", msm
        dst_asn = msm_tuple[ 1 ]
        print "DST_ASN", dst_asn
        if dst_asn in dest_based_graphs:
            G = dest_based_graphs[dst_asn]
        else:
            G = nx.DiGraph()
            dest_based_graphs[ dst_asn ] = G
            
        url_for_msm = "%s/%s/%d/result/?%s" % \
                      (API_HOST, API_MMT_URI, msm, urllib.urlencode(dict({'format': 'txt'})))
        conn = urllib2.urlopen( url_for_msm )
        for dataStr in conn:
            data = json.loads(dataStr)    
            data = filter_cruft( data )
            # Source depends on each probe, so we need the probe's ASN
            if str(data['prb_id']) in probe_to_asn_map:
                src_asn = probe_to_asn_map[ str(data['prb_id']) ]
            else:
                node = pref_tree.search_best(data['from'])
                if node:
                    src_asn = node.data['asn']
                else:
                    logging.debug("Could not find src_asn for probe " + str(data['prb_id']) + \
                                  " " + data['from'])
                    continue
            traceroute_data = traceroute_to_aspath(data, src_asn)
            if not traceroute_data['_links']:
                continue
            links = remove_ixps(traceroute_data, msm)
            for link in links:
                if link['src'] == dst_asn:
                    break
                G.add_edge( link['src'], link['dst'], connection=link['type'],
                            source="atlas", mmt_id=msm,
                            ts=data['timestamp'], probe_id=data['prb_id'])
            if G.out_degree(dst_asn):
                logging.debug("OUT DEGREE OF ROOT IN AS %s IS %d" % \
                              (dst_asn, G.out_degree(dst_asn)))
                    
            # CANNOT ASSUME THIS STUFF ANY MORE
            #if links[-1]['dst'] != dst_asn:
            #    # The probe did not get a reply from the destination
            #    G.add_edge(links[-1]['dst'], dst_asn, connection='i', source="atlas",
            #                mmt_id=msm, ts=data['timestamp'], probe_id=data['prb_id'])
            #    if G.out_degree(dst_asn):
            #        logging.debug("OUT DEGREE OF ROOT IN AS %s IS %d" % \
            #                        (dst_asn, G.out_degree(dst_asn)))
            #        pdb.set_trace()
            #if not nx.is_weakly_connected(G):
            #    logging.debug("MULTIPLE CONNECTED COMPONENTS IN AS %s", dst_asn)
            #    pdb.set_trace()
            
    if not dest_based_graphs:
        logging.warning( "Destination based graph empty for these msms", msms )
    print dest_based_graphs
    return dest_based_graphs

def wrap_function( msms ):
    try:
        return compute_dest_based_graphs( msms )
    except:
        logging.warning( "".join(traceback.format_exception(*sys.exc_info())) )
    
logging.debug( "Number of measurements in the time frame: %d" % len(msms) )

num_msm_per_process = 5
num_chunks = len( msms )/num_msm_per_process + 1
pool = mp.Pool(processes=32)

msms_for_procs = []
results = []
for x in range( num_chunks ):
    start = x * num_msm_per_process
    end = start + num_msm_per_process
    if end > len( msms ) - 1:
        end = len( msms )
    print start, end
    msms_for_procs.extend( msms[ start: end ] )
    results.append( pool.apply_async( wrap_function, args=(msms[ start: end ],) ) )

pool.close()
pool.join()

assert msms_for_procs == msms
output = [ p.get() for p in results ]

print output

def combine_graphs(G, H):
    F = nx.compose(G,H)
    for e,v in F.edges():
        Gtags = []
        Htags = []
        if (e,v) in G.edges():
            Gtag = G[e][v]['trcrt']
            Gtags = Gtag.split(';')
        if (e,v) in H.edges():
            Htag = H[e][v]['trcrt']
            Htags = Htag.split(';')
        tags = ';'.join(list(set().union(Gtags, Htags)))
        F[e][v]['trcrt'] = tags
    return F

all_dst_based_graphs = {}
for res in output:
    print "evaluating result", res
    if res and not res.values():
        logging.warning( "No graph constructed for asn %s" % res.keys() )
    elif not res:
        continue
    for asn, gr in res.iteritems():
        if asn in all_dst_based_graphs and all_dst_based_graphs[asn] and gr:
            print "combining graphs for AS", asn
            #all_dst_based_graphs[ asn ] = combine_graphs(all_dst_based_graphs[ asn ], gr)
            all_dst_based_graphs[ asn ] = nx.compose(all_dst_based_graphs[ asn ], gr)
        elif gr and asn:
            all_dst_based_graphs[ asn ] = gr
            
print all_dst_based_graphs
for asn, gr in all_dst_based_graphs.iteritems():
    if not gr: continue
    try:
        data = json_graph.node_link_data( gr )
        s = json.dumps( data )
        with open( settings.GRAPH_DIR + '%s' % asn, "w" ) as f:
            f.write( s )
    except:
        pdb.set_trace()
