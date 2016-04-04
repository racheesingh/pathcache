#!/usr/bin/python
import logging
import settings
import random
import networkx as nx
import json
import ripe.atlas.sagan
import pycountry
from ripe.atlas.cousteau import ProbeRequest
import subprocess
import pdb
import fnss
import datetime
now = '-'.join( str( datetime.datetime.now() ).split() )

CAIDA_FILE = 'cipollino-verify/20160301.as-rel.txt'
topology = fnss.parse_caida_as_relationships( CAIDA_FILE )
G = nx.Graph()
G.add_nodes_from( topology.nodes() )
nodes = topology.nodes()

G.add_edges_from( topology.edges(data=True))

with open(settings.TOR_SRC_ASES_FILE) as fi:
    srcStr = fi.read()

src_ases = []
for line in srcStr.split('\n'):
    if line:
        _, asn, country_code = line.split()
        asn = asn.split('AS')[-1]
        src_ases.append(int(asn))

random.shuffle(src_ases)
with open( settings.TOR_ENTRIES_FILE%"" ) as fi:
    entriesStr = fi.read()

entry_ases = list( frozenset( [ int( x.split()[ 0 ] ) for x in entriesStr.split( '\n' ) if x ] ) )

random.shuffle(entry_ases)

with open( settings.TOR_EXITS_FILE%"" ) as fi:
    exitsStr = fi.read()
exit_ases = list( frozenset( [ int( int(x.split()[ 0 ] )) for x in exitsStr.split( '\n' ) if x ] ) )
random.shuffle(exit_ases)

with open(settings.TOR_DST_ASES_FILE) as fi:
    dstStr = fi.read()
dest_ases = []
for line in dstStr.split('\n'):
    if line:
        asn, _ = line.split()
        dest_ases.append(int(asn))
random.shuffle(dest_ases)

hijack_src_set = src_ases[:15] + entry_ases[:15] + exit_ases[:15] + dest_ases[:15]
hijack_target_set = src_ases[100:115] + entry_ases[100:115] + exit_ases[100:115] + \
                    dest_ases[100:115]

with open("cipollino-verify/bad_ases") as fi:
    badStr = fi.read()

bad_ases = []
for line in badStr.split('\n'):
    if line.startswith('ASN'): continue
    asn = line.split()[0]
    bad_ases.append(int(asn))

potential_hijacks = {}
for bad_as in bad_ases:
    print "Checking hijack capability of bad AS", bad_as
    total_attempt = 0
    hijacked_count = 0
    for src_as in hijack_src_set:
        for target_as in hijack_target_set:
            if src_as not in G.nodes() or target_as not in G.nodes(): continue
            sp1 = nx.shortest_path(G, source=src_as, target=target_as)
            if bad_as not in G.nodes(): continue
            total_attempt += 1
            sp2 = nx.shortest_path(G, source=src_as, target=bad_as)
            if len(sp1) > len(sp2):
                #potential_hijacks[bad_as].append((src_as, target_as))
                hijacked_count += 1
    try:
        potential_hijacks[bad_as] = float(hijacked_count)/float(total_attempt)
    except:
        pass

with open("cipollino-verify/hijack_potential-%s" % now, "w") as fi:
    json.dump(potential_hijacks, fi)

pdb.set_trace()
