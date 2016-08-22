#!/usr/bin/python
import socket

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

#hijack_src_set = src_ases[:10] + entry_ases[:10] + exit_ases[:10] + dest_ases[:10]
#hijack_target_set = src_ases[100:110] + entry_ases[100:110] + exit_ases[100:110] + \
#                    dest_ases[100:110]
hijack_src_set = dest_ases[:100]
hijack_target_set = exit_ases[:100]

def get_paths_bgp_sim( pairs):
    destinations = list()
    query_1, query_2 = "", "-q "
    for pair in pairs:
        src, dst = (pair.split("-")[0]).split("AS")[1], (pair.split("-")[1]).split("AS")[1]
        if src not in destinations:
            destinations.append(src)
            query_1 += src + " "
        if dst not in destinations:
            destinations.append(dst)
            query_1 += dst + " "
        query_2 += src + " " + dst + " "
    ip = '127.0.0.1'
    port = 11000
    buffer_size = 1000000
    query = query_1 + query_2 + "<EOFc>"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(query)
    data = ""
    result = dict()
    while True:
        d = s.recv(buffer_size)
        data += d
        if len(d) == 0:
            break
        if "<EOFs>" in d:
            break
    s.close()
    data = data.split("-\n<EOFs>")[0]
    arr = data.split("-\n")
    arr = data.split("-\n")
    return arr

with open("cipollino-verify/bad_ases_feamster") as fi:
    badStr = fi.read()

bad_ases = []
<<<<<<< HEAD
    #if line.startswith('ASN'): continue
    asn = line.split()[0]
    bad_ases.append(int(asn))

f = open("cipollino-verify/hijacker_db_en-%s" % now, "w")
potential_hijacks = {}
targets_hijacked = {}
target_attempt = {}

bad_ases_sane = []
for bad_as in bad_ases:
    if bad_as not in G.nodes(): continue
    bad_ases_sane.append(bad_as)

bad_ases = bad_ases_sane
print len(bad_ases)
for bad_as in bad_ases:
    print "Checking hijack capability of bad AS", bad_as
    total_attempt = 0
    hijacked_count = 0
    for src_as in hijack_src_set:
        for target_as in hijack_target_set:
            if src_as not in G.nodes() or target_as not in G.nodes(): continue
            if bad_as not in G.nodes(): continue
            try:
                arr = get_paths_bgp_sim(['AS%d-AS%d' % (src_as, target_as),
                                         'AS%d-AS%d' % (src_as, bad_as)])
                src_target_path = arr[0].split(':')[1].strip().split('\n')
                src_target_path = [int(x) for x in src_target_path]
                src_hijacker_path = arr[1].split(':')[1].strip().split('\n')
                src_hijacker_path = [int(x) for x in src_hijacker_path]
            except ValueError:
                continue
            total_attempt += 1
            if target_as not in targets_hijacked:
                targets_hijacked[target_as] = 0
            if target_as not in target_attempt:
                target_attempt[target_as] = 0
            target_attempt[target_as] += 1
            if len(src_target_path) > len(src_hijacker_path):
                targets_hijacked[target_as] += 1
                f.write("%s,%s,%s\n" % (src_as, target_as, bad_as))
                hijacked_count += 1
    try:
        print hijacked_count, total_attempt
        potential_hijacks[bad_as] = float(hijacked_count)/float(total_attempt)
    except:
        pass
f.close()

targets_hijacked_percent = {}
for asn in targets_hijacked:
    targets_hijacked_percent[asn] = round(
        float(targets_hijacked[asn])/float(target_attempt[asn]), 3)

with open("cipollino-verify/hijack_potential_ex", "w") as fi:
    json.dump(potential_hijacks, fi)
with open("cipollino-verify/target_hijacked_ex", "w") as fi:
    json.dump(targets_hijacked_percent, fi)
