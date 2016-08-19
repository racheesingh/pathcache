#!/usr/bin/python
import socket
import logging
import settings
import random
import networkx as nx
import json
import subprocess
import pdb
import fnss
import datetime
now = '-'.join( str( datetime.datetime.now() ).split() )

CAIDA_FILE = 'cipollino-verify/Cyclops_caida_cons.txt'

G = nx.Graph(directed=True)
with open(CAIDA_FILE) as fi:
    for line in fi:
        if line.startswith('#'): continue
        src, dst, typ = line.split()
        if typ == 'p2p':
            typ = 0
        else:
            assert typ == 'p2c'
            typ = -1
        src = int(src)
        dst = int(dst)
        G.add_edge(src, dst, type=typ)

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
#hijack_src_set = src_ases[:100]
#hijack_target_set = entry_ases[:100]

with open("cipollino-verify/bad_ases_feamster") as fi:
    badStr = fi.read()

bad_ases = []
for line in badStr.split('\n'):
    #if line.startswith('ASN'): continue
    asn = line.split()[0]
    bad_ases.append(int(asn))

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

targets_attempt = {}
targets_intercepted = {}
potential_interceptions = {}
for bad_as in bad_ases:
    print "Checking hijack capability of bad AS", bad_as
    total_attempt = 0
    hijacked_count = 0
    interception_count = 0
    for src_as in hijack_src_set:
        for target_as in hijack_target_set:
            if src_as not in G.nodes() or target_as not in G.nodes(): continue
            if bad_as not in G.nodes(): continue
            try:
                arr = get_paths_bgp_sim(['AS%d-AS%d' % (src_as, target_as), 'AS%d-AS%d' % (src_as, bad_as),
                                         "AS%d-AS%d" % (bad_as, target_as) ])
                src_target_path = arr[0].split(':')[1].strip().split('\n')
                src_target_path = [int(x) for x in src_target_path]
                src_hijacker_path = arr[1].split(':')[1].strip().split('\n')
                src_hijacker_path = [int(x) for x in src_hijacker_path]
                hijacker_target_path = arr[2].split(':')[1].strip().split('\n')
                hijacker_target_path = [int(x) for x in hijacker_target_path]
            except ValueError:
                continue
            
            total_attempt += 1
            if target_as not in targets_intercepted:
                targets_intercepted[target_as] = 0
            if target_as not in targets_attempt:
                targets_attempt[target_as] = 0
            targets_attempt[target_as] += 1
            if len(src_target_path) > len(src_hijacker_path):
                hijacked_count += 1
                if len(src_target_path) < 2 and len(src_hijacker_path) < 2:
                    print "Paths too small to work with", src_hijacker_path, src_target_path
                    targets_attempt[target_as] -= 1
                    hijacked_count -= 1
                    total_attempt -= 1
                    continue
                try:
                    if not G.has_edge(hijacker_target_path[0], hijacker_target_path[1]) \
                       and not G.has_edge(hijacker_target_path[1], hijacker_target_path[0]):
                        print "Could not find first edge in hijacker to target AS"
                        continue
                    if not G.has_edge(src_hijacker_path[-2], src_hijacker_path[-1]) \
                       and not G.has_edge(src_hijacker_path[-1], src_hijacker_path[-2]):
                        print "Could not find last edge in src to hijacker AS"
                        continue
                except IndexError:
                    pdb.set_trace()
                    continue
                
                if G.has_edge(hijacker_target_path[0], hijacker_target_path[1]):
                    hijacker_target_path_type = G.get_edge_data(hijacker_target_path[0],
                                                                hijacker_target_path[1])['type']
                    if hijacker_target_path_type == -1:
                        ptype_t = "customer"
                    elif hijacker_target_path_type == 0:
                        ptype_t = 'peer'
                elif G.has_edge(hijacker_target_path[1], hijacker_target_path[0]):
                    hijacker_target_path_type = G.get_edge_data(hijacker_target_path[1],
                                                                hijacker_target_path[0])['type']
                    if hijacker_target_path_type == -1:
                        ptype_t = "provider"
                    elif hijacker_target_path_type == 0:
                        ptype_t = "peer"
                
                if G.has_edge(src_hijacker_path[-2], src_hijacker_path[-1]):
                    src_hijacker_path_type = G.get_edge_data(src_hijacker_path[-2],
                                                             src_hijacker_path[-1])['type']
                    if src_hijacker_path_type == -1:
                        ptype_s = "provider"
                    elif src_hijacker_path_type == 0:
                        ptype_s = 'peer'
                elif G.has_edge(src_hijacker_path[-1], src_hijacker_path[-2]):
                    src_hijacker_path_type = G.get_edge_data(src_hijacker_path[-1],
                                                             src_hijacker_path[-2])['type']
                    if src_hijacker_path_type == -1:
                        ptype_s = "customer"
                    elif src_hijacker_path_type == 0:
                        ptype_s = "peer"

                if ptype_t == 'customer':
                    interception_count +=1
                    targets_intercepted[target_as] += 1
                elif ptype_t == 'peer':
                    if ptype_s == 'peer' or ptype_s == 'customer':
                        interception_count += 1
                        targets_intercepted[target_as] += 1
                else:
                    assert ptype_t == 'provider'
                    if ptype_s == 'customer':
                        interception_count += 1
                        targets_intercepted[target_as] += 1
    if total_attempt > 0:
        print hijacked_count, interception_count, total_attempt
        #potential_interceptions[bad_as] = (hijacked_count, interception_count, total_attempt)
        potential_interceptions[bad_as] = float(interception_count)/float(total_attempt)
    else:
        print "Bad AS not in the graph", bad_as
 
targets_intercepted_percent = {}
for asn in targets_intercepted:
    targets_intercepted_percent[asn] = round(
        float(targets_intercepted[asn])/targets_attempt[asn], 3)

with open("cipollino-verify/interception_potential_ex_%s" % now, "w") as fi:
    json.dump(potential_interceptions, fi)
with open("cipollino-verify/target_interception_ex_%s" % now, "w") as fi:
    json.dump(targets_intercepted_percent, fi)
