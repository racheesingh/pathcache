import os
import json
import socket
import subprocess
import pdb
from subprocess import Popen
import time

asns = set()
with open("../asgraphs/data2/caida/20161201.as-rel.txt") as f:
    for line in f:
        if line.startswith('#'): continue
        src, dst, typ = line.split('|')
        asns.add(src)
        asns.add(dst)

asns = list(asns)
asns.sort()
def get_paths_bgp_sim(pairs):
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
    port = 11002
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

with open("top_asn") as fi:
    asn_srcs = json.load(fi)

all_paths = {}
#fi = open("allpaths_topasns_bgpsim", "a")

TCP_IP = '127.0.0.1'
TCP_PORT = 11002
BUFFER_SIZE = 10000000

asn_dsts = asns
fout = open('topasn_aspaths', 'w')
for asn_src in asn_srcs:
    if not asn_src: continue
    FNULL = open(os.devnull, 'w')
    proc = Popen(['mono',
                  '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/TestingApplication/bin/Release/TestingApplication.exe',
                  '-server11002 ', '../asgraphs/data2/caida/cyclops/20161201-cyclops',
                  '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/precomp/US-precomp367.txt',
                  '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/cache/exit_asns.txt' ],
                 stderr=subprocess.STDOUT)
    time.sleep(10)
    print "Getting all paths from", asn_src
    MESSAGE = asn_src + " -q"
    count = 0 
    for asn_dst in asn_dsts:
        MESSAGE += " " + asn_dst + " " + asn_src
        count += 1
    MESSAGE += " <EOFc> "
    print "Sending message to BGPSim to get %d paths from %s.." % (count, asn_src)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP, TCP_PORT))
    s.send(MESSAGE)
    data = ""
    result = dict()
    buffer_size = 10000000
    while True:
        d = s.recv(buffer_size)
        data += d
        if len(d) == 0:
            break
        if "<EOFs>" in d:
            break

    #data = s.recv(BUFFER_SIZE)
    s.close()
    fout.write(data + "\n")
    fout.flush()
    proc.terminate()
    '''
    as_pairs = []
    for asn_dst in asns:
        if asn_src == asn_dst: continue
        as_pairs.append('AS%d-AS%d' % (int(asn_src), int(asn_dst)))
    if not as_pairs:
        print "No candidate AS pairs"
        continue
    print "Total AS pairs", len(as_pairs)
    chunk_size  = len(as_pairs)/1000
    running = False
    count = 0
    for i in range(0, 1000):
        count += 1
        if count > 250:
            proc.terminate()
        if not running or count > 250:
            count = 0
            running = True
            FNULL = open(os.devnull, 'w')
            proc = Popen(['mono',
                          '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/TestingApplication/bin/Release/TestingApplication.exe',
                          '-server11002 ', '../asgraphs/data2/caida/cyclops/20161201-cyclops',
                          '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/precomp/US-precomp367.txt',
                          '../tor-traceroutes/astoria-v2/astoria-v2/bgp_sim/cache/exit_asns.txt' ],
                         stdout=FNULL, stderr=subprocess.STDOUT)
            time.sleep(10)

        as_pairs_chunk = as_pairs[i*chunk_size: chunk_size*(i+1)]
        if len(as_pairs_chunk) == 0: continue
        print "Querying BGPSim for %s paths" % len(as_pairs_chunk)
        paths = get_paths_bgp_sim(as_pairs_chunk)
        for path in paths:
            src_target_path = path.split(':')[1].strip().split('\n')
            if src_target_path == ['']: continue
            fi.write(' '.join([str(x) for x in src_target_path]) + "\n")
    '''
fout.close()
