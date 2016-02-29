#!/usr/bin/python
from networkx.readwrite import json_graph
import json
import sys
import traceback
from itertools import groupby
import multiprocessing as mp
import networkx as nx
import pdb
import pygeoip
import re
import os
import settings
from datetime import datetime, timedelta

print "To ensure correct usage, place the extracted Iplane dumps (get from here: http://iplane.cs.washington.edu/data/today/traces_2016_02_27.tar.gz) in ~/data/iplane/. Place the readout file (http://iplane.cs.washington.edu/data/readoutfile) in ~/data/iplane/ and then the APIs will be able to find the data."

dir_files = {}
for date in [dates]:
    dirName = "traces_" + date 

    dir_path = os.path.join(settings.IPLANE_DATA, dirName)
    files = [x for x in os.listdir(dir_path) if
             os.path.isfile(os.path.join(dir_path, x))]
    files = [os.path.join(dir_path, f) for f in files]
    dir_files[ dirName ] = files
    
def parse_iplane_file(dirName, fName):
    print "Parsing file", fName
    ai = pygeoip.GeoIP( settings.MAXMIND_DB, pygeoip.MEMORY_CACHE)
    as_paths_dict = {}
    aspath = []
    current_dest = None
    ipRegex = r"((([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])[ (\[]?(\.|dot)[ )\]]?){3}([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5]))"
    parseCommand = "./%s %s > %s" % (settings.readOutExec, fName, fName+"-read")
    os.system(parseCommand)
    with open(fName+"-read") as fi:
        for line in fi:
            if 'destination' in line:
                match = re.search(ipRegex, line)
                if match:
                    # Add previous AS path to dictionary
                    if current_dest and aspath and current_dest in as_paths_dict:
                        aspath = [k for k,g in groupby(aspath)]
                        as_paths_dict[current_dest].append(aspath)
                    elif current_dest and aspath:
                        aspath = [k for k,g in groupby(aspath)]
                        as_paths_dict[current_dest] = [aspath]
                    dest = match.group(0)
                    asStr = ai.asn_by_addr( dest )
                    if asStr:
                        asn = str(asStr.split()[0])
                        aspath = []
                        current_dest = asn
            else:
                match = re.search(ipRegex, line)
                if match:
                    hop = match.group(0)
                    asStr = ai.asn_by_addr( hop )
                    if asStr:
                        asn = str(asStr.split()[0])
                        aspath = aspath + [asn]
                    
    return as_paths_dict

def wrap_function(dirName, fName):
    try:
        return parse_iplane_file(dirName, fName)
    except:
        print( "".join(traceback.format_exception(*sys.exc_info())) )

results = []
pool = mp.Pool(processes=32)
for dName, files in dir_files.iteritems():
    for f in files:
        results.append( pool.apply_async( wrap_function, args=(dName,f) ) )

pool.close()
pool.join()

output = [ p.get() for p in results ]

print len(output)

dest_based_as_paths = {}
for op in output:
    for dst_asn, aspaths in op.iteritems():
        if not dst_asn in dest_based_as_paths:
            dest_based_as_paths[dst_asn] = aspaths
        else:
            dest_based_as_paths[dst_asn].extend(aspaths)

print len(dest_based_as_paths.keys())

dest_based_graphs = {}
for dst, aspaths in dest_based_as_paths.iteritems():
    G = nx.DiGraph()
    for aspath in aspaths:
        for first, second in zip(aspath, aspath[1:]):
            G.add_edge(first, second)
    dest_based_graphs[dst] = G

for asn, gr in dest_based_graphs.iteritems():
    if not gr: continue
    try:
        data = json_graph.node_link_data( gr )
        s = json.dumps( data )
        with open( settings.GRAPH_DIR_IPLANE + 'AS%s' % asn, "w" ) as f:
            f.write( s )
    except:
        pdb.set_trace()
