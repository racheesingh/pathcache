#!/usr/bin/python
import networkx as nx
from networkx.readwrite import json_graph
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import os
import pdb
import settings
import json
import glob
msms = []

def parse_caida_json_streaming(fname):
    mmt_path_list = []
    with open(fname) as fi:
        dest_based_aspaths = {}
        for line in fi:
            trcrt = json.loads(line)
            if trcrt['stop_reason'] != 'COMPLETED':
                continue
            src = trcrt['src']
            dst = trcrt['dst']
            dst_asn = ip2asn.ip2asn_bgp(dst)
            src_asn = ip2asn.ip2asn_bgp(src)
            if src_asn and dst_asn:
                mmt_path_list.append((int(src_asn), int(dst_asn)))
    return mmt_path_list

overall_path_list = []
files = filter(os.path.isfile, glob.glob(settings.CAIDA_DATA + "*"))
for fname in files:
    print "Converting %s to JSON" % fname
    convert_to_json_cmd = "sc_warts2json %s > %s" % (fname, fname+".json")
    os.system(convert_to_json_cmd)
    overall_path_list.extend(parse_caida_json_streaming(fname+'.json'))
    print "Removing the JSON file to save space"
    os.system("rm %s" % fname+".json")

overall_path_list = list(frozenset(overall_path_list))
with open(settings.MEASURED_CAIDA, "w") as fi:
    json.dump(overall_path_list, fi)
