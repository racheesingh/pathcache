#!/usr/bin/python
import mkit.iplane.parse as iparse
import networkx as nx
from networkx.readwrite import json_graph
import json
import settings
import pdb

dest_aspaths = iparse.get_iplane_graphs('2016_03_11')

path_list = []
for dst, aspaths in dest_aspaths.iteritems():
    dst_asn = int(dst)
    for aspath in aspaths:
        if int(aspath[-1]) == dst_asn:
            path_list.append((int(aspath[0]), dst_asn))

path_list = list(frozenset(path_list))
with open(settings.MEASURED_IPLANE, "w") as fi:
    json.dump(path_list, fi)
            
