#!/usr/bin/python
import mkit.inference.ip_to_asn as ip2asn
import socket
import socket
import alexa
import json
from networkx.readwrite import json_graph
import networkx as nx
import pdb
from graph_tool.all import *
import os
import settings
EYEBALL_THRES = 500
files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
          if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]

all_graphs = {}

for f in files:
    asn = f.split( '/' )[ -1 ].split('.')[0]
    print "COMBINED graph for ASN", asn
    gr = load_graph(f, fmt="gt")
    remove_parallel_edges(gr)
    remove_self_loops(gr)
    overall_origins = {}
    all_graphs[int(asn)] = gr

def path_in_cache( src, dst ):
    if dst in all_graphs:
        gr = all_graphs[dst]
        src = find_vertex(gr, gr.vp.asn, src)
        if src:
            return True
    return False

src_ases_by_country = {}
top_eyeballs = []

f = open("data/aspop")
entries = list()
for table in f:
    records = table.split("[")
    for record in records:
        record = record.split("]")[0]
        entry = dict()
        try:
            entry["rank"] = int(record.split(",")[0])
            entry["as"] = record.split(",")[1].strip("\"")
            entry["country"] = ((record.split(",")[3]).split("=")[2]).split("\\")[0]
            entry["users"] = int(record.split(",")[4])
            if entry["rank"] > 5000:
                continue
            entries.append(entry)
        except (IndexError, ValueError):
            continue
f.close()

for entry in entries:
    if len(top_eyeballs) < EYEBALL_THRES:
        top_eyeballs.append(int(entry['as'].split('AS')[-1]))
    if entry['country'] not in src_ases_by_country:
        src_ases_by_country[entry['country']] = []
    if len(src_ases_by_country[entry['country']]) >= 20:
        continue
    src_ases_by_country[entry['country']].append(int(entry['as'].split('AS')[-1]))
pdb.set_trace()
#count = 0
#for asn in top_eyeballs:
#    num_probes = get_probes_in_asn(asn)
#    if len(num_probes) > 0:
#        count += 1
#print count
#rank_user = {}
#for entry in entries:
#    rank = entry["rank"]
#    users = entry["users"]
#    rank_user[rank] = users
#ranks = rank_user.keys()
#num_users = []
#for rank in ranks:
#    num_users.append(rank_user[rank])
#with open("cipollino-verify/geoff_ranks", "w") as fi:
#    json.dump(ranks, fi)
#with open("cipollino-verify/geoff_num_users", "w") as fi:
#    json.dump(num_users, fi)


CONTENT_THRES = 10    
websites = alexa.top_list(CONTENT_THRES)
content_asns = []
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
    except:
        print w
        continue

#src_ases_by_country_new = {}
#for cc in src_ases_by_country:
#    if len(src_ases_by_country[cc]) > 10:
#        src_ases_by_country_new[cc] = src_ases_by_country[cc]
#src_ases_by_country = src_ases_by_country_new

print "getting overall coverage"
forward_count_overall = 0
rev_count_overall = 0
for src_eyeball in top_eyeballs:
    for dst_content in content_asns:
        if path_in_cache(src_eyeball, dst_content):
            forward_count_overall += 1
print forward_count_overall
print forward_count_overall*1.0/(len(top_eyeballs)*len(content_asns))

country_coverage = {}
for code, eyeballs in src_ases_by_country.iteritems():
    eyeballs = eyeballs[:EYEBALL_THRES]
    forward_count = 0
    for src in eyeballs:
        for dst in content_asns:
            if path_in_cache(src, dst):
                forward_count += 1
    for_fraction = forward_count*1.0/(len(eyeballs)*len(content_asns))
    country_coverage[code] = for_fraction

with open("cipollino-verify/pc_coverage_country_all_alexa_fw_only_%s" % CONTENT_THRES, "w") as fi:
    json.dump(country_coverage, fi)

CONTENT_THRES = 20    
websites = alexa.top_list(CONTENT_THRES)
content_asns = []
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
    except:
        print w
        continue

print "getting overall coverage"
forward_count_overall = 0
rev_count_overall = 0
for src_eyeball in top_eyeballs:
    for dst_content in content_asns:
        if path_in_cache(src_eyeball, dst_content):
            forward_count_overall += 1
print forward_count_overall
print forward_count_overall*1.0/(len(top_eyeballs)*len(content_asns))

country_coverage = {}
for code, eyeballs in src_ases_by_country.iteritems():
    eyeballs = eyeballs[:EYEBALL_THRES]
    forward_count = 0
    for src in eyeballs:
        for dst in content_asns:
            if path_in_cache(src, dst):
                forward_count += 1
    for_fraction = forward_count*1.0/(len(eyeballs)*len(content_asns))
    country_coverage[code] = for_fraction

with open("cipollino-verify/pc_coverage_country_all_alexa_fw_only_%s" % CONTENT_THRES, "w") as fi:
    json.dump(country_coverage, fi)

CONTENT_THRES = 30    
websites = alexa.top_list(CONTENT_THRES)
content_asns = []
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
    except:
        print w
        continue

print "getting overall coverage"
forward_count_overall = 0
rev_count_overall = 0
for src_eyeball in top_eyeballs:
    for dst_content in content_asns:
        if path_in_cache(src_eyeball, dst_content):
            forward_count_overall += 1
print forward_count_overall
print forward_count_overall*1.0/(len(top_eyeballs)*len(content_asns))

country_coverage = {}
for code, eyeballs in src_ases_by_country.iteritems():
    eyeballs = eyeballs[:EYEBALL_THRES]
    forward_count = 0
    for src in eyeballs:
        for dst in content_asns:
            if path_in_cache(src, dst):
                forward_count += 1
    for_fraction = forward_count*1.0/(len(eyeballs)*len(content_asns))
    country_coverage[code] = for_fraction

with open("cipollino-verify/pc_coverage_country_all_alexa_fw_only_%s" % CONTENT_THRES, "w") as fi:
    json.dump(country_coverage, fi)


CONTENT_THRES = 40    
websites = alexa.top_list(CONTENT_THRES)
content_asns = []
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
    except:
        print w
        continue

print "getting overall coverage"
forward_count_overall = 0
rev_count_overall = 0
for src_eyeball in top_eyeballs:
    for dst_content in content_asns:
        if path_in_cache(src_eyeball, dst_content):
            forward_count_overall += 1
print forward_count_overall
print CONTENT_THRES, forward_count_overall*1.0/(len(top_eyeballs)*len(content_asns))

country_coverage = {}
for code, eyeballs in src_ases_by_country.iteritems():

    eyeballs = eyeballs[:EYEBALL_THRES]
    forward_count = 0
    for src in eyeballs:
        for dst in content_asns:
            if path_in_cache(src, dst):
                forward_count += 1
    for_fraction = forward_count*1.0/(len(eyeballs)*len(content_asns))
    country_coverage[code] = for_fraction

with open("cipollino-verify/pc_coverage_country_all_alexa_fw_only_%s" % CONTENT_THRES, "w") as fi:
    json.dump(country_coverage, fi)

CONTENT_THRES = 50    
websites = alexa.top_list(CONTENT_THRES)
content_asns = []
for w in websites:
    try:
        asn = int(ip2asn.ip2asn_bgp(socket.gethostbyname(w[1])))
        content_asns.append(asn)
    except:
        print w
        continue

print "getting overall coverage"
forward_count_overall = 0
rev_count_overall = 0
for src_eyeball in top_eyeballs:
    for dst_content in content_asns:
        if path_in_cache(src_eyeball, dst_content):
            forward_count_overall += 1
print forward_count_overall
print CONTENT_THRES, forward_count_overall*1.0/(len(top_eyeballs)*len(content_asns))

country_coverage = {}
for code, eyeballs in src_ases_by_country.iteritems():
    eyeballs = eyeballs[:EYEBALL_THRES]
    forward_count = 0
    for src in eyeballs:
        for dst in content_asns:
            if path_in_cache(src, dst):
                forward_count += 1
    for_fraction = forward_count*1.0/(len(eyeballs)*len(content_asns))
    country_coverage[code] = for_fraction

with open("cipollino-verify/pc_coverage_country_all_alexa_fw_only_%s" % CONTENT_THRES, "w") as fi:
    json.dump(country_coverage, fi)

pdb.set_trace()
