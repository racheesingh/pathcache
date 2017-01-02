from mkit.inference import ip_to_asn as ip2asn
import json
import csv
import pdb

with open("dest_pref.json") as fi:
    dest_pref = json.load(fi)
per_pref_count = {}
per_asn_count = {}
for dest, pref_list in dest_pref.iteritems():
    for pref in pref_list:
        if pref in per_pref_count:
            per_pref_count[pref] += 1
        else:
            per_pref_count[pref] = 1
        asn = ip2asn.ip2asn_bgp(pref)
        if asn in per_asn_count:
            per_asn_count[asn] += 1
        else:
            per_asn_count[asn] = 1

per_pref_count_sorted = sorted(per_pref_count.items(), key=lambda x: x[1], reverse=True)
csv_list = [['pref', 'count']]
for pref, count in per_pref_count_sorted:
    csv_list.append([pref, count])
pdb.set_trace()
with open("per_prefix_count.csv", "w") as fi:
    writer = csv.writer(fi)
    writer.writerows(csv_list)

per_asn_count_sorted = sorted(per_asn_count.items(), key=lambda x: x[1], reverse=True)
csv_list = [['asn', 'count']]
for asn, count in per_asn_count_sorted:
    csv_list.append([asn, count])

with open("per_asn_count.csv", "w") as fi:
    writer = csv.writer(fi)
    writer.writerows(csv_list)

pdb.set_trace()
