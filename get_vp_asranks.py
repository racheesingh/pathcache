from mkit.ripeatlas import probes
import json
import random
import radix
import pdb
from netaddr import *
import getpass
user = getpass.getuser()
CAIDA_CUST_CONE = "/home/%s/data/20161201.ppdc-ases.txt" % user
PFX2ASN_DATA = "/home/%s/data/routeviews-rv2-20161225-1200.pfx2as" % user

customer_cones = {}
with open(CAIDA_CUST_CONE) as fi:
    for line in fi:
        if line.startswith('#'): continue
        asn_list = line.split()
        if len(asn_list) > 1:
            try:
                customer_cones[int(asn_list[0])] = len([int(x) for x in asn_list[1:]])
            except:
                continue

top_customer_cones = sorted(customer_cones.items(), key=lambda x: x[1], reverse=True)[:100]

top_customer_cone_ases = [x[0] for x in top_customer_cones]

asn_to_pref = {}
rtree_bgp = radix.Radix()
prefs = []
with open(PFX2ASN_DATA) as fi:
    for line in fi:
        ip, preflen, asn = line.split()
        if ',' in asn:
            tokens = asn.split(',')
            asn = tokens[0]
            print tokens[1:]
        if '_' in asn:
            tokens = asn.split('_')
            print tokens[1:]
            asn = tokens[0]
        rnode = rtree_bgp.add(network=ip, masklen=int(preflen))
        rnode.data["asn"] = asn
        if asn not in asn_to_pref:
            asn_to_pref[asn] = [IPNetwork("%s/%s" % (ip, preflen))]
        else:
            asn_to_pref[asn].append(IPNetwork("%s/%s" % (ip, preflen)))
        prefs.append(IPNetwork("%s/%s" % (ip, preflen)))

top_cust_cone_ips = {}
for asn in top_customer_cone_ases:
    try:
        ips = asn_to_pref[str(asn)]
        random.shuffle(ips)
        prefs.remove(ips[0])
        ips = list(ips[0])
        random.shuffle(ips)
        top_cust_cone_ips[asn] = str(ips[0])
    except KeyError:
        pass
    
f = open("aspop")
entries = list()
for table in f:
    records = table.split("[")
    for record in records:
        record = record.split("]")[0]
        entry = dict()
        try:
            entry["rank"] = record.split(",")[0]
            entry["as"] = record.split(",")[1].strip("\"")
            entry["country"] = ((record.split(",")[3]).split("=")[2]).split("\\")[0]
            entry["ip"] = None
            entries.append(entry)
        except IndexError:
            continue
f.close()
eyeballs = [(int(e["as"].split("AS")[-1]), e["rank"]) for e in entries]
top_eyeball_ips = {}
for asn, _ in eyeballs:
    try:
        ips = asn_to_pref[str(asn)]
        random.shuffle(ips)
        try:
            prefs.remove(ips[0])
        except ValueError:
            pdb.set_trace()
        ips = list(ips[0])
        random.shuffle(ips)
        top_eyeball_ips[asn] = str(ips[0])
        if len(top_eyeball_ips) >= 100: break
    except KeyError:
        pass

random.shuffle(prefs)
random_ips = {}
for pref in prefs:
    ips = list(pref)
    random.shuffle(ips)
    rnode = rtree_bgp.search_best(str(ips[0]))
    if not rnode: pdb.set_trace()
    asn = rnode.data["asn"]
    random_ips[asn] = str(ips[0])
    if len(random_ips) >= 100: break

with open("random_ips.json", "w") as fi:
    json.dump(random_ips, fi)
pdb.set_trace()
