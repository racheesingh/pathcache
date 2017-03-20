from Atlas import Measure
import itertools
from ripe.atlas.cousteau import ProbeRequest
from ripe.atlas.cousteau import (
    Ping,
    Traceroute,
    AtlasSource,
    AtlasCreateRequest
)
from datetime import datetime
import random
import csv
import json
import pdb
import sys
from mkit.ripeatlas import probes

dest_typ = sys.argv[1]
if len(sys.argv) > 2:
    api_key = sys.argv[2]
    Measure.KEY = api_key
else:
    api_key= "5f35d4a7-b7d3-4ab8-b519-4fb18ebd2ead"
    
with open("coverage/%s_mmts_to_run.json" % dest_typ) as fi:
    mmts_to_run = json.load(fi)

all_probes = probes.all_probes
probes_per_asn = {}
for pr in all_probes:
    if 'system-ipv4-works' in pr['tags'] and pr['status_name'] == 'Connected':
        asn = pr['asn_v4']
    if not asn: continue
    if asn in probes_per_asn:
        probes_per_asn[asn].append(pr)
    else:
        probes_per_asn[asn] = [pr]

def run_oneofftrace(src_probes, dst_prefix):
    if not src_probes:
        print "Send me a source probe, yo"
        return None
    print "Launching traceroute:", len(src_probes), dst_prefix
    try:
        msm_id = Measure.oneofftrace(
            src_probes, dst_prefix, af=4, paris=1,
            description="RS ALGO %s to %s" % (dest_typ, dst_prefix ))
        return msm_id
    except:
        return None

greedy_msms = {}
source_per_asn = {}
for pref, info in mmts_to_run.iteritems():
    print pref
    source_asns = info[0]
    if len(source_asns) > 500: pdb.set_trace()
    msms = []
    sources = []
    print len(source_asns)
    for source_asn in source_asns:
        if source_asn in source_per_asn:
            if source_per_asn[source_asn]:
                source = source_per_asn[source_asn]
            else:
                continue
        else:
            probes = ProbeRequest(return_objects=True, asn_v4='%s' % source_asn)
            try:
                probes.next()
                print "Got probe in", source_asn
                source = AtlasSource(type="asn", value="%s" % source_asn , requested=1)
                source_per_asn[source_asn] = source
            except StopIteration:
                print "No probe in", source_asn
                source_per_asn[source_asn] = None
                # Stop Iter in the first get, empty set of probes
                continue
        sources.append(source)
            
        # if int(source_asn) not in probes_per_asn:
        #     continue
        # probes = probes_per_asn[int(source_asn)]
        # pr = random.choice(probes)
        # source_probes.append(pr['id'])
        # while source_asns:
        #     source_asn = source_asns[0]
        #     print "Source ASN", source_asn
        #     probes = probes_per_asn[int(source_asn)]
        #     if not probes: pdb.set_trace()
        #     msm_id = run_oneofftrace(random.choice(probes), pref)
        #     if msm_id:
        #         msms.append(msm_id)
        #         source_asns.pop(0)
        #     random.shuffle(source_asns)

    print "Traceroute from %s sources" % len(sources)
    now = datetime.now().strftime("%b-%d-%Y")
    traceroute = Traceroute(
        af=4,
        target=pref,
        description="RSALGO %s %s traceroute to %s" % (now, dest_typ, pref),
        protocol="ICMP")
    is_success = False
    while not is_success:
        atlas_request = AtlasCreateRequest(
            start_time=datetime.utcnow(),
            key=api_key,
            measurements=[traceroute],
            sources=sources,
            is_oneoff=True)
        print "issuing measurement req to", pref
        (is_success, response) = atlas_request.create()
        print response
        if 'error' in response:
            if 'detail' in response['error'] and 'Invalid target' in response['error']['detail']:
                # Something is up with the measurement itself, dont wanna get hung up on this one
                # Moving on..
                is_success = True
    if 'measurements' in response:
        greedy_msms[pref] = response['measurements']
    # MEAS = False
    # while not MEAS:
    #     print "Measuring", pref, "from %d probes", len(source_probes)
    #     msm_id = run_oneofftrace(source_probes, pref)
    #     if 'error' not in msm_id
    #         MEAS = True
    #     if 'Invalid target' in msm_id:
    #         MEAS = True
    #         msm_id = "invalid"

with open("coverage/%s-msm_ids", "w") as fi:
    json.dump(greedy_msms, fi)

