#!/usr/bin/python
import pdb
import multiprocessing as mp
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import json
import settings

with open(settings.RIPE_MSMS) as fi:
    msms = json.load(fi)
msms_all = list(frozenset(msms))

def compute_dest_based_graphs(msms):
    measured_paths = []
    for msm in msms:
        info = parse.mmt_info(msm)
        # Start time should not be < Jan 1st 2016
        if info['start_time'] < 1451606400: continue
        if info['type']['af'] != 4: continue
        dst_asn = int(info['dst_asn'])
        data = parse.parse_msm_trcrt(msm, count=1000)
        for d in data:
            src_asn = ip2asn.ip2asn_bgp(d['from'])
            if not src_asn: continue
            measured_paths.append((int(src_asn), dst_asn))
    return list(frozenset(measured_paths))

#msms_all = msms_all[:20]
num_msm_per_process = 5
num_chunks = len( msms_all )/num_msm_per_process + 1
pool = mp.Pool(processes=24, maxtasksperchild=100)
results = []
for x in range( num_chunks ):
    start = x * num_msm_per_process
    end = start + num_msm_per_process
    if end > len( msms_all ) - 1:
        end = len( msms_all )
    print start, end
    results.append(pool.apply_async(compute_dest_based_graphs, args=(msms_all[ start: end ],)))
    #results.append(compute_dest_based_graphs(msms_all[start:end]))

pool.close()
pool.join()

all_path_list = set()
for p in results:
    try:
        all_path_list = all_path_list.union(set(p.get()))
    except:
        pass

all_path_list = list(all_path_list)
with open(settings.MEASURED_RIPE, "w") as fi:
    json.dump(all_path_list, fi)
