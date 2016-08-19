#!/usr/bin/python
import os
import bz2
import pdb
import settings
import json
import glob
msms = []

#files = filter(os.path.isfile, glob.glob("data/ripe/meta/meta_for_gr/" + "*"))
files = ['data/ripe/meta/meta-20150116.txt.bz2', 'data/ripe/meta/meta-20150406.txt.bz2', 'data/ripe/meta/meta-20150706.txt.bz2', 'data/ripe/meta/meta-20151005.txt.bz2', 
         'data/ripe/meta/meta-20160104.txt.bz2', 'data/ripe/meta/meta-20160411.txt.bz2', 'data/ripe/meta/meta-20160502.txt.bz2']
len_msms = []
for fname in files:
    msms = []
    print fname
    with bz2.BZ2File(fname) as fi:
        try:
            for line in fi:
                mmt = json.loads(line)
                #if mmt['start_time'] < 1451606400: continue
                if mmt['type']['name'] != 'traceroute': continue
                msm_id = mmt['msm_id']
                msms.append(msm_id)
            msms = list(set(msms))
            print max(msms)
            len_msms.append(max(msms))
        except EOFError:
            continue

print len_msms
