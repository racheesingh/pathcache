#!/usr/bin/python
import os
import bz2
import pdb
import settings
import json
import glob
msms = []

files = filter(os.path.isfile, ['data/ripe/meta/meta_for_gr/meta-20160808.txt.bz2',
                                'data/ripe/meta/meta_for_gr/meta-20160815.txt.bz2'])
for fname in files:
    print fname
    with bz2.BZ2File(fname) as fi:
        try:
            for line in fi:
                mmt = json.loads(line)
                if mmt['start_time'] < 1451606400: continue
                if mmt['type']['name'] != 'traceroute': continue
                msm_id = mmt['msm_id']
                msms.append(msm_id)
        except EOFError:
            continue

msms = list(frozenset(msms))
print len(msms)
with open("data/ripe/msms-useful", "w") as fi:
    json.dump(msms, fi)
