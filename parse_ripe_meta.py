#!/usr/bin/python
import bz2
import pdb
import settings
import json
import glob
msms = []

files = filter(os.path.isfile, glob.glob(settings.RIPE_META + "*"))
for fname in files:
    print fname
    with bz2.BZ2File(settings.RIPE_META + fname) as fi:
        for line in fi:
            mmt = json.loads(line)
            if mmt['type']['name'] != 'traceroute': continue
            msm_id = mmt['msm_id']
            msms.append(msm_id)
