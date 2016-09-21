#!/usr/bin/python
import bz2
import glob
import os
from graph_tool.all import *
import mkit.ripeatlas.probes as prb
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import settings
import sys
import traceback
import multiprocessing as mp
import urllib2
import json
import pdb
import urllib
import datetime
import time
import logging
import multiprocessing
logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.INFO)
logger.setLevel(multiprocessing.SUBDEBUG)

now = '-'.join(str(datetime.datetime.now() ).split())
API_HOST = "https://atlas.ripe.net:443"
API_MMT_URI = 'api/v2/measurements'
start_ts = int(time.mktime(datetime.datetime(2016, 7, 25).timetuple()))
end_ts = int(time.mktime(datetime.datetime(2016, 9, 19).timetuple()))
msms = []
files = filter(os.path.isfile, ['data/ripe/meta/meta-20160912.txt.bz2'])
for fname in files:
    print fname
    with bz2.BZ2File(fname) as fi:
        try:
            for line in fi:
                mmt = json.loads(line)
                if mmt['stop_time'] > end_ts: continue
                if mmt['stop_time'] < start_ts: continue
                if mmt['type']['name'] != 'traceroute': continue
                if mmt['af'] != 4 : continue
                msm_id = mmt['msm_id']
                msms.append(msm_id)
        except EOFError:
            continue

msms_all = list(frozenset(msms))
print len(msms_all)

'''
def fetch_json(timestamp_start, timestamp_end, page):
    data = []
    api_args = dict(start_time__gte="%s" % timestamp_start,
                    start_time__lte="%s" % timestamp_end, type="traceroute", af=4,
                    page_size=500, page=page)
    url = "%s/%s/?%s" % ( API_HOST, API_MMT_URI, urllib.urlencode( api_args ) )
    print url
    response = urllib2.urlopen( url )
    data = json.load( response )
    return data

msms_all = []
ts_start = int(time.mktime(datetime.datetime(2016, 9, 5).timetuple()))
ts_end = int(time.mktime(datetime.datetime(2016, 9, 19).timetuple()))
count = 1
while( 1 ):
    try:
        data = fetch_json(ts_start, ts_end, count)
    except urllib2.HTTPError:
        break
    if not data['results']:
        break
    for d in data['results']:
        msms_all.append(d['id'])
    count += 1

end_ts = 1474156800
start_ts = end_ts - (14*24*3600)
incr = 24*60*60
working_ts1 = start_ts
working_ts2 = start_ts + incr
msms_all = []
while working_ts2 < end_ts:
    print working_ts1, working_ts2
    res = fetch_json(working_ts1, working_ts2)
    for res_part in res['results']:
        msms_all.append(res_part['id'])
    working_ts1  = working_ts2
    working_ts2 = working_ts2 + incr
pdb.set_trace()
msms_all = list(set(msms_all))'''

print len(msms_all)

def compute_dest_based_graphs(msms):
    path_dict = {}
    for msm in msms:
        info = parse.mmt_info(msm)
        # Start time should not be < Jan 1st 2016
        #if info['start_time'] < 1451606400: continue
        if info['type']['af'] != 4: continue
        dst_asn = info['dst_asn']
        if not dst_asn:
            continue
        if not info['is_oneoff']:
            period  = int(info['interval'])
            if info['stop_time']:
                stop_time = int(info['stop_time'])
            else:
                stop_time = int(time.time())
            start = stop_time - 5 * period
            end = stop_time
            try:
                data = parse.parse_msm_trcrt(msm, start=start, end=end, count=500)
            except urllib2.HTTPError:
                continue
        else:
            data = parse.parse_msm_trcrt(msm)
        for d in data:
            src_asn = prb.get_probe_asn(d['prb_id'])
            stop_ts = d['endtime']
            if not src_asn:
                continue
            aslinks = asp.traceroute_to_aspath(d)
            if not aslinks['_links']: continue
            aslinks = ixp.remove_ixps(aslinks)
            path = []
            prev = None
            contiguous = True
            for link in aslinks:
                if prev: 
                    if not link['src'] == prev['dst']:
                        contiguous = False
                prev = link
            if not contiguous: continue
            for link in aslinks:
                if link['src'] == dst_asn:
                    break
                path.append(int(link['src']))
            path.append(int(link['dst']))
            if path[-1] != dst_asn:
                continue
            path = tuple(path)
            if "%s-%s" % (src_asn, dst_asn) in path_dict:
                path_dict["%s-%s" % (src_asn, dst_asn)][stop_ts] = path
            else:
                path_dict["%s-%s" % (src_asn, dst_asn)] = {stop_ts: path}

    print path_dict.keys()
    return path_dict

def wrap_function( msms ):
    try:
        return compute_dest_based_graphs( msms )
    except:
        logging.warning( "".join(traceback.format_exception(*sys.exc_info())) )
    
logging.debug( "Number of measurements in the time frame: %d" % len(msms) )

num_msm_per_process = 5
num_chunks = len( msms_all )/num_msm_per_process + 1
pool = mp.Pool(processes=45, maxtasksperchild=30)
results = []
for x in range(num_chunks):
    start = x * num_msm_per_process
    end = start + num_msm_per_process
    if end > len( msms_all ) - 1:
        end = len( msms_all )
    print start, end
    #wrap_function(msms_all[ start: end ])
    results.append(pool.apply_async(wrap_function, args=(msms_all[ start: end ],)))
    
pool.close()
pool.join()

output = [ p.get() for p in results ]
del results

all_path_dicts = {}
for res in output:
    print "evaluating result"
    if res and not res.values():
        logging.warning( "No graph constructed for asn %s" % res.keys() )
    elif not res:
        continue
    for tup, pdict in res.iteritems():
        if tup in all_path_dicts:
            for ts, path in pdict.iteritems():
                all_path_dicts[tup][ts] = path
        else:
            all_path_dicts[tup] = pdict

pdb.set_trace()
with open("churn_20160725_20160919", "w") as fi:
    json.dump(all_path_dicts, fi)
