import pdb
import radix
from ripe.atlas.sagan import helpers
import base64
import dns.message
from ripe.atlas.cousteau import AtlasResultsRequest
import urllib
import urllib2
import json
from datetime import datetime
from ripe.atlas.cousteau import (
    Dns,
    AtlasSource,
    AtlasCreateRequest
)
ATLAS_API_KEY= "5f35d4a7-b7d3-4ab8-b519-4fb18ebd2ead"
source = AtlasSource(type="area", value="WW", requested=80,
                     tags={"include":["system-ipv4-works", "system-resolves-a-correctly"]})
with open("urls_for_dns") as fi:
    urls = json.load(fi)
msms = []
print len(urls)

API_HOST = 'https://atlas.ripe.net'
API_MMT_URI = 'api/v1/measurement'

def fetch_json( offset=0 ):
    data = []
    timestamp  = int( (datetime.utcnow() - \
                       datetime( 1970, 1, 1 ) ).total_seconds() ) - ( 60 * 60 * 24 * 7)
    
    api_args = dict( offset=offset, use_iso_time="true",
                     description__startswith="Alexa top sites DNS lookup",
                     type="dns" )
    url = "%s/%s/?%s" % ( API_HOST, API_MMT_URI, urllib.urlencode( api_args ) )
    print url
    response = urllib2.urlopen( url )
    data = json.load( response )
    return data

with open("dns_msms.json") as fi:
    msms_old = json.load(fi)

dest_to_ips = {}
pref_tree = radix.Radix()
for dest in msms_old:
    kwargs = {"msm_id": msms_old[dest]}
    is_success, results = AtlasResultsRequest(**kwargs).create()
    if is_success:
        ip_addrs = []
        print len(results)
        for result in results:
            for subresult in result['resultset']:
                if 'result' not in subresult: continue
                abuf = subresult['result']['abuf']
                response = helpers.abuf.AbufParser.parse(base64.b64decode(abuf))
                if 'AnswerSection' not in response: continue
                for res in response['AnswerSection']:
                    if 'Address' in res:
                        ip_addr = res['Address']
                        octets = ip_addr.split('.')
                        octets[-1] = '0'
                        ip_addr = '.'.join(octets)
                        ip_addrs.append(ip_addr)
        dest_to_ips[dest] = list(set(ip_addrs))

with open("dest_pref.json", "w") as fi:
    json.dump(dest_to_ips, fi)
pdb.set_trace()

'''
pref_tree = radix.Radix()
all_counts_per_prefix = {}
all_counts_per_prefix
for dest in dest_to_ips:
    for ip in dest_to_ips[dest]:
        if pref_tree.search_best(ip):
            rnode = pref_tree.search_best(ip)
            all_counts_per_prefix[rnode.prefix] += 1
        else:
            pref_tree.add(network=ip, masklen=24)
            pref = pref_tree.search_best(ip).prefix
            all_counts_per_prefix[pref] = 1
all_counts_csv = [['prefix', 'count']]
for pref, count in all_counts_per_prefix.iteritems():
    all_counts_csv.append([pref, count])

with open("countent_prefix.csv", "w") as fi:
    writer = csv.writer(fi)
    writer.writerows(all_counts_csv)

pdb.set_trace()
'''
