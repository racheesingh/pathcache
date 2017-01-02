import pdb
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

count = 0
msms_old = {}
while(1):
    try:
        data = fetch_json(offset=count*100)
        count += 1
    except urllib2.HTTPError:
        break
    if not data[ 'objects' ]:
        break
    for d in data[ 'objects' ]:
        dest = d['description'].split('Alexa top sites DNS lookup')[-1]
        dest = dest.strip()
        msms_old[dest] = d['msm_id']

for url in urls:
    url = url.split('//')[1]
    url = url.split('www.')[-1]
    print url
    if 'xvideo' in url:
        print "I think this is inappropriate.. skipping"
        continue
    url = url.split(':')[0]
    print url
    if not url:
        continue
    if url in msms_old:
        print "Already measured", url
        continue
    dns = Dns(query_class='IN', description="Alexa top sites DNS lookup %s" % url, query_type='A',
              query_argument=url, af=4,
              use_probe_resolver=True,
              set_rd_bit=True)
    is_success = False
    while not is_success:
        atlas_request = AtlasCreateRequest(start_time=datetime.utcnow(),
                                           key=ATLAS_API_KEY,
                                           measurements=[dns],
                                           sources=[source],
                                           is_oneoff=True)
        (is_success, response) = atlas_request.create()
        if is_success:
            msms.append(response['measurements'][0])
        else:
            print "Failed Measurement to", url
            print response
        

with open("dns_msms", "w") as fi:
    json.dump(msms, fi)
pdb.set_trace()
