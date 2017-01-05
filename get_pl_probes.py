import sys, os, urllib, xmlrpclib, socket
from bs4 import BeautifulSoup
import pdb
import json
import  mkit.inference.ip_to_asn as ip2asn
# apiurl = 'https://www.planet-lab.org/PLCAPI/'
# api_server = xmlrpclib.ServerProxy(apiurl)

# auth = {}
# auth['Role'] = "user"
# auth['AuthMethod'] = "anonymous"
# all_nodes = api_server.GetNodes(auth)
# pl_probes_in_asns = []
# for node in all_nodes:
#     print node['hostname']
#     try:
#         ip_addr = socket.gethostbyname(node['hostname'])
#     except socket.gaierror:
#         print "Could not resolve, moving on"
#         continue
#     asn = ip2asn.ip2asn_bgp(ip_addr)
#     pl_probes_in_asns.append(asn)

# with open("pl_probe_asns.json", "w") as fi:
#     json.dump(pl_probes_in_asns, fi)
    
with open("caida_ark") as fi:
    html = fi.read()
soup = BeautifulSoup(html)
table = soup.find("table", id="html_monitor_table")
ark_asns = []
for row in table.find_all("tr"):
    try:
        tds = row.find_all("td")
        if not tds: continue
        ark_asns.append([td.get_text() for td in tds][-3])
    except:
        pdb.set_trace()

ark_asns = list(set(ark_asns))
with open("ark_probe_asns.json", "w") as fi:
    json.dump(ark_asns, fi)
