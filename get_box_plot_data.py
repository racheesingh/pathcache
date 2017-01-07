import json
import csv
import pdb
import sys

typ = sys.argv[1]
dest_typ = sys.argv[2]
with open("coverage/%s_coverage-%s-random-nosh.json" % (dest_typ, typ)) as fi:
    top_content_cov = json.load(fi)

csv_data = [["k","coverage", "strat"]]
for pref, info in top_content_cov.iteritems():
    info = info[0]
    assert len(info) == 2
    mmts = info[0]
    coverage = info[1]
    assert len(mmts) == len(coverage)
    print pref
    for i in range(1, len(coverage)):
        cov = []
        for j in range(0, i):
            cov.extend(coverage[j])
        cov = set(cov)
        csv_data.append([i, len(cov), "Random"])
        print i, len(cov)

with open("coverage/%s_coverage-%s-greedy-nosh.json" % (dest_typ, typ)) as fi:
    top_content_cov = json.load(fi)

for pref, info in top_content_cov.iteritems():
    info = info[0]
    assert len(info) == 2
    mmts = info[0]
    coverage = info[1]
    assert len(mmts) == len(coverage)
    print pref
    for i in range(1, len(coverage)):
        cov = []
        for j in range(0, i):
            cov.extend(coverage[j])
        cov = set(cov)
        csv_data.append([i, len(cov), "Greedy"])
        print i, len(cov)

with open("coverage/%s_coverage-%s-geod-nosh.json" % (dest_typ, typ)) as fi:
    top_content_cov = json.load(fi)

for pref, info in top_content_cov.iteritems():
    info = info[0]
    assert len(info) == 2
    mmts = info[0]
    coverage = info[1]
    assert len(mmts) == len(coverage)
    print pref
    for i in range(1, len(coverage)):
        cov = []
        for j in range(0, i):
            cov.extend(coverage[j])
        cov = set(cov)
        csv_data.append([i, len(cov), "Geo-distributed"])
        print i, len(cov)

with open("coverage/%s_cov_%s_all_strategies-nosh.csv" % (dest_typ, typ), "w") as fi:
    writer = csv.writer(fi)
    writer.writerows(csv_data)

