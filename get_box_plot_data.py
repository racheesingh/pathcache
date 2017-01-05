import json
import csv
import pdb
import sys

typ = sys.argv[1]
with open("top_content_coverage-%s.json" % typ) as fi:
    top_content_cov = json.load(fi)

csv_data = [["k","coverage", "pref"]]
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
        csv_data.append([i, len(cov), pref])
        print i, len(cov)

with open("top_content_cov_%s.csv" % typ, "w") as fi:
    writer = csv.writer(fi)
    writer.writerows(csv_data)

