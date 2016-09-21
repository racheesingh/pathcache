import json
import pdb
import sys

with open("%s" % sys.argv[1]) as fi:
    churn = json.load(fi)
    
print len(churn)

rel_churn = {}
for src_target, paths in churn.iteritems():
    if len(paths) >= 5:
        rel_churn[src_target] = paths
        
print "%d src-dst pairs had > 5 measurements" % len(rel_churn)

plens = {}
paths_diff = {}
for src_target, paths in rel_churn.iteritems():
    path_list =  paths.values()
    path_list_unique = []
    for pa in path_list:
        path_list_unique.append(tuple(pa))
    path_list_unique = list(set(path_list_unique))
    plens[src_target] = len(path_list_unique)
    if len(path_list_unique) > 1:
        paths_diff[src_target] = paths

with open("plens_%s" % sys.argv[1], "w") as fi:
    json.dump(plens, fi)
    
pdb.set_trace()

