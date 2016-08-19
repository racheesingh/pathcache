#!/usr/bin/python
import re
import json
import pdb

asgroups = {}
asgroups_by_group = {}
def initSiblings():
    with open('siblings', 'r') as fin:
        group = None
        arr = []
        for line in fin.readlines():
            if line.startswith("@"):
                if group != None:
                    for a in arr:
                        try:
                            asgroups[int(a)] = group
                        except ValueError:
                            continue
                            
                asgroups_by_group[group] = [int(x) for x in arr if x.isdigit()]
                group = line.strip()
                arr = []
            else:
                arr.append(line.strip())
initSiblings()

def str_match(reg, list_paths):
    if not reg:
        return False
    for path in list_paths:
        pstr = ''
        print path
        for hop in path:
            pstr += str(hop)
        match = reg.search(pstr)
        if match:
            return True
    return False

'''
with open("cipollino-verify/path_comp_accuracy_may26") as fi:
    path_comp = json.load(fi)
path_comp_sane = [x for x in path_comp if len(x[0]) >= 1]
path_comp = path_comp_sane
print "Evaluating against", len(path_comp), "paths"
top5 = 0
top4 = 0
top3 = 0
top2 = 0
top1 = 0


for pset in path_comp:
    pc_paths = pset[0]
    real_path = pset[-1]

    real_path_str = ''
    for hop in real_path:
        siblings  = []
        if hop in asgroups:
            siblings = asgroups_by_group[asgroups[hop]]
        if not siblings:
            real_path_str += str(hop)
        else:
            real_path_str += '('
            for sib in siblings:
                real_path_str +=  str(sib) + '|'
            if real_path_str[-1] == '|':
                real_path_str = real_path_str[:-1]
            real_path_str += ')'
    real_path_reg = re.compile(real_path_str)
    pdb.set_trace()
    if real_path in pc_paths or str_match(real_path_reg, pc_paths):
        top5 += 1
    
    if real_path in pc_paths[:4] or str_match(real_path_reg,pc_paths[:4]):
        top4 += 1
    if real_path in pc_paths[:3] or str_match(real_path_reg, pc_paths[:3]):
        top3 += 1
    if real_path in pc_paths[:2] or str_match(real_path_reg, pc_paths[:2]):
        top2 += 1
    if real_path in pc_paths[:1] or str_match(real_path_reg, pc_paths[:1]):
        top1 += 1
    
print top5, top4, top3, top2, top1, len(path_comp)
print "RES"
print float(top5)/len(path_comp),float(top4)/len(path_comp),float(top3)/len(path_comp),float(top2)/len(path_comp),float(top1)/len(path_comp)
'''

#with open("cipollino-verify/path_comp_accuracy_dict_june23_new2") as fi:
#    path_comp = json.load(fi)
#with open("cipollino-verify/path_comp_accuracy_dict_not_found_june23_new2") as fi:
#    not_found = json.load(fi)
with open("cipollino-verify/path_comp_accuracy_dict_june23_new_500") as fi:
    path_comp = json.load(fi)
with open("cipollino-verify/path_comp_accuracy_dict_not_found_june23_new_500") as fi:
    not_found = json.load(fi)


paths = {}
for pset in path_comp:
    tup = tuple(pset[0])
    real_path = pset[-1]
    pc_paths = pset[1]
    paths[tup] = [pc_paths, real_path]

cou1, cou2, cou3, cou4, cou5 = 0,0,0,0,0
print "Unique src, dst pairs", len(paths)
not_in_pc = 0
count = 0
for query, pset in paths.iteritems():
    print query
    pc_paths = pset[0]
    real_path = pset[-1]
    if len(pc_paths) < 1:
        continue
    assert real_path
    count += 1
    real_path_str = ''
    for hop in real_path:
        siblings  = []
        if hop in asgroups:
            siblings = asgroups_by_group[asgroups[hop]]
        if not siblings:
            real_path_str += str(hop)
        else:
            real_path_str += '('
            for sib in siblings:
                real_path_str +=  str(sib) + '|'
            if real_path_str[-1] == '|':
                real_path_str = real_path_str[:-1]
            real_path_str += ')'
    real_path_reg = re.compile(real_path_str)

    if real_path in pc_paths[:5] or str_match(real_path_reg,pc_paths[:5]):
        cou5 += 1
    else:
        print real_path, pc_paths
        #not_in_pc += 1
        if real_path in not_found:
            print real_path_str
            not_in_pc += 1
    if real_path in pc_paths[:4] or str_match(real_path_reg,pc_paths[:4]):
        cou4 += 1
    if real_path in pc_paths[:3] or str_match(real_path_reg,pc_paths[:3]):
        cou3 += 1
    if real_path in pc_paths[:2] or str_match(real_path_reg,pc_paths[:2]):
        cou2 += 1
    if real_path in pc_paths[:1] or str_match(real_path_reg,pc_paths[:1]):
        cou1 += 1
print "RES", count, len(paths)
print float(cou1)/count, float(cou2)/count, float(cou3)/count, float(cou4)/count, float(cou5)/count
print float(not_in_pc)/count
