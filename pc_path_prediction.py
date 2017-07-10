from pebble import concurrent
from pebble import ProcessPool
from pebble import ThreadPool
import random
import gc
import pylibmc
import traceback
import signal
from operator import mul
import graph_tool.topology
import radix
import multiprocessing as mp
import socket
import time
from subprocess import Popen
import graph_tool.topology
from mkit.ripeatlas import probes
import os
from graph_tool.all import *
import mkit.ripeatlas.probes as prb
import mkit.inference.ip_to_asn as ip2asn
import mkit.inference.ixp as ixp
import mkit.ripeatlas.parse as parse
import mkit.inference.ippath_to_aspath as asp
import urllib
import urllib2
import itertools
from datetime import datetime
import random
import csv
import json
import pdb
import sys
from mkit.ripeatlas import probes
import subprocess
import pdb
error_lookups = []
query_num = int(sys.argv[1])
dirname = "/srv/data1/pathcache/graphs_%d_ff/" % query_num
with open("test_queries_%d_ff" % query_num) as fi:
    queries = json.load(fi)
mc = pylibmc.Client(["127.0.0.1:10999"], behaviors={"cas": True})
mc_bgpsim_pid = pylibmc.Client(["127.0.0.1:10998"], behaviors={"cas": True})
mc_bgpsim = pylibmc.Client(["127.0.0.1:10997"], behaviors={"cas": True})
query_composed = {}

for qr in queries:
    path = qr['path']
    tup = tuple(qr['pc'])
    dst = qr['iplane'][-1]
    if ip2asn.ip_to_pref(dst).prefix.replace('/', '_') != str(tup[-1]):
        pdb.set_trace()
    query_composed[tup] = path

query_composed_list = ["-".join([str(x) for x in tup[0]]) + "-" + tup[1]
                       for tup in query_composed.iteritems()]
with open("query_composed_list.json", "w") as fi:
    json.dump(query_composed_list, fi)

print "Get all single homed ASes"
# providers of an asn
provider_asns = {}
# customers of an asn
customer_asns = {}
with open("/home/rachee/data/20160101.as-rel.txt") as f:
    for line in f:
        if line.startswith('#'): continue
        prov, cust, typ = line.split('|')
        if typ == '-1\n':
            if cust in provider_asns:
                provider_asns[cust].append(prov)
            else:
                provider_asns[cust] = [prov]
            if prov in customer_asns:
                customer_asns[prov].append(cust)
            else:
                customer_asns[prov] = [cust]
        else:
            # Treating p2p as provider links that go both
            # ways. Since I want to find ASNs that have *only*
            # one way to go outside of their network
            if cust in provider_asns:
                provider_asns[cust].append(prov)
            else:
                provider_asns[cust] = [prov]
            if prov in provider_asns:
                provider_asns[prov].append(cust)
            else:
                provider_asns[prov] = [cust]
                
single_homed_asns = [x[0] for x in provider_asns.items() if len(x[1]) == 1]

def get_single_homed_customers(asn):
    if str(asn) not in customer_asns:
        return []
    single_homed = set()
    for cust in customer_asns[str(asn)]:
        if cust in single_homed_asns:
            single_homed.add(cust)
    return list(single_homed)

#query_composed_list = query_composed_list[:len(query_composed_list)/2]
    
#query_composed_list_half = query_composed_list[:len(query_composed_list)/10]
#query_composed_list_other_half = query_composed_list[len(query_composed_list)/2:]
#qc_other = dict(query_composed_list_other_half)
#with open("qc_2.json", "w") as fi:
#    json.dump(qc_other, fi)
#print "Going to query for %d paths" % len(query_composed_list_half)
#query_composed = dict(query_composed_list_half)
#with open("qc_1.json", "w") as fi:
#    json.dump(query_composed, fi)

#if sys.argv[2] == 'part2':
#    query_composed = qc_other

orgs = {}
with open("/home/rachee/data/20161001.as-org2info.txt", "rb") as f:
    for line in f:
        # ignore commented lines
        if line[0] == "#":
            continue
        tokens = line.rstrip().split('|')
        # aut|changed|name|org_id|source
        if tokens[0].isdigit():
            asn = int(tokens[0])
            orgs[asn] = tokens[3]

def are_siblings(as1, as2):
    as1 = int(as1)
    as2 = int(as2)
    #print "Checking if siblings:", as1, as2
    if as1 not in orgs or as2 not in orgs:
        return False
    return orgs[ as1 ] == orgs[ as2 ]

def leanify(path):
    lean_paths = []
    assert isinstance(path, list)
    path_lean = [str(path[0])]
    for i in range(1, len(path)):
        if are_siblings(path[i], path_lean[0]):
            continue
        path_lean.append(str(path[i]))
    return path_lean

def add_single_homed_gains(gr_old):        
    gr = Graph(gr_old)
    nodes = [x for x in gr.vertices()]
    asns = [int(gr.vp.asn[node]) for node in nodes]
    gain = 0
    for node in nodes:
        sh_nodes = get_single_homed_customers(gr.vp.asn[node])
        for sh in sh_nodes:
            if int(sh) in asns: continue
            gain += 1
            ver = gr.add_vertex()
            gr.vp.asn[ver] = int(sh)
            ed = gr.add_edge(ver, node)
            gr.vp.mmt_type[ver] = 'SH'
    if gain > 0:
        gr = add_single_homed_gains(gr)
    return gr

def dfs_paths(gr, src_node, dst_node):
    visited = set()
    stack = [tuple([src_node, src_node])]
    while stack:
        path_tuple = stack.pop()
        vertex = path_tuple[-1]
        path = list(path_tuple[:-1])
        visited.add(tuple(path))
        for next in set(vertex.out_neighbours()):
            if tuple(path + [next]) in visited:
                continue
            if next == dst_node:
                yield path + [next]
            else:
                stack.append(tuple( path + [next] + [next]))
                
def has_path(gr, src_node, dst_node):
    allp_iter =  graph_tool.topology.all_paths(gr, src_node, dst_node)
    count = 0
    for x in allp_iter:
        count += 1
        if count > 0:
            return True
    return False

def get_path_prob(gr, path):
    transition_prob = []
    incident_on_node = {}
    for x, y in zip(path, path[1:]):
        edge = gr.edge(x,y)
        source = edge.source()
        target = edge.target()
        if gr.vp.mmt_type[source] == 'SH':
            assert not gr.ep.origin[edge]
            assert source.out_degree() == 1
            transition_prob.append(1.0)
            continue
            
        origin_through_edge = gr.ep.origin[edge]
        if source in incident_on_node:
            incident_on_src = incident_on_node[source]
        else:
            incident_on_src = 0
            for in_nbr in source.in_neighbours():
                if in_nbr == source: continue
                incident_edge = gr.edge(in_nbr, source)
                assert incident_edge
                incident_origin = gr.ep.origin[incident_edge]
                if not incident_origin:
                    assert gr.vp.mmt_type[in_nbr] == 'SH'
                    incident_origin = []
                for origin in incident_origin:
                    incident_on_src += gr.ep.origin[incident_edge][origin]
            incident_on_src += gr.vp.generated[source]
            incident_on_node[source] = incident_on_src
        traversed_on_edge = 0
        for org in origin_through_edge:
            traversed_on_edge += origin_through_edge[org]
        # Laplacian smoothing
        prob = float(traversed_on_edge + 1)/(incident_on_src + source.out_degree())
        assert prob > 0
        assert prob <= 1
        transition_prob.append(prob)
    return round(reduce(mul, transition_prob, 1), 4)

FNULL = open(os.devnull, 'w')
# bgpsim_procs = []
# for i in range(30):
#     portnum = 11000 + 100*query_num + i
#     proc = Popen(['mono',
#                   'bgp_sim/TestingApplication/bin/Release/TestingApplication.exe',
#                   '-server%d' % (portnum), '20160101-cyclops',
#                   'bgp_sim/precomp/US-precomp367.txt',
#                   'bgp_sim/cache/exit_asns.txt' ],
#                  stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
#     print "started bgpsim count", i+1
#     bgpsim_procs.append(proc)
# time.sleep(5)
    
def get_bgp_path(src_asn, dst_asn, portnum):
    bgp_sim_key = "%s-%s" % (str(src_asn), str(dst_asn))
    if bgp_sim_key in mc_bgpsim and mc_bgpsim.get(bgp_sim_key):
        return mc_bgpsim.get(bgp_sim_key)
    MESSAGE = str(dst_asn) + " -q"
    MESSAGE += " " + str(src_asn) + " " + str(dst_asn)
    MESSAGE += " <EOFc> "
    print "Sending message to BGPSim to get paths from %s to %s on port num %d" % \
        (src_asn, dst_asn, portnum)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    try:
        s.connect(('127.0.0.1', portnum))
    except:
        print "Connection refused when connecting to bgpsim portnum", portnum
        return
    s.send(MESSAGE)
    data = ""
    result = dict()
    buffer_size = 100000
    while True:
        d = s.recv(buffer_size)
        data += d
        if len(d) == 0:
            break
        if "<EOFs>" in d:
            break
    s.close()
    #pdb.set_trace()
    arr = data.split("-\n")
    del data
    arr = arr[0]
    arr = arr.split(':\n')[-1]
    hops = arr.split('\n')
    del arr
    hops_new = []
    for hop in hops:
        if hop == str(dst_asn):
            hops_new.append(hop)
            break
        if not hop: continue
        hops_new.append(hop)
    #print hops_new
    bgp_path = " ".join(hops_new)
    mc_bgpsim.set(bgp_sim_key,  bgp_path)
    return bgp_path

def get_ranked_paths(gr, src_asn, dst_node, add_single_homed=True):
    src_node = find_vertex(gr, gr.vp.asn, src_asn)
    if src_node:
        src_node = src_node[0]
    elif 'sh' in gr.gp and gr.gp["sh"]:
        return []
    elif not add_single_homed:
        return []
    else:
        gr = add_single_homed_gains(gr)
        gprop = gr.new_graph_property("boolean")
        gr.gp["sh"] = gprop
        gr.gp["sh"] = True
        fname = gr.vp.prefix[dst_node]
        gr.save("%s/%s_sh.gt" % (dirname, fname))
        src_node = find_vertex(gr, gr.vp.asn, src_asn)
        if not src_node:
            return []
        assert len(src_node) == 1
        src_node = src_node[0]
        
    src_dst_paths_gen = dfs_paths(gr, src_node, dst_node)
    src_dst_paths = []
    count = 0
    for path in src_dst_paths_gen:
        count += 1
        if count > 50: break
        for v, w in zip(path[:-1], path[1:]):
            assert gr.edge(v, w)
        path_asns = []
        for vertex in path:
            path_asns.append(gr.vp.asn[vertex])
        prob = get_path_prob(gr, path)
        src_dst_paths.append((path_asns,  prob))
    src_dst_paths = sorted(src_dst_paths, key=lambda x: x[1], reverse=True)
    return src_dst_paths[:10]

def get_spliced_bgp_pathcache(pref, src_asn, bgp_hops):
    grfname = os.path.join(dirname, "%s.gt" % pref)
    grfname_sh = os.path.join(dirname, "%s_sh.gt" % pref)
    if os.path.isfile(grfname_sh):
        gr = load_graph(grfname_sh, fmt="gt")
    elif os.path.isfile(grfname):
        gr = load_graph(grfname, fmt="gt")
    else:
        return [], None
    #pdb.set_trace()
    dst_node = find_vertex(gr, gr.vp.prefix, pref)
    assert len(dst_node) == 1
    dst_node = dst_node[0]
    if not bgp_hops:
        return [], None
    bgp_hops = bgp_hops.split()
    assert bgp_hops[0] == str(src_asn)
    dst_node_in_gr = find_vertex(gr, gr.vp.asn, bgp_hops[-1])
    if not( dst_node_in_gr and len(dst_node_in_gr) == 1):
        return [], None
    dst_node_in_gr = dst_node_in_gr[0]
    if not dst_node_in_gr == dst_node:
        return [], None
    spliced_path = []
    found = False
    for node in bgp_hops:
        node_in_gr = find_vertex(gr, gr.vp.asn, node)
        if not node_in_gr:
            spliced_path.append(str(node))
        else:
            if not len(node_in_gr) == 1:
                return [], None
            node_in_gr = node_in_gr[0]
            node_asn = gr.vp.asn[node_in_gr]
            path_key = "%s-%s" % (node_asn, pref)
            if path_key in mc and mc.get(path_key):
                ranked_paths = mc.get(path_key)[0]
                found = True
                break
            else:
                if not has_path(gr, node_in_gr, dst_node) or node_in_gr.out_degree() == 0:
                    spliced_path.append(node)
                    continue
                ranked_paths = get_ranked_paths(gr, node, dst_node)
                if not ranked_paths:
                    spliced_path.append(node)
                    continue                    
                found = True
                break
    spliced_paths = []
    len_splices = {}
    if found:
        for pa in ranked_paths:
            path_list = pa[0]
            prob = pa[1]
            mix_path = spliced_path + ['|'] + path_list
            spliced_paths.append(([str(x) for x in mix_path], prob))
            #len_splices[" ".join([str(x) for x in spliced_path + path_list])] = len(spliced_path)
    else:
        #spliced_paths = [(spliced_path, 1)],
        #{" ".join([str(x) for x in spliced_path]): len(spliced_path)}
        #return [(spliced_path, 1)], {" ".join([str(x) for x in spliced_path]): len(spliced_path)}
        spliced_paths = [(spliced_path + ['|'], 1.0)]
    return spliced_paths

def siblings(asn):
    if int(asn) not in orgs:
        return []
    org_id = orgs[int(asn)]
    sib = []
    for elem in orgs:
        if orgs[elem] == org_id:
            sib.append(elem)
    return sib
                                                        
def find_rank_prob(measured_path, pathcache_paths, dst_asn):
    rank = -1
    prob = -1
    count = 0
    if '*' in measured_path:
        measured_path_mod = " ".join([x for x in measured_path.split() if x != '*'])
    else:
        measured_path_mod = measured_path

    for path_list in pathcache_paths:
        path_raw = path_list[0]
        if '|' in path_raw:
            path = [x for x in path_raw if x != '|']
        else:
            path = path_raw
        if str(path[-1]) != dst_asn:
            if dst_asn in measured_path:
                try:
                    path = path + [int(dst_asn)]
                except TypeError:
                    print "Don't know what happend", path, dst_asn
                    return rank, prob
        pr = path_list[1]
        measured_path_mod_substitute = [x for x in measured_path.split()]
        path_str = " ".join([str(x) for x in path])
        count += 1
        if path_str == measured_path or path_str == measured_path_mod:
            rank = count
            prob = pr
            break
        else:
            if '*' in measured_path and len(measured_path_mod_substitute) == len(path):
                try:
                    ind_star =  measured_path_mod_substitute.index('*')
                except ValueError:
                    pdb.set_trace()
                measured_path_mod_substitute[ind_star] = str(path[ind_star])
                if path_str == " ".join(measured_path_mod_substitute):
                    rank = count
                    prob = pr
                    break
            # Time to try sibling resolution at each hop
            measured_hops = [x for x in measured_path.split() if x!='*']
            meas_lean = leanify(measured_hops)
            path_lean = leanify(path)
            if len(meas_lean) != len(path_lean): continue
            mismatch = False
            for x in meas_lean:
                ind = meas_lean.index(x)
                if x == path_lean[ind]:
                    continue
                if int(x) in siblings(path[ind]):
                    continue
                mismatch = True
                break
            if not mismatch:
                rank = count
                prob = pr
                break
    return rank, prob

def get_pc_path(src_asn, dst_pref, add_single_homed=True):
    grfname = os.path.join(dirname, "%s.gt" % dst_pref)
    grfname_sh = os.path.join(dirname, "%s_sh.gt" % dst_pref)
    if os.path.isfile(grfname_sh):
        gr = load_graph(grfname_sh, fmt="gt")
    elif os.path.isfile(grfname):
        gr = load_graph(grfname, fmt="gt")
    else:
        return []
    dst_node = find_vertex(gr, gr.vp.prefix, dst_pref)
    assert len(dst_node) == 1
    dst_node = dst_node[0]
    return get_ranked_paths(gr, src_asn, dst_node, add_single_homed=add_single_homed)

def get_pc_or_bgp_path(src_asn, dst_pref, portnum, measured_path):
    typ = None
    dst_asn = ip2asn.ip2asn_bgp(dst_pref.split('_')[0])
    path_key = "%s-%s" % (src_asn, dst_pref)
    if path_key in mc and mc.get(path_key):
        return
    paths = get_pc_path(src_asn, dst_pref)
    if not paths:
        if dst_asn in ip2asn.asn_to_prefs: 
            prefs = ip2asn.asn_to_prefs[dst_asn]
        else:
            prefs = []
        sister_paths = []
        while prefs:
            sister_pref = prefs.pop(0)
            print "trying sister pref", sister_pref
            sister_pref = sister_pref.replace('/', '_')
            sister_paths = get_pc_path(src_asn, sister_pref, add_single_homed=False)
            if sister_paths:
                break
        if not sister_paths:
            bgp_path = get_bgp_path(src_asn, dst_asn, portnum)
            if not bgp_path:
                mc.set(path_key, "")
                return
            if bgp_path.split()[-1] != dst_asn:
                print "bgpsim: error, not querying"
                mc.set(path_key, "")
                return
            spliced_paths = get_spliced_bgp_pathcache(dst_pref, src_asn, bgp_path)
        if sister_paths:
            paths = sister_paths
            del sister_paths
            typ = "sis"
        elif spliced_paths:
            paths = spliced_paths
            del spliced_paths
            typ = "spliced"
    else:
        typ = "pc"
        
    if not paths:
        mc.set(path_key, ([], measured_path, -1, -1, None))
        return
    print "Got these as paths:",  paths, typ, "for query:", src_asn, dst_pref
    pred_path = paths[0]
    if not pred_path:
        mc.set(path_key, ([], measured_path, -1, -1, None))
        #mc.set(path_key, ())
        return
    rank, prob = find_rank_prob(measured_path, paths, dst_asn)
    mc.set(path_key, (paths, measured_path, rank, prob, typ))

def wrap_function(qr_set, portnum):
    pid = None
    try:
        current = mp.current_process()
        identity = current._identity
        #identity = (1,)
        print "ID:", identity
        proc = Popen(['mono',
                      'bgp_sim/TestingApplication/bin/Release/TestingApplication.exe',
                      '-server%d' % (portnum), '20160101-cyclops',
                      'bgp_sim/precomp/US-precomp367.txt',
                      'bgp_sim/cache/exit_asns.txt' ],
                     preexec_fn=os.setsid,
                     stdin=subprocess.PIPE,
                     stderr=subprocess.PIPE,
                     stdout=subprocess.PIPE)
        time.sleep(10)
        pid = proc.pid
        mc_bgpsim_pid.set(str(portnum), str(pid))
        
        for query in qr_set:
            src_asn = int(query.split('-')[0])
            dst_pref = query.split('-')[1]
            dst_asn = ip2asn.ip2asn_bgp(dst_pref.split('_')[0])
            meas_path = query.split('-')[-1]
            path_key = "%s-%s" % (src_asn, dst_pref)
            if path_key in mc and mc.get(path_key):
                print "Found it in the cache", path_key
                continue
            get_pc_or_bgp_path(src_asn, dst_pref, portnum, meas_path)
    except:
        print "".join(traceback.format_exception(*sys.exc_info()))
        
    try:
        proc.kill()
        os.killpg(pid, signal.SIGTERM)
        os.system("kill -9 %s" % pid)
        mc_bgpsim_pid.set(str(portnum), None)
    except:
        pass
    return pid

def wrapper_wrap(st, en, portnum):
    global query_composed_list
    gc.collect()
    try:
        pid = wrap_function(query_composed_list[st:en], portnum)
        return pid
    except:
        print "".join(traceback.format_exception(*sys.exc_info()))
        pass
    
def task_done(future):
    print "Done"
    try:
        pid = future.result()  # blocks until results are ready
        print "PID of bgpsim:", pid
        try:
            if pid:
                os.killpg(pid, signal.SIGTERM)
                os.system("kill -9 %s" % pid)
                mc_bgpsim_pid.set(str(pid), False)
                print "Killed BGPSIM"
        except OSError:
            pass
    except Exception as error:
        print("Function raised %s" % error)
        print(error.traceback)  # traceback of the function
        
num_queries_per_process = 10
num_chunks = len(query_composed_list)/num_queries_per_process + 1
print num_chunks
count = 0
with ProcessPool(max_workers=30, max_tasks=1) as pool:
    for x in range( num_chunks ):
        count += 1
        start = x * num_queries_per_process
        end = start + num_queries_per_process
        if end > len(query_composed_list) - 1:
            end = len(query_composed_list)
        print start, end
        portnum = 10000 + 100*query_num + x
        future = pool.schedule(wrapper_wrap, args=[start,end, portnum], timeout=600)
        future.add_done_callback(task_done)


# pool = mp.Pool(processes=70, maxtasksperchild=1)
# for x in range(num_chunks):
#     start = x * num_queries_per_process
#     end = start + num_queries_per_process
#     if end > len(query_composed_list) - 1:
#         end = len(query_composed_list)
#     print start, end
#     pool.apply_async(wrapper_wrap, args=(query_composed_list[start: end],))
    #wrapper_wrap(query_composed_list[start: end],)
    
pool.close()
pool.join()
pdb.set_trace()
