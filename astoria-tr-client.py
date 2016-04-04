#!/usr/bin/python
from graph_tool.all import *
import os
import multiprocessing as mp
import pdb
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from collections import defaultdict
from itertools import groupby
import sqlite3
import pygeoip
import settings
import os
import json
import time
import urllib2
from networkx.readwrite import json_graph
import networkx as nx
import socket
import logging
import argparse
import radix

# Global cache for paths and IPs
path_cache = dict()
ip_asn_cache = dict()
#logging.basicConfig( filename="logs/tr_client.log", level=logging.DEBUG )

def profile_time(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logging.debug('[PERFORMANCE-LOG] %r (%r) %2.2f sec' % (method.__name__, kw, te-ts))
        return result
    return timed

@profile_time
def parse_args(print_help=False):
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', '-d', help='Enable debug logging display',
                        action='store_true')
    parsed_args = parser.parse_args()
    if print_help:
        parser.print_help()
        sys.exit(0)
    return parsed_args

class RequestHandler():
    def __init__(self, sock):
        self.rtree_bgp = radix.Radix()
        with open(constants.PFX2ASN_DATA) as fi:
            for line in fi:
                ip, preflen, asn = line.split()
                if ',' in asn:
                    tokens = asn.split(',')
                    asn = tokens[0]
                if '_' in asn:
                    tokens = asn.split('_')
                    asn = tokens[0]
                rnode = rtree_bgp.add(network=ip, masklen=int(preflen))
                rnode.data["asn"] = asn

        self.dest_graphs = self.load_dest_graphs()
        cur_time  = time.time()
        self.graph_last_update_time = time.time()
        self.hijacks = dict()
        self.prefix_radix = radix.Radix()
        #self.build_moas_db()
        self.hijack_update_time = time.time()
        print "Ready to accept connections.."

    def load_dest_graphs(self):
        files = [ x for x in os.listdir( settings.GRAPH_DIR_FINAL ) \
                  if os.path.isfile( os.path.join( settings.GRAPH_DIR_FINAL, x ) ) ]
        files = [ os.path.join( settings.GRAPH_DIR_FINAL, f ) for f in files ]
        all_graphs = {}
        for f in files:
            asn_to_id = {}
            asn = f.split( '/' )[ -1 ].split('.')[0]
            gr = load_graph(f, fmt="gt")
            for vertex in gr.vertices():
                asn_to_id[gr.vp.asn[vertex]] = int(vertex)
            all_graphs[asn] = (gr, asn_to_id)
        return all_graphs

    @profile_time
    def update_graph_state(self):
        update = self.load_graphs_in_mem()
        for asn, gr in update:
            logging.info("Updating the graph for", asn)
            self.graphs[asn] = gr
        self.graph_last_update_time = time.time()

    @profile_time
    def ip_to_asn(self, ip):
        global ip_asn_cache
        asn = None
        try:
            asn = ip_asn_cache[ip]
            logging.debug("Found IP (%s) ->ASN (%s) map in local cache." % (ip, str(asn)))
        except KeyError:
            try:
                node = self.rtree_bgp.search_best(ip)
                if node:
                    asn = node.data['asn']
                ip_asn_cache[ip] = asn
            except Exception as e:
                logging.debug("Non-fatal Exception: %s" % e)
                return "AS: 123456"
        if asn:
            asn = "AS: " + str(asn)
        if "-1" in asn:
            asn = "AS: 123456"
        return asn
            
    def tr_graphs_cb( self ):
        print "Finished running update_graphs"

    @profile_time
    def get_paths(self, pairs):
        global path_cache
        all_paths = dict()
        bgpsim_pairs = list()

        for pair in pairs:
            src, dst = pair.split("-")[0], pair.split("-")[1]
            logging.debug("Getting path between " + src + " and " + dst)
            # Checking the global cache for the path
            try:
                all_paths[pair] = path_cache[pair]
                logging.debug("Found entry in cache.")
                continue
            except KeyError as e:
                logging.debug("Non-fatal KeyError Exception: %s" % e)
                pass
            # Checking to see if Sibyl can give us the path
            all_paths[pair] = self.get_path(src, dst)
            if all_paths[pair] is not None:
                logging.debug("Found entry in Sibyl/iplane based TRs or BGPStream")
                continue
            # Have to go to BGPSim. Saving the pair for this.
            logging.debug("Path not found in cache or graphs. Using BGPSim for path estimation.")
            bgpsim_pairs.append(pair)
            try:
                del all_paths[pair]
            except KeyError as e:
                logging.debug("Non-fatal KeyError encountered while deleting entry. %s" % e)
                pass
        if len(bgpsim_pairs) > 0:
            bgpsim_paths = self.get_paths_bgp_sim(bgpsim_pairs)
            all_paths.update(bgpsim_paths)
        # Update the global cache
        for key in all_paths:
            path_cache[key] = all_paths[key]
        return all_paths

    @profile_time
    def create_prefix_radix(self):
        logging.info("Creating prefix radix")
        self.prefix_radix = radix.Radix()
        for keys in self.hijacks.keys():
            self.prefix_radix.add(keys)
            logging.debug("Added %s to prefix radix tree" % keys)

    @profile_time
    def build_moas_db(self):
        os.system("sudo cp /home/rufy/moas.db .")
        logging.info("Building MOAS DB")
        conn = sqlite3.connect(settings.HIJACKS_DB)
        cursor = conn.cursor()
        try:
            data = cursor.execute("SELECT * FROM rel_info")
        except:
            data = []
            logging.info("At time %s MOAS db was empty" % str(time.time()))

        # The db schema is prefix, dom_asn, sec_asn, susp, reason, timestamp
        # suspicion > 0 => event is suspicious
        self.hijacks = dict()
        for row in data:
            if ':' in row[0]:
                # Ignoring IPv6 activity
                continue
            if row[3] > 0:
                prefix, dom, sec, susp, reason, timestamp = row[0], str(row[1]), str(row[2]), str(row[3]), str(row[4]), int(row[5])
                self.hijacks[prefix] = {'dom_asn': dom, 'sec_asn': sec, 'susp': susp, 'reason': reason, 'timestamp': timestamp}
                logging.debug("Loading entry from MOAS DB: %s" % json.dumps(self.hijacks[prefix]))
        ## DISABLE THIS FOR PRODUCTION ##
        # logging.info("Adding a fake hijack entry for testing")
        #self.hijacks['130.245.0.0/16'] = {'dom_asn': "5719", 'sec_asn': "3", 'susp': "2", 'reason': "testing", 'timestamp': "time.time()"}
        self.create_prefix_radix()
        self.moas_last_update_time = time.time()

    def dfs_paths(gr, src_node, dst_node):
        stack = [(src_node, [src_node])]
        while stack:
            (vertex, path) = stack.pop()
            for next in set(vertex.out_neighbours()) - set(path):
                if next == dst_node:
                    yield path + [next]
                else:
                    stack.append((next, path + [next]))

    @profile_time
    def get_path(self, src, dst):
        if dst in self.dest_graphs:
            gr = self.dest_graphs[dst]
            # Find all paths from src to dst in this graph
            src_node = find_vertex(gr, gr.vp.asn, int(src))
            if not src_node:
                return None
            src_node = src_node[0]
            dst_node = find_vertex(gr, gr.vp.asn, int(dst))
            assert dest_node
            dest_node = dest_node[0]
            src_dst_paths = dfs_paths(gr, src_node, dst_node)
        else:
            return None

        ases_in_path = set()
        count = 0
        # src_dst_paths is an iterable, getting 1000 paths
        # working with those AS hops
        for path in src_dst_paths:
            count += 1
            if count > 1000:
                break
            ases_in_path.update(path)
        ases_in_path = list(ases_in_path)
        ases_in_path = [str(x) for x in ases_in_path]

    @profile_time
    def get_paths_bgp_sim(self, pairs):
        destinations = list()
        query_1, query_2 = "", "-q "
        for pair in pairs:
            src, dst = (pair.split("-")[0]).split("AS")[1], (pair.split("-")[1]).split("AS")[1]
            if src not in destinations:
                destinations.append(src)
                query_1 += src + " "
            if dst not in destinations:
                destinations.append(dst)
                query_1 += dst + " "
            query_2 += src + " " + dst + " "
        ip = '127.0.0.1'
        port = 11000
        buffer_size = 1000000
        query = query_1 + query_2 + "<EOFc>"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        s.send(query)
        data = ""
        result = dict()
        while True:
            d = s.recv(buffer_size)
            data += d
            if len(d) == 0:
                break
            if "<EOFs>" in d:
                break
        s.close()
        data = data.split("-\n<EOFs>")[0]
        arr = data.split("-\n")
        for a in arr:
            pair, path = list(), list()
            for line in a.split("\n"):
                if line.startswith("ASes"):
                    line = line.strip(":")
                    pair = [s for s in line.split(" ") if s.isdigit()]
                    pair_string = "AS" + pair[0] + "-" + "AS" + pair[1]
                if not line.startswith("ASes") and line != '':
                    path.append(int(line))
            try:
                result[pair_string] = path
            except Exception as e:
                logging.debug("Exception encountered. %s " % e)
        return result

    @profile_time
    def handle_ip_query(self, query):
        ip = query.split(":")[1]
        socket.inet_aton(ip)
        response = self.ip_to_asn(ip) + "\n<EOFs>"
        return response

    @profile_time
    def handle_batch_ip_query(self, query):
        data = query.split(",")
        response = ""
        for d in data:
            if len(d) < 10:
                continue
            ip = d.split(":")[1]
            socket.inet_aton(ip)
            response += "IP: " + ip + " " + self.ip_to_asn(ip) + "\n"
        response += "<EOFs>"
        return response

    @profile_time
    def handle_path_query(self, query):
        src, en_string, ex_string, dst, eofc = query.split(" ")
        en_list, ex_list, all_pairs = list(), list(), list()
        src, dst = "AS" + src, "AS" + dst
        for en in en_string.split(","):
            en_list.append("AS" + en)
        for ex in ex_string.split(","):
            ex_list.append("AS" + ex)
        for en in set(en_list):
            all_pairs.append(en + "-" + src)
            all_pairs.append(src + "-" + en)
        for ex in set(ex_list):
            all_pairs.append(ex + "-" + dst)
            all_pairs.append(dst + "-" + ex)
        all_paths = self.get_paths(all_pairs)
        response = ""
        for key in all_paths:
            src, dst = key.split("-")[0], key.split("-")[1]
            response += "ASes from " + src.split("AS")[1] + " to " + dst.split("AS")[1] + "\n"
            for a in all_paths[key]:
                response += str(a) + "\n"
            response += "-\n"
        response += "<EOFs>"
        return response

    @profile_time
    def parse_hijack_and_path_query(self, query):
        src, en_string, ex_string, dst, src_ip, en_string_ip, ex_string_ip, dst_ip, eofc = query.split(" ")
        parsed_query = dict()
        parsed_query['source'] = {'ip': str(src_ip), 'as': str(src), 'prefix': None, 'hijacking_ases': None}
        parsed_query['destination'] = {'ip': str(dst_ip), 'as': str(dst), 'prefix': None, 'hijacking_ases': None}
        entry_ases, entry_ips = en_string.split(","), en_string_ip.split(",")
        exit_ases, exit_ips = ex_string.split(","), ex_string_ip.split(",")
        for i in range(0, len(entry_ases)):
            key = "entry-" + str(i)
            parsed_query[key] = {'ip': str(entry_ips[i]), 'as': str(entry_ases[i]), 'prefix': None, 'hijacking_ases': None}
        for i in range(0, len(exit_ases)):
            key = "exit-" + str(i)
            parsed_query[key] = {'ip': str(exit_ips[i]), 'as': str(exit_ases[i]), 'prefix': None, 'hijacking_ases': None}
        for key in parsed_query.keys():
            try:
                prefix = self.prefix_radix.search_best(parsed_query[key]['ip']).prefix
            except AttributeError:
                prefix = None
            parsed_query[key]['prefix'] = prefix
            try:
                parsed_query[key]['hijacking_ases'] = str(self.hijacks[prefix]['sec_asn'])
            except KeyError:
                parsed_query[key]['hijacking_ases'] = None
            logging.debug("Key: %s, Value: %s" % (key, json.dumps(parsed_query[key])))
        return parsed_query

    @profile_time
    def get_pairs_considering_hijacks(self, parsed_query, var_string, fixed_string):
        pairs = list()
        for key in parsed_query.keys():
            if var_string in key:
                # Pick one of the paths to be the "forward" path. Let the other be the reverse path.
                # The FWD attacking AS is the AS which hijacks paths to the forward destination.
                # The REV attacking AS is the AS which hijacks paths to the reverse destination.
                fwd_src, fwd_dst, fwd_att_as = parsed_query[fixed_string]["as"], parsed_query[key]["as"], parsed_query[key]["hijacking_ases"]
                rev_src, rev_dst, rev_att_as = fwd_dst, fwd_src, parsed_query[fixed_string]["hijacking_ases"]
                pair_fwd, pair_rev = "AS" + fwd_src + "-AS" + fwd_dst, "AS" + rev_src + "-AS" + rev_dst
                pairs.append(pair_fwd)
                pairs.append(pair_rev)
                if fwd_att_as is not None:
                    attack_pair_1, attack_pair_2 = "AS" + fwd_src + "-AS" + fwd_att_as, "AS" + fwd_att_as + "-AS" + fwd_dst
                    pairs.append(attack_pair_1)
                    pairs.append(attack_pair_2)
                    logging.debug("FWD query (wo hijack): %s | FWD query (w hijack) also includes : %s and %s" % (pair_fwd, attack_pair_1, attack_pair_2))
                if rev_att_as is not None:
                    attack_pair_1, attack_pair_2 = "AS" + rev_src + "-AS" + rev_att_as, "AS" + rev_att_as + "-AS" + rev_dst
                    pairs.append(attack_pair_1)
                    pairs.append(attack_pair_2)
                    logging.debug("REV query (wo hijack): %s | REV query (w hijack) also includes : %s and %s" % (pair_rev, attack_pair_1, attack_pair_2))
        return pairs

    @profile_time
    def generate_hijack_response(self, parsed_query, paths, var_string, fixed_string):
        response = ""
        for key in parsed_query.keys():
            if var_string in key:
                fwd_src, fwd_dst, fwd_att_as = parsed_query[fixed_string]["as"], parsed_query[key]["as"], parsed_query[key]["hijacking_ases"]
                rev_src, rev_dst, rev_att_as = fwd_dst, fwd_src, parsed_query[fixed_string]["hijacking_ases"]
                pair_fwd, pair_rev = "AS" + fwd_src + "-AS" + fwd_dst, "AS" + rev_src + "-AS" + rev_dst
                response += "ASes from " + fwd_src + " to " + fwd_dst + "\n"
                if fwd_att_as is None:
                    for a in paths[pair_fwd]:
                        response += str(a) + "\n"
                    response += "-\n"
                if fwd_att_as is not None:
                    no_hijack_pair, blackhole_pair, forwarding_pair = pair_fwd, "AS" + fwd_src + "-AS" + fwd_att_as, "AS" + fwd_att_as + "-AS" + fwd_dst
                    ases = set(paths[no_hijack_pair] + paths[blackhole_pair] + paths[forwarding_pair])
                    for a in ases:
                        response += str(a) + "\n"
                    response += "-\n"
                response += "ASes from " + rev_src + " to " + rev_dst + "\n"
                if rev_att_as is None:
                    for a in paths[pair_rev]:
                        response += str(a) + "\n"
                    response += "-\n"
                if rev_att_as is not None:
                    no_hijack_pair, blackhole_pair, forwarding_pair = pair_rev, "AS" + rev_src + "-AS" + rev_att_as, "AS" + rev_att_as + "-AS" + rev_dst
                    ases = set(paths[no_hijack_pair] + paths[blackhole_pair] + paths[forwarding_pair])
                    for a in ases:
                        response += str(a) + "\n"
                    response += "-\n"
        return response

    @profile_time
    def handle_hijack_and_path_query(self, query):
        parsed_query = self.parse_hijack_and_path_query(query)
        pairs_source = self.get_pairs_considering_hijacks(parsed_query=parsed_query, var_string="entry", fixed_string="source")
        pairs_dest = self.get_pairs_considering_hijacks(parsed_query=parsed_query, var_string="exit", fixed_string="destination")
        all_pairs = pairs_source + pairs_dest
        all_paths = self.get_paths(all_pairs)
        response = self.generate_hijack_response(parsed_query=parsed_query, paths=all_paths, var_string="entry", fixed_string="source")
        response += self.generate_hijack_response(parsed_query=parsed_query, paths=all_paths, var_string="exit", fixed_string="destination")
        response += "<EOFs>"
        logging.debug("W/HIJACKS RESPONSE: %s", response)
        return response

    @profile_time
    def handle_read(self, sock):
        data = sock.recv(100000)
        if data:
            query = str(data)
            logging.info("Query: %s. [To see response, enable debug mode]" % query)
            response = ""
            if ("IP" in data) and ("Batch" not in data):
                logging.info("Query is a single IP->AS map request")
                try:
                    response = self.handle_ip_query(data)
                except socket.error:
                    logging.debug("Non-fatal socket exception.")
                    pass
            elif ("IP" in data) and ("Batch" in data):
                logging.info("Query is a batch IP->AS map request")
                try:
                    response = self.handle_batch_ip_query(data)
                except socket.error:
                    logging.debug("Non-fatal socket exception.")
                    pass
            elif len(data.split(" ")) <= 9:
                try:
                    if " -1 " in data:
                        logging.info("Found a -1 in the old query (%s) . " % data) 
                        data.replace(" -1 ", " 123456 ")
                        logging.info("New query: %s " % data)
                    if len(data.split(" ")) == 5:
                        logging.info("Query is a hijack-independent path request")
                        response = self.handle_path_query(data)
                    elif len(data.split(" ")) == 9:
                        logging.info("Query is a hijack-dependent path request")
                        response = self.handle_hijack_and_path_query(data)
                    else:
                        logging.error("Non-fatal parsing error.")
                        sock.send("Parsing Error")
                        return
                except Exception as e:
                    logging.error("Fatal or non-fatal exception: %s." % e)
                    sock.send("Parsing Error")
                    return
            else:
                logging.error("Non-fatal parsing error.")
                sock.send("Parsing Error")
                return
            logging.debug("Response: %s" % response)
            return sock.send(response)


def main():
    args = parse_args()
    # remove default handler if there is any before calling basicConfig
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)-8s %(filename)s:%(lineno)-4d: %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='tr-client.log')
        logging.info("Detailed logging info enabled")
    else:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s %(filename)s:%(lineno)-4d: %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='tr-client.log')
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host, port = "localhost", 8182
    try:
        sock.bind((host, port))
    except Exception as e:
        logging.error("Exception: %s" % e)
        pass
    sock.listen(1)
    handler = RequestHandler(sock)
    while True:
        logging.info("Waiting for a connection on port: %d " % port)
        connection, client = sock.accept()
        try:
            handler.handle_read(connection)
        finally:
            logging.debug("Done sending response")
        connection.close()

main()
