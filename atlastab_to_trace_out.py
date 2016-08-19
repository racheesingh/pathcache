import sys
import argparse
from collections import defaultdict
import re, os
import errno
from subprocess import call


def create_traceroute_text(dst, iptraceroute, node_ip):
    """
	Convert the iptraceroute toward dst to a string
	compliant with convert_trc_summary.c

	Sample output (the third column is the returned TTL,
	not available from the current drivers):
	traceroute 123.123.123.123
	1 1.1.1.1 3 0
	2 2.2.2.2 13 0
	3 3.3.3.3 22 0
	4 4.4.4.4 31 0
	5 123.123.123.123 45  0

	Sample input:
	node_ip = 1.1.1.1
	dst = 123.123.123.123
	iptraceroute = [(ip, latency, revttl)]
	"""

    out = ['traceroute ' + dst]
    ips = [x[0] for x in iptraceroute]

    hh = 0
    # if node_ip not in ips:
    #    out.append(str(hh + 1) + " " + node_ip + " 0 0")
    #    hh += 1

    for ii in range(0, len(iptraceroute)):
        if iptraceroute[ii][0] != "*":
            out.append(str(hh + 1) + " " + iptraceroute[ii][0] + " " + iptraceroute[ii][1] + " " + iptraceroute[ii][2])
        else:
            out.append(str(hh + 1) + " 0.0.0.0 0 0")
        hh += 1

    return "\n".join(out) + "\n\n"


def dump_all_traceroutes(node, node_ip, inpath, lines, outdir, format):
    lines.sort()

    with open(outdir + '/trace.text.' + node, "w") as fw:

        ff = open(inpath)

        next_line = 0

        while True:

            if next_line >= len(lines):
                break

            #print "Dumping line ", lines[next_line]

            try:
                # subset of lines
                ff.seek(lines[next_line])
                next_line += 1

                path = ff.readline()

                if path == "":
                    break

                tmp = path.split()
                src, dest = tmp[0], tmp[1]

                traceroute = " ".join(tmp[2:])

                traceroute = [tuple(x.split()[0].split(",")) for x in traceroute.split("|")]
                traceroute_str = create_traceroute_text(dest, traceroute, node_ip)
                fw.write(traceroute_str)

            except:
                sys.stderr.write('Errors in this line: ' + path)
                continue

        ff.close()


def convert(convert_path, outdir, node):
    infile = outdir + 'trace.text.' + node
    outfile = outdir + 'trace.out.' + node
    print infile, outfile

    call([convert_path, infile, outfile])


def load_mapping(mapping_file):
    """
    Load the nodeid to IP source address mapping file
    :param mapping_file: ip nodeid
    :return:
    """
    node2ip = {}
    f = open(mapping_file)
    for ll in f:
        ll = ll.strip()
        
        #if len(ll.strip()) == 0 or ll.startswith("#"):
        #    continue
        if ll == '' or ll.startswith('#'):
            continue

        ip, node = ll.split()
        node2ip[node] = ip        

        #node2ip[ll.split(" ")[1].strip()] = ll.split(" ")[0]
    f.close()
    return node2ip


def create_parser():
    desc = '''Generate a new source atlas for a vp starting from the traceroutes stored in the database.'''

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--atlastab-dir',
                        dest='dirpaths',
                        metavar='FILE',
                        type=str,
                        required=True,
                        help='Directory containing all the atlas *.tab files.')

    parser.add_argument('--node-ip-mapping',
                        dest='idip',
                        metavar='FILE',
                        type=str,
                        required=True,
                        help='Dile containing ip id mapping for each node.')

    parser.add_argument('--out-dir',
                        dest='out',
                        metavar='DIR',
                        type=str,
                        required=True,
                        help='Output directory.')

    parser.add_argument('--convert-path',
                        dest='convert',
                        metavar='FILE',
                        type=str,
                        required=True,
                        help='Convert binary.')

    return parser


def mkdir_p(path):  # {{{
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


# }}}

def main():
    parser = create_parser()
    opts = parser.parse_args()

    pathconvert = opts.convert
    mapping = load_mapping(opts.idip)

    print('Loaded %d entries in node2ip mapping' % len(mapping))

    from os import listdir
    from os.path import isfile, join

    onlyfiles = [f for f in listdir(opts.dirpaths) if isfile(join(opts.dirpaths, f)) and f.endswith(".tab")]

    for file_ in onlyfiles:

        print "Working on tab file: ", file_

        res = re.findall('\d{4,4}\-\d{2,2}\-\d{2,2}', file_)
        mdate = "".join(res[0].split("-"))

        mkdir_p(opts.out + "/" + mdate)

        probe2paths = defaultdict(list)

        cpos = 0
        # with open(opts.paths) as f:
        f = open(opts.dirpaths + '/' + file_)
       
        while True:

            line = f.readline()

            if line == '':
                break

            try:
                tmp = line.split()
                src, dst = tmp[0], tmp[1]

                probe2paths[src].append(cpos)

            except:
                sys.stderr.write('Error in this line: ' + line)

            finally:
                cpos = f.tell()

        f.close()

        print "Loaded nodes: ", len(probe2paths)

        for node in probe2paths:

            #print "Node", node

            #if node == "19719":
            #    print probe2paths[node]

            #print node, len(probe2paths[node])

            if 'ripe.' + node + '.id' not in mapping:
                sys.stderr.write("No mapping for: %s\n" % node)
                continue
            
            dump_all_traceroutes('ripe.' + node + '.id', mapping['ripe.' + node + '.id'], opts.dirpaths + '/' + file_, probe2paths[node],
                                 opts.out + "/" + mdate + "/", 'atlas_tab')
            convert(pathconvert, opts.out + "/" + mdate + "/", 'ripe.' + node + '.id')
            

    return 0


if __name__ == '__main__':
    sys.exit(main())

