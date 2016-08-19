import json
import pdb

def get_customer_cone_sizes():
    customer_cones = {}
    with open("data/20160201.ppdc-ases.txt") as fi:
        for line in fi:
            if line.startswith('#'): continue
            asn_list = line.split()
            if len(asn_list) > 1:
                try:
                    customer_cones[int(asn_list[0])] = len([int(x) for x in asn_list[1:]])
                except:
                    pdb.set_trace()
    return customer_cones
customer_cone_sizes = get_customer_cone_sizes()


with open("cipollino-verify/violations") as fi:
    violations = json.load(fi)

asrank_violation_data = []
for asn, violation in violations.iteritems():
    if int(asn) not in customer_cone_sizes: continue
    asrank_violation_data.append((customer_cone_sizes[int(asn)], violation)) 

with open("cipollino-verify/violation_asrank", "w") as fi:
    json.dump(asrank_violation_data, fi)

    
