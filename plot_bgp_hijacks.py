#!/usr/bin/python
import numpy as np
import os
import glob
import json
import pdb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
#import seaborn as sns
MMT_PATH = "cipollino-verify/"
files = filter(os.path.isfile, glob.glob( MMT_PATH + "*"))
hijack_numbers = {}
for fi in files:
    if "hijack_potential" in fi:
        with open(fi) as fd:
            hijack_data = json.load(fd)
            for asn, percent in hijack_data.iteritems():
                if asn in hijack_numbers:
                    hijack_numbers[asn].append(percent)
                else:
                    hijack_numbers[asn] = [percent]
num_bins = 20
fig, ax = plt.subplots()
for asn, data in hijack_numbers.iteritems():
    print asn
    counts, bin_edges = np.histogram(data, bins=num_bins)
    cdf = np.cumsum(counts)
    
    # And finally plot the cdf
    ax.plot(bin_edges[1:], cdf, label=str(asn))

#plt.legend(loc="upper right", fontsize=8)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
          fancybox=True, shadow=True, ncol=7, fontsize=6)
plt.savefig("hijacks_feasibility.pdf", bbox_inches='tight')
pdb.set_trace()

