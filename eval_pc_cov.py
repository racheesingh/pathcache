import json
import pdb
top5_cc = ['DE', 'US', 'NO', 'BR', 'FR']

for count in [10, 20, 30, 40, 50]:
    with open("cipollino-verify/pc_coverage_country_alexa_fw_only_%s" % count) as fi:
        country_cov = json.load( fi)
    sorted_cc = sorted(country_cov.items(), key=lambda x: x[1], reverse=True)
    print sorted_cc[:10]
    print count
    print [round(country_cov[x],3) for x in top5_cc]
pdb.set_trace()
