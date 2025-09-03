import argparse
from sliderule import sliderule

parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--cycles', type=int, nargs='+', default=[])
parser.add_argument('--max_resources', type=int, default=100000)
parser.add_argument('--output_dir', type=str, default="data")
args,_ = parser.parse_known_args()

for cycle in args.cycles:
    parms = {
        "asset": "icesat2-atl24",
        "cycle": cycle,
        "max_resources": args.max_resources
    }
    granules = sliderule.source("earthdata", parms)
    with open(f'{args.output_dir}/atl24_granules_cycle_{cycle}.txt', 'w') as file:
        for granule in granules:
            file.write(f'{granule}\n')
