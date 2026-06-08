import sys
import boto3
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from sliderule import sliderule, earthdata

# command line arguments
parser = argparse.ArgumentParser(description="""Kd Experiment""")
parser.add_argument('--name',       type=str,               default="kd_experiment")
parser.add_argument('--vcpus',      type=int,               default=4)
parser.add_argument('--memory',     type=int,               default=16000)
parser.add_argument('--script',     type=str,               default="utils/kd_experiment.lua")
parser.add_argument('--arg',        type=str,               default=None) # ATL03_20241107234251_08052501_007_01.h5,ATL09_20241107234251_08052501_007_01.h5
parser.add_argument('--args',       type=str,               default=None) # data/atl03_granules_cycle_1.txt
parser.add_argument('--slice',      type=int, nargs=2,      default=[0, 10000])
parser.add_argument('--submission', type=str,               default="/tmp/kd_experiment_submission.txt")
parser.add_argument('--outputs',    type=str,               default="/tmp/kd_experiment_outputs.txt")
parser.add_argument('--cycle',      type=int,               default=None) # 1
parser.add_argument('--results',    action='store_true',    default=False)
parser.add_argument('--submit',     action='store_true',    default=False)
parser.add_argument('--status',     action='store_true',    default=False)
args = parser.parse_args()

# create runner session and authentiate
session = sliderule.create_session(verbose=True)
session.authenticate() # gives privileges to access SlideRule Runner

# submit jobs
if args.submit:
    # get list of granules
    if args.arg:
        args_list = [args.arg]
    elif args.args:
        args_list = []
        with open(args.args, "r") as file:
            for arg in file.readlines():
                args_list.append(arg.strip())
    else:
        print("Error: must supply arguments to process")
        sys.exit(1)
    # submit jobs to runner
    args_list = args_list[args.slice[0]:args.slice[1]]
    print(f"Submitting job {args.name} using script {args.script} with {len(args_list)} entries")
    lua_script = open(args.script, "r").read()
    rsps = session.runner.submit(name=args.name, script=lua_script, args=args_list, optional_args={"vcpus":args.vcpus, "memory":args.memory})
    print(f"Saving job submission to {args.submission}", rsps)
    with open(args.submission, 'w') as file:
        file.write(f'{json.dumps(rsps, indent=2)}')

# create cycle file
if args.cycle:
    args_list = []
    print(f"Requesting ATL03 granules from CMR for cycle {args.cycle}")
    atl03_granules = sliderule.source("earthdata", {
        "asset": "icesat2",
        "cycle": args.cycle,
        "max_resources": 100000
    })
    print(f"Retrieved list of {len(atl03_granules)} granules to process")
    print(f"Requesting ATL09 granules from CMR for cycle {args.cycle}")
    atl09_granules = sliderule.source("earthdata", {
        "asset": "icesat2-atl09",
        "cycle": args.cycle,
        "max_resources": 100000
    })
    print(f"Retrieved list of {len(atl09_granules)} granules to process")
    atl09_table = {cycle: {} for cycle in range(100)}
    for granule in atl09_granules:
        rgt = int(granule[21:25])
        cycle = int(granule[25:27])
        atl09_table[cycle][rgt] = granule
    for granule in atl03_granules:
        rgt = int(granule[21:25])
        cycle = int(granule[25:27])
        try:
            atl09_granule = atl09_table[cycle][rgt]
            args_list.append(f"{granule},{atl09_granule}")
        except Exception as e:
            print(f"Skipping granule {granule}: {e}")
    local_cycle_file = f'data/atl03_granules_cycle_{args.cycle}.txt'
    print(f"Saving cycle {args.cycle} to file {local_cycle_file}")
    with open(local_cycle_file, 'w') as file:
        for arg in args_list:
            file.write(f'{arg}\n')

# status jobs
if args.status:
    # get progress of submitted jobs
    with open(args.submission) as file:
        submission = json.loads(file.read())
    jobs_in_progress = session.runner.queue(name=submission["name"], job_id=submission["job_id"])
    print(json.dumps(jobs_in_progress["report"], indent=2))

# run results
if args.results:
    outputs = []
    with open(args.submission) as file:
        submission = json.loads(file.read())
    s3 = boto3.client("s3", region_name="us-west-2")
    def load_remote_file(bucket, key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        contents = obj["Body"].read().decode("utf-8")
        return json.loads(contents)
    bucket = submission["run_url"].split("s3://")[-1].split("/")[0]
    prefix = "/".join(submission["run_url"].split("s3://")[-1].split("/")[1:])
    results = load_remote_file(bucket, f"{prefix}/receipt.json") # {"name": ..., "username": ... "args": <path to arg file>, "environment": ...}
    args_list = load_remote_file(bucket, results["args"])
    for i in range(len(args_list)):
        try:
            result = load_remote_file(bucket, f"{prefix}/result{i}.json")
            output = "output" in result and result["output"] or result
            outputs.append(output)
            print(f"{args_list[i]} =>", json.dumps(result, indent=2))
        except Exception as e:
            print(f"{i} =>", {e})
    print(f"Saving outputs to {args.outputs}")
    with open(args.outputs, 'w') as file:
        for output in outputs:
            file.write(f'{output}\n')


############################################################################################
# session = sliderule.create_session(domain="localhost", cluster=None, verbose=True)
#
# gdf = sliderule.run("atl24kd", {
#     "atl09_fields": ["low_rate/met_v10m", "low_rate/met_u10m"],
#     "output": {
#         "format": "geoparquet",
#         "path": "ATL03_20241107234251_08052501_007_01_kd_v4.parquet",
#         "open_on_complete": True
#     }
# }, resources=["ATL03_20241107234251_08052501_007_01.h5"], session=session)
#
# print(gdf)
