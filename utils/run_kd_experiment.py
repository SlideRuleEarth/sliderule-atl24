import sys
import boto3
import json
import argparse
from sliderule import sliderule, earthdata

# command line arguments
parser = argparse.ArgumentParser(description="""Kd Experiment""")
parser.add_argument('--name',       type=str,               default="kd_experiment")
parser.add_argument('--script',     type=str,               default="utils/kd_experiment.lua")
parser.add_argument('--arg',        type=str,               default=None) # ATL03_20241107234251_08052501_007_01.h5,ATL09_20241107234251_08052501_007_01.h5
parser.add_argument('--args',       type=str,               default=None) # data/atl03_granules_cycle_1.txt
parser.add_argument('--submission', type=str,               default="data/kd_experiment_submission.txt")
parser.add_argument('--outputs',    type=str,               default="data/kd_experiment_outputs.txt")
parser.add_argument('--cycle',      type=int,               default=None) # 1
parser.add_argument('--results',    type=str,               default=None) # "run url"
parser.add_argument('--submit',     action='store_true',    default=False)
parser.add_argument('--status',     action='store_true',    default=False)
args,_ = parser.parse_known_args()

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
    print(f"Submitting job {args.name} using script {args.script} with {len(args_list)} entries")
    lua_script = open(args.script, "r").read()
    rsps = session.runner.submit(name=args.name, script=lua_script, args_list=args_list, optional_args={"vcpus":4, "memory":32768})
    print(f"Saving job submission to {args.submission}", rsps)
    with open(args.submission, 'w') as file:
        file.write(f'{json.dumps(rsps, indent=2)}')

# create cycle file
if args.cycle:
    args_list = []
    parms = {
        "asset": "icesat2",
        "cycle": args.cycle,
        "max_resources": 100000
    }
    granules = sliderule.source("earthdata", parms)
    for granule in granules:
        rgt = int(granule[21:25])
        cycle = int(granule[25:27])
        name_filter = f'*_{rgt:04d}{cycle:02d}??_*'
        atl09_parms = {
            "asset": "icesat2-atl09",
            "name_filter": name_filter
        }
        granule09 = earthdata.search(atl09_parms)
        if len(granule09) > 0:
            arg = f"{granule},{granule09[0]}"
            args_list.append(arg)
            print(f"Appending {arg}")
        else:
            print(f"Skipping {granule}")
    local_cycle_file = f'data/atl03_granules_cycle_{args.cycle}.txt'
    print(f"Saving cycle {args.cycle} to file {local_cycle_file}")
    with open(local_cycle_file, 'w') as file:
        for arg in args_list:
            file.write(f'{arg}\n')

# status jobs
if args.status:
    # get progress of submitted jobs
    jobs_in_progress = session.runner.queue(job_name=args.name)
    print(json.dumps(jobs_in_progress, indent=2))

# run results
if args.results:
    outputs = []
    s3 = boto3.client("s3", region_name="us-west-2")
    def load_remote_file(bucket, key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        contents = obj["Body"].read().decode("utf-8")
        return json.loads(contents)
    bucket = args.results.split("s3://")[-1].split("/")[0]
    prefix = "/".join(args.results.split("s3://")[-1].split("/")[1:])
    results = load_remote_file(bucket, f"{prefix}/receipt.json") # {"name": ..., "username": ... "args": {"0": ..., ...}, "environment": ...}
    for item in results["args"]:
        try:
            result = load_remote_file(bucket, f"{prefix}/result_{item}.json")
            output = "output" in result and result["output"] or result
            outputs.append(output)
            print(f"{item} =>", json.dumps(result, indent=2))
        except Exception as e:
            print(f"{item} =>", {e})
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
