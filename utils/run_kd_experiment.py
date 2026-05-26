import sys
import boto3
import json
import argparse
import geopandas as gpd
from sliderule import sliderule

# command line arguments
parser = argparse.ArgumentParser(description="""Kd Experiment""")
parser.add_argument('--name',       type=str,               default="kd_experiment")
parser.add_argument('--script',     type=str,               default="utils/kd_experiment.lua")
parser.add_argument('--granule',    type=str,               default=None) # "ATL03_20241107234251_08052501_007_01.h5"
parser.add_argument('--granules',   type=str,               default=None) # "data/atl03_granules_cycle_1.txt"
parser.add_argument('--cycle',      type=int,               default=None) # 1
parser.add_argument('--run',        type=str,               default=None) # "run url"
parser.add_argument('--submit',     action='store_true',    default=False)
parser.add_argument('--status',     action='store_true',    default=False)
args,_ = parser.parse_known_args()

# create runner session and authentiate
session = sliderule.create_session(verbose=True)
session.authenticate() # gives privileges to access SlideRule Runner

# submit jobs
if args.submit:
    # get list of granules
    if args.granule:
        granules = [args.granule]
    elif args.granules:
        granules = []
        with open(args.granules, "r") as file:
            for granule in file.readlines():
                granules.append(granule.strip())
    elif args.cycle:
        parms = {
            "asset": "icesat2",
            "cycle": args.cycle,
            "max_resources": 100000
        }
        granules = sliderule.source("earthdata", parms)
        local_cache_file = f'data/atl03_granules_cycle_{args.cycle}.txt'
        print(f"Saving off cycle {args.cycle} to file {local_cache_file}")
        with open(local_cache_file, 'w') as file:
            for granule in granules:
                file.write(f'{granule}\n')
    else:
        print("Error: must supply granules to process")
        sys.exit(1)
    # submit jobs to runner
    print(f"Submitting job {args.name} using script {args.script} with {len(granules)} entries")
    lua_script = open(args.script, "r").read()
    rsps = session.runner.submit(name=args.name, script=lua_script, args_list=granules, optional_args={"vcpus":4, "memory":32768})
    print("Submitted jobs!\n", rsps)

# status jobs
if args.status:
    # get progress of submitted jobs
    jobs_in_progress = session.runner.queue(job_state=["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED"])
    print(json.dumps(jobs_in_progress, indent=2))

# run results
if args.run:
    # create s3 session
    s3 = boto3.client("s3", region_name="us-west-2")
    # list contents of an s3 bucket
    def list_bucket(url):
        filenames = []
        bucket = url.split("s3://")[-1].split("/")[0]
        prefix = "/".join(url.split("s3://")[-1].split("/")[1:])
        is_truncated = True
        continuation_token = None
        while is_truncated:
            # make request
            if continuation_token:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            # parse contents
            if 'Contents' in response:
                for obj in response['Contents']:
                    filenames.append(f"{bucket}/{obj['Key']}")
            # check if more data is available
            is_truncated = response['IsTruncated']
            continuation_token = response.get('NextContinuationToken')
        return filenames
    # download and display run artifacts
    filenames = list_bucket(args.run)
    for filename in filenames:
        bucket = filename.split("/")[0]
        key = "/".join(filename.split("/")[1:])
        local_file = f"/tmp/{filename.split("/")[-1]}"
        print(f"\ndownloading s3://{filename} to {local_file}")
        s3.download_file(bucket, key, local_file)
        print(f"contents of {local_file}:")
        with open(local_file, "r") as file:
            contents = file.read()
            print(contents)


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
