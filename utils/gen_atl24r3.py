import importlib
import boto3
import json
import argparse
from sliderule import sliderule

try:
    tqdm = importlib.import_module("tqdm").tqdm
except Exception:
    print(f"tqdm unavailable, progress of operations will not be reported")
    def tqdm(iterable, **kwargs):
        return iterable

#########################################
# command line arguments
#########################################
parser = argparse.ArgumentParser(description="""Kd Experiment""")
parser.add_argument('--cycle',      type=int,               default=None) # 1, 2, 3, etc.
parser.add_argument('--vcpus',      type=int,               default=4)
parser.add_argument('--memory',     type=int,               default=16000)
parser.add_argument('--batch_size', type=int,               default=10000)
parser.add_argument('--script',     type=str,               default="utils/gen_atl24r3.lua")
parser.add_argument('--database',   type=str,               default="data/atl24r3_database.json")
parser.add_argument('--vset',       type=str,               default="data/atl24r3_verification_set.txt")
parser.add_argument('--verify',     action='store_true',    default=False)
parser.add_argument('--test',       action='store_true',    default=False)
parser.add_argument('--status',     action='store_true',    default=False)
parser.add_argument('--report',     action='store_true',    default=False)
args = parser.parse_args()

#########################################
# open database
#########################################
# {
#     "submissions": {
#         "<name>": {
#             "run_url": <run url>,
#             "job_id": <job id>,
#             "status": {
#                 "SUBMITTED": <x>,
#                 "PENDING": <x>,
#                 "RUNNABLE": <x>,
#                 "STARTING": <x>,
#                 "RUNNING": <x>,
#                 "SUCCEEDED": <x>,
#                 "FAILED": <x>
#             },
#             "complete": <true|false>
#         },
#         ...
#     },
#     "granules": {
#         "<ATL03 granule>": {
#             "job": <name> ,-- of job responsible for processing granule
#             "status": <"pending", "empty", "output", "error">, -- pending: has not run yet, empty: ran and produced no output, output: completed with results, error: failed to complete
#             "output": <url to output granule>, -- e.g. s3://sliderule-public/atl24_2026...parquet
#         }
#     }
# }
try:
    # read database
    with open(args.database, "r") as file:
        database = json.load(file)
except:
    # create database
    with open(args.database, "w") as file:
        database = {"submissions": {}, "granules": {}}
        json.dump(database, file)

#########################################
# create runner sessions and authentiate
#########################################
session = sliderule.create_session(verbose=True)
session.authenticate() # gives privileges to access SlideRule Runner

#########################################
# function: get argument list
#########################################
def get_args_list(cycle):

    print(f"Requesting ATL03 granules from CMR for cycle {cycle}")
    atl03_granules = sliderule.source("earthdata", {
        "asset": "icesat2",
        "cycle": cycle,
        "max_resources": 100000
    })
    print(f"Retrieved list of {len(atl03_granules)} granules to process")

    return [f"{granule}" for granule in atl03_granules]

#########################################
# function: get results
#########################################
def get_results(run_url):

    results = {}

    s3 = boto3.client("s3", region_name="us-west-2")
    def load_remote_file(bucket, key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        contents = obj["Body"].read().decode("utf-8")
        return json.loads(contents)
    bucket = run_url.split("s3://")[-1].split("/")[0]
    prefix = "/".join(run_url.split("s3://")[-1].split("/")[1:])
    rsps = load_remote_file(bucket, f"{prefix}/receipt.json") # {"name": ..., "username": ... "args": <path to arg file>, "environment": ...}
    args_list = load_remote_file(bucket, rsps["args"])

    for i in tqdm(range(len(args_list)), total=len(args_list), desc=f"{run_url}", unit="granule"):
        granule,_ = args_list[i].split(",")
        try:
            result = {}
            rsps = load_remote_file(bucket, f"{prefix}/result{i}.json")
            if not rsps["status"] or "output" not in rsps:
                result["status"] = "empty"
            else:
                result["status"] = "output"
                result["output"] = rsps["output"]
                result["duration"] = rsps["stop"] - rsps["start"]
        except Exception as e:
            result = {"status": "error"}
        results[granule] = result

    return results

#########################################
# process granules for verification
#########################################
if args.verify:

    # get granules from verification set
    with open(args.vset, "r") as file:
        lines = file.readlines()
        granules = [line.strip().replace("_006_01", "_006_02")+ ".h5" for line in lines if len(line) > 30]
        if args.test: # just pull out two granules to run (the current batch api requires at least two jobs to be submitted)
            granules = granules[1:3]

    # process granules in batches
    for i in range(0, len(granules), args.batch_size):

        # submit job
        name = f"atl24r3_vset_{i}"
        args_list = granules[i:i+args.batch_size]
        lua_script = open(args.script, "r").read()
        rsps = session.runner.submit(name=name, script=lua_script, args=args_list, optional_args={"vcpus":args.vcpus, "memory":args.memory})
        print(f"Submitted job {name} using script {args.script} with {len(args_list)} entries")

        # save job
        database["submissions"][name] = rsps | {"complete": False}
        print(f"Saved job submission", rsps)

        # save granules
        for granule in args_list:
            database["granules"][granule] = {"name": name, "status": "pending"}

#########################################
# process granules for cycle
#########################################
if args.cycle:

    # get all arguments for the cycle
    args_for_cycle = get_args_list(args.cycle)
    for i in range(0, len(args_for_cycle), args.batch_size):

        # submit job
        name = f"atl24r3_{args.cycle}_{i}"
        args_list = args_for_cycle[i:i+args.batch_size]
        lua_script = open(args.script, "r").read()
        rsps = session.runner.submit(name=name, script=lua_script, args=args_list, optional_args={"vcpus":args.vcpus, "memory":args.memory})
        print(f"Submitted job {name} using script {args.script} with {len(args_list)} entries")

        # save job
        database["submissions"][name] = rsps | {"complete": False}
        print(f"Saved job submission", rsps)

        # save granules
        for granule in args_list:
            database["granules"][granule] = {"name": name, "status": "pending"}

#########################################
# status jobs
#########################################
if args.status:

    # get status
    for name,job in database["submissions"].items():
        print(f"Statusing {name} - {job['complete'] and 'complete' or 'checking'}")
        if not job["complete"]:
            status = session.runner.queue(name=name, job_id=job["job_id"])["report"]
            database["submissions"][name]["status"] = status
            if sum([status[s] for s in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]]) == 0:
                print(f"Job {name} complete, reading results...")
                results = get_results(job["run_url"])
                for granule, result in tqdm(results.items(), total=len(results), desc=f"{name} results", unit="granule"):
                    if type(result) is dict:
                        database["granules"][granule] |= result
                    else:
                        print(f"Result for {granule}: {result}")
                database["submissions"][name]["complete"] = True

    # display status
    columns = ["SUCCEEDED", "FAILED", "RUNNING", "STARTING", "RUNNABLE", "PENDING", "SUBMITTED"]
    print(",".join([f"{c:>10}" for c in ["          NAME"] + columns]))
    for name,job in database["submissions"].items():
        print(",".join([f"{c:>10}" for c in [name] + [job["status"][state] for state in columns]]))

#########################################
# report on status of granules
#########################################
if args.report:

    stats = {
        "pending": 0,
        "empty": 0,
        "output": 0,
        "error": 0
    }
    for granule,data in database["granules"].items():
        try:
            stats[data["status"]] += 1
        except Exception as e:
            print(f"Unable to count {granule} with {data}: {e}")
    print(json.dumps(stats, indent=2))

#########################################
# save database
#########################################
with open(args.database, 'w') as file:
    json.dump(database, file)
