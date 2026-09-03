import importlib
import boto3
import json
import argparse
from sliderule import sliderule
from atl24r3_database import Database, Status

try:
    tqdm = importlib.import_module("tqdm").tqdm
except Exception:
    print(f"tqdm unavailable, progress of operations will not be reported")
    def tqdm(iterable, **kwargs):
        return iterable

#########################################
# command line arguments
#########################################

parser = argparse.ArgumentParser(description="""ATL24 Platinum Run""")
parser.add_argument('--database',   type=str,   default="data/atl24r3_database.json")
args = parser.parse_args()

#########################################
# open database
#########################################

database = Database(args.database)

#########################################
# create runner sessions and authentiate
#########################################

session = sliderule.create_session(verbose=True)
session.authenticate() # gives privileges to access SlideRule Runner

#########################################
# status jobs
#########################################

# s3 client
s3 = boto3.client("s3", region_name="us-west-2")

# local function: load remote file from s3
def load_remote_file(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    contents = obj["Body"].read().decode("utf-8")
    return json.loads(contents)

# local function: get results for a job run
def get_results(run_url):
    results = {}
    bucket = run_url.split("s3://")[-1].split("/")[0]
    prefix = "/".join(run_url.split("s3://")[-1].split("/")[1:])
    rsps = load_remote_file(bucket, f"{prefix}/receipt.json") # {"name": ..., "username": ... "args": <path to arg file>, "environment": ...}
    args_list = load_remote_file(bucket, rsps["args"])
    for i in tqdm(range(len(args_list)), total=len(args_list), desc=f"{run_url}", unit="granule"):
        try:
            rsps = load_remote_file(bucket, f"{prefix}/result{i}.json")
            result = {
                "status": rsps["status"] and Status.OUTPUT or Status.EMPTY,
                "duration": rsps["status"] and (rsps["stop"] - rsps["start"]) or 0.0,
                "rsps": rsps
            }
        except Exception as e:
            result = {"status": Status.ERROR}
        granule = args_list[i]
        results[granule] = result
    return results

# get status
for name,job in database.submissions.items():
    print(f"Statusing {name} - {job['complete'] and 'complete' or 'checking'}")
    if not job["complete"]:
        status = session.runner.queue(name=name, job_id=job["job_id"])["report"]
        database.submissions[name]["status"] = status
        if sum([status[s] for s in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]]) == 0:
            print(f"Job {name} complete, reading results...")
            results = get_results(job["run_url"])
            for granule, result in tqdm(results.items(), total=len(results), desc=f"{name} results", unit="granule"):
                if type(result) is dict:
                    database.granules[granule] |= result
                else:
                    print(f"Result for {granule}: {result}")
            database.submissions[name]["complete"] = True

# display status
columns = ["SUCCEEDED", "FAILED", "RUNNING", "STARTING", "RUNNABLE", "PENDING", "SUBMITTED"]

print(",".join([f"{c:>30}" for c in ["NAME"]] + [f"{c:>10}" for c in columns]))
for name,job in database.submissions.items():
    print(",".join([f"{c:>30}" for c in [name]] + [f"{c:>10}" for c in [job["status"][state] for state in columns]]))

#########################################
# report on status of granules
#########################################

stats = {status.value: 0 for status in Status}
duration = {"avg": 0.0, "total": 0.0}
processed = 0
pending = 0
for granule,data in database.granules.items():
    try:
        stats[data["status"]] += 1
        duration["total"] += data["duration"]
        processed += 1
    except Exception as e:
        pending += 1
duration["avg"] = duration["total"] / processed
print("Processed:", processed)
print("Pending:", pending)
print("States:", json.dumps(stats, indent=2))
print("Duration:", json.dumps(duration, indent=2))

#########################################
# save database
#########################################

database.write()
