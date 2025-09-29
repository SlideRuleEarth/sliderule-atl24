import argparse
import os
import time
import threading
import queue
import sys
import json
import boto3
import pandas as pd
from sliderule import sliderule

# Command Line Arguments
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--summary_file',       type=str,               default="data/atl24_v2_granule_collection.csv")
parser.add_argument('--error_file',         type=str,               default="data/atl24_v2_granule_errors.txt")
parser.add_argument('--url',                type=str,               default="s3://sliderule-public")
parser.add_argument('--domain',             type=str,               default="slideruleearth.io")
parser.add_argument('--organization',       type=str,               default="developers")
parser.add_argument('--concurrency',        type=int,               default=None)
parser.add_argument('--startup_separation', type=int,               default=1) # seconds
parser.add_argument('--report_only',        action='store_true',    default=False)
parser.add_argument('--test',               action='store_true',    default=False)
parser.add_argument('--reprocess_errors',   action='store_true',    default=False)
parser.add_argument("--release" ,           type=str,               default="002")
parser.add_argument("--version" ,           type=str,               default="01")
args,_ = parser.parse_known_args()

# Initialize Organization
if args.organization == "None":
    args.organization = None

# Create Multithreaded Control
lock = threading.Lock()
initial_summary_write = True

# Initialize Lists
granules_already_processed = {} # [name]: True
granules_to_process = [] # (name, size)
granules_in_error = {} # [name]: True

# ################################################
# Open Summary File
# ################################################

# get existing granules
try:
    summary_df = pd.read_csv(args.summary_file)
    initial_summary_write = False
    for granule in summary_df["granule"]:
        granules_already_processed[granule] = True
    summary_file = open(args.summary_file, "a")
except:
    summary_file = open(args.summary_file, "w")

# get granules in error
if os.path.exists(args.error_file):
    with open(args.error_file, "r") as file:
        for line in file.readlines():
            granules_in_error[line.strip()] = True
error_file = open(args.error_file, "w")

# handle granules in error
for granule in granules_in_error:
    if args.reprocess_errors:
        # remove granules in error from granules already processed (so that they are reprocessed)
        if granule in granules_already_processed:
            del granules_already_processed[granule]
    else:
        # add them to the error file so that they are not lost
        error_file.write(f'{granule}\n')
        error_file.flush()

# report number of existing granules
print(f'Granules Already Processed: {len(granules_already_processed)}')
print(f'Granules In Error: {len(granules_in_error)}')

# ################################################
# List H5 Granules
# ################################################

if not args.test:
    # initialize s3 client
    s3 = boto3.client('s3')

    # get bucket and subfolder from url
    path = args.url.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])

    # read granules
    is_truncated = True
    continuation_token = None
    while is_truncated:
        # make request
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder)
        # display status
        print("#", end='')
        sys.stdout.flush()
        # parse contents
        if 'Contents' in response:
            for obj in response['Contents']:
                granule = obj['Key'].split("/")[-1]
                if granule.startswith("ATL24") and granule.endswith(args.release + "_" + args.version + ".h5"):
                    if granule not in granules_already_processed:
                        granules_to_process.append((granule, obj["Size"]))
        # check if more data is available
        is_truncated = response['IsTruncated']
        continuation_token = response.get('NextContinuationToken')
    print("") # new line
else:
    granules_to_process = ['ATL24_20181120020325_08010106_006_02_002_01.h5']

# report new granules
print(f'Granules Left to Process: {len(granules_to_process)}')

# ################################################
# Configure Processing
# ################################################

# Initialize Concurrent Requests
rqst_q = queue.Queue()
if args.concurrency != None:
    concurrent_rqsts = args.concurrency
else:
    concurrent_rqsts = 1

# Queue Processing Requests
for granule in granules_to_process:
    if "#" not in granule:
        rqst_q.put(granule)

# Check for Early Exit
if args.report_only:
    sys.exit(0)

# Initialize Python Client
sliderule.init(args.domain, verbose=False, organization=args.organization, rethrow=True)

# ################################################
# Thread Processing
# ################################################

# Worker Thread
def worker(worker_id):

    global args, initial_summary_write

    # While Queue Not Empty
    complete = False
    while not complete:

        # Get Request
        try:
            granule = rqst_q.get(block=False)
        except Exception as e:
            # Handle No More Requests
            if rqst_q.empty():
                complete = True
            else:
                print(f'<{worker_id}> exception: {e}')
                time.sleep(5) # prevents a spin
            continue

        # Process Request
        try:
            parms = {
                "image": "atl24d:latest",
                "command": "/env/bin/python /runner.py", # <input file> <output file>
                "parms": {
                    "function": "metadata",
                    "granule": granule,
                    "url": args.url
                }
            }
            rsps = sliderule.source("execre", parms)
            if rsps["status"]:
                with lock:
                    if initial_summary_write:
                        initial_summary_write = False
                        summary_file.write(rsps["result"])
                    else:
                        result = rsps["result"].split("\n")
                        summary_file.write('\n'.join(result[1:])) # removes csv header line
                    summary_file.flush()
                print(f'Processed: {granule}')
            else:
                raise RuntimeError(f'{rsps["result"]}')
        except Exception as e:
            print(f'Error: {granule} - {e}')
            if not args.test:
                with lock:
                    error_file.write(f'{granule}\n')
                    error_file.flush()
            time.sleep(5) # slow it down

# Start Workers
threads = [threading.Thread(target=worker, args=(worker_id,), daemon=True) for worker_id in range(concurrent_rqsts)]
for thread in threads:
    thread.start()
    time.sleep(args.startup_separation)
for thread in threads:
    thread.join()
print('all processing requests completed')
