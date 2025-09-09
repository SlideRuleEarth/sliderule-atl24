from sliderule import sliderule, icesat2
import argparse
import time
import threading
import queue
import logging
import sys
import glob
import boto3

# Command Line Arguments
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--domain',             type=str,               default="slideruleearth.io")
parser.add_argument('--organization',       type=str,               default="developers")
parser.add_argument('--desired_nodes',      type=int,               default=None)
parser.add_argument('--time_to_live',       type=int,               default=600) # 10 hours
parser.add_argument('--log_file',           type=str,               default="atl24v2.log")
parser.add_argument('--concurrency',        type=int,               default=50)
parser.add_argument('--startup_separation', type=int,               default=2) # seconds
parser.add_argument('--input_file',         type=str,               default="data/atl24_granules_cycle_1.txt")
parser.add_argument('--input_files',        type=str,               default=None) # glob support
parser.add_argument('--url',                type=str,               default="s3://sliderule-public")
parser.add_argument('--retries',            type=int,               default=3)
parser.add_argument('--report_only',        action='store_true',    default=False)
parser.add_argument('--test',               action='store_true',    default=False)
args,_ = parser.parse_known_args()

# Initialize Constants
RELEASE = "002"
VERSION = "01"

# Initialize Organization
if args.organization == "None":
    args.organization = None
    args.desired_nodes = None
    args.time_to_live = None

# Setup Log File
LOG_FORMAT = '%(created)f %(levelname)-5s [%(filename)s:%(lineno)5d] %(message)s'
log        = logging.getLogger(__name__)
logfmt     = logging.Formatter(LOG_FORMAT)
logfile    = logging.FileHandler(args.log_file)
logfile.setFormatter(logfmt)
log.addHandler(logfile)
log.setLevel(logging.INFO)
logfile.setLevel(logging.INFO)

# Initialize Concurrent Requests
rqst_q = queue.Queue()
if args.test:
    concurrent_rqsts = 1
if args.concurrency != None:
    concurrent_rqsts = args.concurrency
elif args.desired_nodes != None:
    concurrent_rqsts = args.desired_nodes
else:
    concurrent_rqsts = 1

# Creat S3 Client
s3_client = boto3.client("s3")

# Get List of Input Files
if args.input_files == None:
    list_of_input_files = [args.input_file]
else:
    list_of_input_files = glob.glob(args.input_files)

# gGet Bucket and Subfolder from URL
path = args.url.split("s3://")[-1]
bucket = path.split("/")[0]
subfolder = '/'.join(path.split("/")[1:])

# Get List of Granules from S3 Bucket
existing_granules = []
is_truncated = True
continuation_token = None
while is_truncated:
    # make request
    if continuation_token:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=subfolder, ContinuationToken=continuation_token)
    else:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=subfolder)
    # display status
    print("#", end='')
    sys.stdout.flush()
    # parse contents
    if 'Contents' in response:
        for obj in response['Contents']:
            resource = obj['Key'].split("/")[-1]
            if resource.startswith("ATL24") and resource.endswith(".h5"):
                existing_granules.append(resource.replace(RELEASE + "_" + VERSION + ".h5", "001_01.h5"))
    # check if more data is available
    is_truncated = response['IsTruncated']
    continuation_token = response.get('NextContinuationToken')
print('') # new line

# Open and Read Input File (containing list of granules to process)
granules_to_process = []
granules_already_processed = []
for input_file in list_of_input_files:
    with open(input_file, "r") as file:
        for line in file.readlines():
            granule = line.strip()
            if granule in existing_granules:
                granules_already_processed.append(granule)
            else:
                granules_to_process.append(granule)

# Display Parameters
print(f'Granules Already Processed   {len(granules_already_processed)}')
print(f'Granules Left to Process:    {len(granules_to_process)}')

# Execute Test
if args.test:
    granules_to_process = ['ATL24_20181120020325_08010106_006_02_001_01.h5']

# Queue Processing Requests
for granule in granules_to_process:
    if "#" not in granule:
        rqst_q.put(granule)

# Check for Early Exit
if args.report_only:
    sys.exit(0)

# Initialize Python Client
sliderule.init(
    args.domain,
    verbose=False,
    loglevel=logging.INFO,
    organization=args.organization,
    desired_nodes=args.desired_nodes,
    time_to_live=args.time_to_live,
    rethrow=True,
    log_handler=logfile)

#
# Worker
#
def worker(worker_id):

    # While Queue Not Empty
    complete = False
    while not complete:

        # Get Request
        try:
            granule = rqst_q.get(block=False)
        except Exception as e:
            # Handle No More Requests
            if rqst_q.empty():
                log.info(f'<{worker_id}> no more requests {e}')
                complete = True
            else:
                log.info(f'<{worker_id}> exception: {e}')
                time.sleep(5) # prevents a spin
            continue

        # Process Request
        status = "Unknown"
        attempts = args.retries
        while (status != "Pass") and attempts > 0:
            attempts -= 1
            try:
                log.info(f'<{worker_id}> processing granule {granule} ...')
                parms = {
                    "resource": granule,
                    "output": {
                        "asset": "sliderule-stage",
                        "path": granule[:-10] + "_" + RELEASE + "_" + VERSION + ".h5",
                        "format": "h5",
                        "open_on_complete": False,
                        "with_checksum": False,
                    }
                }
                if args.test:
                    parms["output"]["asset"] = None
                    parms["output"]["path"] = "/tmp/" + granule[:-10] + "_" + RELEASE + "_" + VERSION + ".h5"
                rsps = sliderule.source("atl24g2", {"parms": parms}, stream=True)
                outfile = sliderule.procoutputfile(parms, rsps)
                log.info(f'<{worker_id}> finished granule {outfile}')
                status = "Pass"
            except Exception as e:
                log.critical(f'failure on {granule}: {e}')
                status = f'Error - {e}'
                time.sleep(30) # slow it down

        # Display Status
        print(f'{granule}: {status}')

# Start Workers
threads = [threading.Thread(target=worker, args=(worker_id,), daemon=True) for worker_id in range(concurrent_rqsts)]
for thread in threads:
    thread.start()
    time.sleep(args.startup_separation)
for thread in threads:
    thread.join()
log.info('all processing requests completed')
