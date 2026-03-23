import argparse
import threading
import queue
import sys
from contextlib import suppress
import boto3

# Command Line Arguments
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--source',             type=str,               default="s3://sliderule/data/ATL24")
parser.add_argument('--destination',        type=str,               default="s3://sliderule/data/ATL24r2")
parser.add_argument('--cache',              type=str,               default="data/xml_cache.txt")
parser.add_argument('--concurrency',        type=int,               default=1)
parser.add_argument("--release" ,           type=str,               default="002")
parser.add_argument("--version" ,           type=str,               default="01")
parser.add_argument('--report_only',        action='store_true',    default=False)
args,_ = parser.parse_known_args()

# Display Helper Function
def display(s):
    sys.stdout.write(s)
    sys.stdout.flush()

# Parse URL Helper Function
def parse_url(url):
    path = url.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])
    return bucket, subfolder

# List Bucket Helper Function
def list_bucket(bucket, subfolder, cache):
    filenames = {}
    if cache: # use local cache file
        with suppress(FileNotFoundError):
            with open(cache, "r") as file:
                for filename in file.readlines():
                    filenames[filename.strip()] = True
                print(f"Using cache file <{cache}> with {len(filenames)} entries")
    if len(filenames) == 0: # no files were cached
        # enumerate objects in s3 bucket
        is_truncated = True
        continuation_token = None
        while is_truncated:
            # make request
            if continuation_token:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=subfolder, ContinuationToken=continuation_token)
            else:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=subfolder)
            # display status
            display("#")
            # parse contents
            if 'Contents' in response:
                for obj in response['Contents']:
                    resource = obj['Key'].split("/")[-1]
                    if resource.startswith("ATL24") and resource.endswith(".iso.xml"):
                        filenames[resource] = True
            # check if more data is available
            is_truncated = response['IsTruncated']
            continuation_token = response.get('NextContinuationToken')
        display("\n")
        if cache: # write files out to cache
            with open(cache, "w") as file:
                for filename in filenames:
                    file.write(f"{filename}\n")
    # return filenames
    return filenames

# Initialize Globals
rqst_q = queue.Queue()
s3_client = boto3.client("s3")
source_bucket, source_subfolder = parse_url(args.source)
destination_bucket, destination_subfolder = parse_url(args.destination)
xml_files_available_to_process = list_bucket(source_bucket, source_subfolder, args.cache)
xml_files_already_processed = list_bucket(destination_bucket, destination_subfolder, None)

# Get List of XML Files to Process
xml_files_to_process = {}
for filename in xml_files_available_to_process:
    new_filename = filename.replace("_001_01.iso.xml", f"_{args.release}_{args.version}.iso.xml")
    if new_filename not in xml_files_already_processed:
        xml_files_to_process[filename] = True

# Display Parameters
print(f'XML Files Already Processed:    {len(xml_files_already_processed)}')
print(f'XML Files Available to Process: {len(xml_files_available_to_process)}')
print(f'XML Files Left to Process:      {len(xml_files_to_process)}')
if args.report_only:
    sys.exit(0)

# Queue Processing Requests
for filename in list(xml_files_to_process):
    rqst_q.put(filename)

# Worker Thread Function
def worker(worker_id):
    try:
        while True:
            filename = rqst_q.get(block=False)
            print(f'<{worker_id}> reading s3://{source_bucket}/{source_subfolder}/{filename}')
            response = s3_client.get_object(Bucket=source_bucket, Key=f"{source_subfolder}/{filename}")
            text = response["Body"].read().decode("utf-8")
            new_filename = filename.replace("_001_01.iso.xml", f"_{args.release}_{args.version}.iso.xml")
            old_granule = filename.replace(".iso.xml", f".h5")
            new_granule = filename.replace("_001_01.iso.xml", f"_{args.release}_{args.version}.h5")
            new_text = text.replace(old_granule, new_granule).replace("<gco:CharacterString>001</gco:CharacterString>", f"<gco:CharacterString>{args.release}</gco:CharacterString>")
            s3_client.put_object(Bucket=destination_bucket, Key=f"{destination_subfolder}/{new_filename}", Body=new_text.encode("utf-8"), ContentType="text/plain")
            print(f'<{worker_id}> writing s3://{destination_bucket}/{destination_subfolder}/{new_filename}')
    except Exception as e:
        if not rqst_q.empty():
            raise

# Start Workers
threads = [threading.Thread(target=worker, args=(worker_id,), daemon=True) for worker_id in range(args.concurrency)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
