import argparse
import sys
from contextlib import suppress
import boto3
import hashlib
import json
import uuid
from datetime import datetime

# ###############################
# Globals
# ###############################

# Command Line Arguments
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--source',                 type=str,               default="s3://sliderule/data/ATL24r2")
parser.add_argument('--xml_cache',              type=str,               default="data/xml_cache.txt")
parser.add_argument('--h5_cache',               type=str,               default="data/h5_cache.txt")
parser.add_argument('--cnm_cache',              type=str,               default="data/cnm_cache.txt")
parser.add_argument('--collection',             type=str,               default="ATL24")
parser.add_argument('--provider',               type=str,               default="ICESat-2_sliderule")
parser.add_argument('--response_stream_arn',    type=str,               default="arn:aws:kinesis:us-west-2:790840705381:stream/nsidc-ops-prod-ATLAS_sliderule_response") # "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response"
parser.add_argument('--assume_role',            type=str,               default="arn:aws:iam::790840705381:role/nsidc-ops-prod_cross_provider_kinesis_role") # "arn:aws:iam::024284894447:role/nsidc-ops-uat_cross_provider_kinesis_role"
parser.add_argument('--notification_stream',    type=str,               default="nsidc-ops-prod-ATLAS_sliderule_notification") # "nsidc-ops-uat-ATLAS_sliderule_notification"
parser.add_argument('--data_version',           type=str,               default="002")
parser.add_argument('--partition_key',          type=str,               default="SlideRule")
parser.add_argument('--transfer',               type=int,               default=0) # must provide in order to actually transfer
parser.add_argument('--verbose',                action='store_true',    default=False)
args,_ = parser.parse_known_args()

# create s3 client
s3 = boto3.client("s3")

# ###############################
# Helper Functions
# ###############################

# Display Raw
def display(s):
    sys.stdout.write(s)
    sys.stdout.flush()

# Parse URL into Bucket and Subfolder
def parse_url(url):
    path = url.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])
    return bucket, subfolder

# Read Filenames from Cache
def read_cache(cache):
    data = {}
    with suppress(FileNotFoundError):
        with open(cache, "r") as file:
            data = json.loads(file.read())
            print(f"Found cache file <{cache}> with {len(data)} entries")
    return data

# Write Filenames to Cache
def write_cache(cache, data):
    with open(cache, "w") as file:
        file.write(json.dumps(data))

# List Files in Bucket
def list_bucket(bucket, subfolder):
    # read cache
    xml_filenames = read_cache(args.xml_cache)
    h5_filenames = read_cache(args.h5_cache)
    if len(h5_filenames) == 0 or len(xml_filenames) == 0: # no files were cached
        # clear cache
        print(f"Clearing cache and listing contents of {args.source}")
        xml_filenames = {}
        h5_filenames = {}
        # enumerate objects in s3 bucket
        is_truncated = True
        continuation_token = None
        while is_truncated:
            # make request
            if continuation_token:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder, ContinuationToken=continuation_token)
            else:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder)
            # display status
            display("#")
            # parse contents
            if 'Contents' in response:
                for obj in response['Contents']:
                    resource = obj['Key'].split("/")[-1]
                    if resource.startswith(args.collection):
                        if resource.endswith(".iso.xml"):
                            xml_filenames[resource] = obj['Size']
                        elif resource.endswith(".h5"):
                            h5_filenames[resource] = obj['Size']
            # check if more data is available
            is_truncated = response['IsTruncated']
            continuation_token = response.get('NextContinuationToken')
        display("\n")
        # write cache
        write_cache(args.xml_cache, xml_filenames)
        write_cache(args.h5_cache, h5_filenames)
    # return filenames
    return xml_filenames, h5_filenames

# Calculate Checksum
def calc_checksum(bucket, key):
    if args.verbose:
        print(f"Calculating checksum for s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket,Key=key)
    sha256 = hashlib.sha256()
    for chunk in iter(lambda: response["Body"].read(1024 * 1024), b""):
        sha256.update(chunk)
    return sha256.hexdigest()

# ###############################
# Main
# ###############################

# get granules to transfer
bucket, subfolder = parse_url(args.source)
xml_file_sizes, h5_file_sizes = list_bucket(bucket, subfolder)
cnm_granules = read_cache(args.cnm_cache)
granules_to_transfer = {}
for h5_filename in h5_file_sizes:
    granule = h5_filename.split(".h5")[0]
    xml_filename = f"{granule}.iso.xml"
    if (xml_filename in xml_file_sizes) and (granule not in cnm_granules):
        granules_to_transfer[granule] = {
            "xml": {
                "size": xml_file_sizes[xml_filename],
                "checksum": None # to be caclulated just prior to transfer
            },
            "h5": {
                "size": h5_file_sizes[h5_filename],
                "checksum": None # to be caclulated just prior to transfer
            }
        }

# Post records to stream
records_to_transfer = len(granules_to_transfer)
records_transfered = 0
records_success = 0
records_failure = 0
for granule in granules_to_transfer:

    # Get kinesis client by assuming the role and getting credentials
    if records_transfered % 1000 == 0:
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(RoleArn=args.assume_role, RoleSessionName='AssumeRoleSession')
        credentials = assumed_role['Credentials'] # get temporary credentials
        kinesis_client = boto3.client(
            'kinesis',
            region_name='us-west-2',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'])

    # Build record to post
    granules_to_transfer[granule]["xml"]["checksum"] = cnm_granules.get(granule, {}).get('xml', {}).get('checksum') or calc_checksum(bucket, f"{subfolder}/{granule}.iso.xml")
    granules_to_transfer[granule]["h5"]["checksum"] = cnm_granules.get(granule, {}).get('xml', {}).get('checksum') or calc_checksum(bucket, f"{subfolder}/{granule}.h5")
    record = {
        "version": 1.3,
        "submissionTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000000"),
        "identifier": str(uuid.uuid4()),
        "collection": args.collection,
        "provider": args.provider,
        "responseStreamArn": args.response_stream_arn,
        "product": {
            "name": f"{granule}.h5",
            "dataVersion": args.data_version,
            "files": [
                {
                    "name": f"{granule}.iso.xml",
                    "type": "metadata",
                    "uri": f"{args.source}/{granule}.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": granules_to_transfer[granule]["xml"]["checksum"],
                    "size": xml_file_sizes[f"{granule}.iso.xml"],
                },
                {
                    "name": f"{granule}.h5",
                    "type": "data",
                    "uri": f"{args.source}/{granule}.h5",
                    "checksumType": "SHA256",
                    "checksum": granules_to_transfer[granule]["h5"]["checksum"],
                    "size": h5_file_sizes[f"{granule}.h5"],
                }
            ]
        }
    }

    # Post the record
    if args.verbose:
        print(f"[{records_transfered} of {args.transfer}] Posting {granule}:", json.dumps(record,indent=2))
    if records_transfered < args.transfer:
        records_transfered += 1
        # post transfer record
        response = kinesis_client.put_record(StreamName=args.notification_stream, Data=json.dumps(record), PartitionKey=args.partition_key)
        if response == "test":
            pass
        elif response['ResponseMetadata']['HTTPStatusCode'] == 200:
            records_success += 1
            # update cache
            cnm_granules[granule] = granules_to_transfer[granule]
            write_cache(args.cnm_cache, cnm_granules)
            print(f"[{records_transfered} of {records_to_transfer}] {granule}: success")
        else:
            records_failure += 1
            print(f"[{records_transfered} of {records_to_transfer}] {granule}: failure")
    else:
        print(f"Finished transfering {records_transfered} of {records_to_transfer} records: {records_success} succeeded, {records_failure} failed.")
        sys.exit(0)

