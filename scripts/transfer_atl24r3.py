import argparse
import base64
import sys
from contextlib import suppress
import boto3
import hashlib
import json
import uuid
import time
from datetime import datetime

# ###############################
# Globals
# ###############################

# Command Line Arguments
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--source',                 type=str,               default="s3://sliderule/data/ATL24r3")
parser.add_argument('--database',               type=str,               default="data/atl24r3_database.json")
parser.add_argument('--data_version',           type=str,               default="003")
parser.add_argument('--transfer',               type=int,               default=0) # must provide in order to actually transfer
parser.add_argument('--batch_size',             type=int,               default=100)
parser.add_argument('--test',                   action='store_true',    default=False)
parser.add_argument('--verbose',                action='store_true',    default=False)
args = parser.parse_args()

# transfer parameters
collection              = "ATL24"
provider                = "ICESat-2_sliderule"
partition_key           = "SlideRule"
if args.test:
    response_stream_arn = "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response"
    assume_role         = "arn:aws:iam::024284894447:role/nsidc-ops-uat_cross_provider_kinesis_role"
    notification_stream = "nsidc-ops-uat-ATLAS_sliderule_notification"
else:
    response_stream_arn = "arn:aws:kinesis:us-west-2:790840705381:stream/nsidc-ops-prod-ATLAS_sliderule_response"
    assume_role         = "arn:aws:iam::790840705381:role/nsidc-ops-prod_cross_provider_kinesis_role"
    notification_stream = "nsidc-ops-prod-ATLAS_sliderule_notification"

# create s3 client
s3 = boto3.client("s3")

# Get kinesis client by assuming the role and getting credentials
sts = boto3.client('sts')
assumed_role = sts.assume_role(RoleArn=assume_role, RoleSessionName='AssumeRoleSession')
credentials = assumed_role['Credentials'] # get temporary credentials
kinesis = boto3.client(
    'kinesis',
    region_name='us-west-2',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'])

# read database
with open(args.database, "r") as file:
    database = json.load(file)

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

# Calculate Checksum
def calc_checksum(bucket, key):
    print(f"Warning! Calculating checksum for s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    sha256 = hashlib.sha256()
    for chunk in iter(lambda: response["Body"].read(1024 * 1024), b""):
        sha256.update(chunk)
    return sha256.hexdigest()

# Get Size and SHA256 Checksum of S3 Object
def get_attributes(bucket, key):
    response = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    checksum = response.get("ChecksumSHA256")
    if checksum and "-" not in checksum: # "-N" suffix is a composite (per-part) checksum, not the whole-object digest
        checksum = base64.b64decode(checksum).hex()
    else:
        checksum = calc_checksum(bucket, key)
    return {
        "size": response["ContentLength"],
        "checksum": checksum
    }

# ###############################
# Main
# ###############################

# get granules to transfer
granules_to_transfer = {}
bucket, subfolder = parse_url(args.source)
for atl03_granule, entry in database["granules"].items():
    if entry["status"] == "output":
        granule = atl03_granule.replace("ATL03", "ATL24").replace(".h5", f"_{args.data_version}_01")
        granules_to_transfer[granule] = {
            "h5": get_attributes(bucket, f"{subfolder}/{granule}.h5"),
            "xml": get_attributes(bucket, f"{subfolder}/{granule}.iso.xml")
        }

# Metrics
records_to_transfer = min(len(granules_to_transfer), args.transfer)
records_success = 0
records_failure = 0

# Post records to stream
for i in range(0, records_to_transfer, args.batch_size):

    # Verbose status
    if args.verbose:
        print(f"Posting granules {i} to {i + args.batch_size} of {records_to_transfer}")

    # Build batch of records
    batch = [{
        "Data": json.dumps({
            "version": 1.3,
            "submissionTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000000"),
            "identifier": str(uuid.uuid4()),
            "collection": collection,
            "provider": provider,
            "responseStreamArn": response_stream_arn,
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
                        "size": granules_to_transfer[granule]["xml"]["size"],
                    },
                    {
                        "name": f"{granule}.h5",
                        "type": "data",
                        "uri": f"{args.source}/{granule}.h5",
                        "checksumType": "SHA256",
                        "checksum": granules_to_transfer[granule]["h5"]["checksum"],
                        "size": granules_to_transfer[granule]["h5"]["size"],
                    }
                ]
            }
        }),
        "PartitionKey": partition_key
    } for granule in granules_to_transfer[i:i+args.batch_size]]

    # Post batch of records
    backoff_performed = False
    batch_response = kinesis.put_records(StreamName=notification_stream, Records=batch)
    for record, result in zip(batch, batch_response["Records"]):
        if "ErrorCode" in result:   # e.g. ProvisionedThroughputExceededException
            # Perform backoff
            if not backoff_performed:
                backoff_performed = True
                time.sleep(5)
            # Retry post
            individual_response = kinesis.put_record(StreamName=notification_stream, Data=record["Data"], PartitionKey=partition_key)
            if individual_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                if not args.test: database["granules"][granule]["status"] = "tx_failed" # update database
                records_failure += 1
            else:
                if not args.test: database["granules"][granule]["status"] = "tx_initiated" # update database
                records_success += 1
        else:
            if not args.test: database["granules"][granule]["status"] = "tx_initiated" # update database
            records_success += 1

    # Status
    print(f"Finished transfering {records_to_transfer} records: {records_success} succeeded, {records_failure} failed.")

#########################################
# save database
#########################################
with open(args.database, 'w') as file:
    json.dump(database, file)
