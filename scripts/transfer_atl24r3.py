import argparse
import base64
import sys
import traceback
import boto3
import hashlib
import json
import uuid
import time
from datetime import datetime, timezone
from atl24r3_database import Database, Status

# ###############################
# Globals
# ###############################

# Command Line Arguments
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--source',                 type=str,               default="s3://sliderule/data/ATL24r3")
parser.add_argument('--database',               type=str,               default="data/atl24r3_database.json")
parser.add_argument('--data_version',           type=str,               default="003")
parser.add_argument('--transfer',               type=int,               default=0) # must provide in order to actually transfer
parser.add_argument('--batch_size',             type=int,               default=100, choices=range(1, 501))
parser.add_argument('--status_to_transfer',     type=Status,            default=Status.OUTPUT)
parser.add_argument('--test',                   action='store_true',    default=False)
parser.add_argument('--verbose',                action='store_true',    default=False)
args = parser.parse_args()

# transfer parameters
collection              = "ATL24"
provider                = "ICESat-2_sliderule"
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

# read database
database = Database(args.database)

# program status
exit_code = 0
num_granules_to_transfer = 0
records_success = 0
records_failure = 0

# ###############################
# Helper Functions
# ###############################

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
def get_attributes(filename):
    bucket, subfolder = parse_url(args.source)
    key = f"{subfolder}/{filename}"
    response = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    checksum = response.get("ChecksumSHA256")
    if checksum and "-" not in checksum: # "-N" suffix is a composite (per-part) checksum, not the whole-object digest
        checksum = base64.b64decode(checksum).hex()
    else:
        checksum = calc_checksum(bucket, key)
    return {
        "name": filename,
        "path": f"{args.source}/{filename}",
        "size": response["ContentLength"],
        "checksum": checksum
    }

# ###############################
# Main
# ###############################

try:
    # Get granules to transfer
    granules_to_check = [granule for granule, entry in database.granules.items() if entry["status"] == args.status_to_transfer]
    print(f"Preparing {len(granules_to_check)} granules with status {args.status_to_transfer}")
    for i in range(len(granules_to_check)):
        granule = granules_to_check[i]
        atl24_granule = granule.replace("ATL03", "ATL24").replace(".h5", f"_{args.data_version}_01")
        if i % 10 == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
        try:
            database.update_attributes(granule, {
                "h5": get_attributes(f"{atl24_granule}.h5"),
                "xml": get_attributes(f"{atl24_granule}.iso.xml")
            })
        except Exception as e:
            print(f"Error! Missing output for {granule}: {e}")
            database.update_status(granule, Status.MISSING)
    sys.stdout.write("\n")
    sys.stdout.flush()

    # Initialize loop variables
    granules_to_transfer = [granule for granule, entry in database.granules.items() if entry["status"] == Status.TX_READY]
    num_granules_to_transfer = min(len(granules_to_transfer), args.transfer)
    previous_cred_refresh = 0.0 # previous time
    print(f"Transfering {num_granules_to_transfer} of {len(granules_to_transfer)} granules ready to be transferred")

    # Post records to stream
    for i in range(0, num_granules_to_transfer, args.batch_size):

        # Get kinesis client by assuming the role and getting credentials
        now = time.time()
        if (now - previous_cred_refresh) > (60 * 30): # 30 minutes
            print(f"Refreshing credentials for {assume_role}")
            previous_cred_refresh = now
            sts = boto3.client('sts')
            assumed_role = sts.assume_role(RoleArn=assume_role, RoleSessionName='AssumeRoleSession')
            credentials = assumed_role['Credentials'] # get temporary credentials
            kinesis = boto3.client(
                'kinesis',
                region_name='us-west-2',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'])

        # Verbose status
        if args.verbose:
            print(f"Posting granules {i} to {i + args.batch_size} of {num_granules_to_transfer}")

        # Build batch of records
        batch = [{
            "Data": json.dumps({
                "version": 1.3,
                "submissionTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000"),
                "identifier": str(uuid.uuid4()),
                "collection": collection,
                "provider": provider,
                "responseStreamArn": response_stream_arn,
                "product": {
                    "name": database.granules[granule]["attributes"]["h5"]["name"],
                    "dataVersion": args.data_version,
                    "files": [
                        {
                            "name": database.granules[granule]["attributes"]["xml"]["name"],
                            "type": "metadata",
                            "uri": database.granules[granule]["attributes"]["xml"]["path"],
                            "checksumType": "SHA256",
                            "checksum": database.granules[granule]["attributes"]["xml"]["checksum"],
                            "size": database.granules[granule]["attributes"]["xml"]["size"],
                        },
                        {
                            "name": database.granules[granule]["attributes"]["h5"]["name"],
                            "type": "data",
                            "uri": database.granules[granule]["attributes"]["h5"]["path"],
                            "checksumType": "SHA256",
                            "checksum": database.granules[granule]["attributes"]["h5"]["checksum"],
                            "size": database.granules[granule]["attributes"]["h5"]["size"],
                        }
                    ]
                }
            }),
            "PartitionKey": granule
        } for granule in granules_to_transfer[i:min(i+args.batch_size, num_granules_to_transfer)]]

        # Post batch of records
        backoff_performed = False
        batch_response = kinesis.put_records(StreamName=notification_stream, Records=batch)
        for record, result, k in zip(batch, batch_response["Records"], range(len(batch))):
            granule = granules_to_transfer[i + k]
            if "ErrorCode" in result:   # e.g. ProvisionedThroughputExceededException
                # Perform backoff
                if not backoff_performed:
                    backoff_performed = True
                    time.sleep(5)
                # Retry post
                try:
                    individual_response = kinesis.put_record(StreamName=notification_stream, Data=record["Data"], PartitionKey=granule)
                    database.update_status(granule, Status.TX_INITIATED)
                    records_success += 1
                except Exception as e:
                    database.update_status(granule, Status.TX_FAILED)
                    print(f"Error! Failed to put granule {granule}: {e}")
                    records_failure += 1
            else:
                database.update_status(granule, Status.TX_INITIATED)
                records_success += 1

except Exception:

    # Status Failure
    print(f"Error! Unhandled exception:\n{traceback.format_exc()}")
    exit_code = 1

finally:

    # Status
    print(f"Finished transfering {num_granules_to_transfer} records: {records_success} succeeded, {records_failure} failed.")

    # Save Database
    if not args.test:
        database.write()

sys.exit(exit_code)
