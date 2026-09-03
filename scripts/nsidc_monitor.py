import argparse
import boto3
import sys
import time
from botocore.exceptions import ClientError

# ###############################
# Globals
# ###############################

# Command Line Arguments
parser = argparse.ArgumentParser(description="""Monitor the NSIDC response stream""")
parser.add_argument('--iterator_type',  type=str,               default="LATEST", choices=["LATEST", "TRIM_HORIZON"])
parser.add_argument('--limit',          type=int,               default=10)
parser.add_argument('--poll',           type=float,             default=1.0)
parser.add_argument('--test',           action='store_true',    default=False)
args = parser.parse_args()

# monitor parameters
if args.test:
    response_stream_arn = "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response"
    assume_role         = "arn:aws:iam::024284894447:role/nsidc-ops-uat_cross_provider_kinesis_role"
else:
    response_stream_arn = "arn:aws:kinesis:us-west-2:790840705381:stream/nsidc-ops-prod-ATLAS_sliderule_response"
    assume_role         = "arn:aws:iam::790840705381:role/nsidc-ops-prod_cross_provider_kinesis_role"

# kinesis client
kinesis = None
previous_cred_refresh = 0.0 # previous time

# ###############################
# Helper Functions
# ###############################

# Get kinesis client by assuming the role and getting credentials
def refresh_credentials():
    global kinesis, previous_cred_refresh
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

# List every shard in the stream
def list_shard_ids():
    shard_ids = []
    next_token = None
    while True:
        # the stream lives in the producer's account, so it can only be addressed by ARN
        if next_token: response = kinesis.list_shards(StreamARN=response_stream_arn, NextToken=next_token)
        else: response = kinesis.list_shards(StreamARN=response_stream_arn)
        shard_ids += [shard["ShardId"] for shard in response["Shards"]]
        next_token = response.get("NextToken")
        if not next_token:
            return shard_ids

# Get individual shard id
def get_shard_id(index=0):
    stream_name = "nsidc-cumulus-uat-external_response"
    response = kinesis.describe_stream(StreamName=stream_name)
    shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]
    return [shard_id]

# Get iterator for a shard, resuming after the last record read if there was one
def get_shard_iterator(shard_id, sequence_number):
    if sequence_number:
        response = kinesis.get_shard_iterator(StreamARN=response_stream_arn, ShardId=shard_id, ShardIteratorType="AFTER_SEQUENCE_NUMBER", StartingSequenceNumber=sequence_number)
    else:
        response = kinesis.get_shard_iterator(StreamARN=response_stream_arn, ShardId=shard_id, ShardIteratorType=args.iterator_type)
    return response["ShardIterator"]

# ###############################
# Main
# ###############################

exit_code = 0

try:
    # Open every shard in the stream
    refresh_credentials()
    shards = {shard_id: {"iterator": get_shard_iterator(shard_id, None), "sequence": None} for shard_id in list_shard_ids()}
    print(f"Monitoring {len(shards)} shard(s) of {response_stream_arn}")

    # Read records in a loop
    while shards:

        # Refresh credentials before they expire
        refresh_credentials()

        # Read each shard in turn
        for shard_id in list(shards):
            try:
                response = kinesis.get_records(StreamARN=response_stream_arn, ShardIterator=shards[shard_id]["iterator"], Limit=args.limit)
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "ExpiredIteratorException":
                    shards[shard_id]["iterator"] = get_shard_iterator(shard_id, shards[shard_id]["sequence"])
                elif error_code == "ProvisionedThroughputExceededException":
                    time.sleep(1)
                else:
                    raise
                continue

            # Process the records
            for record in response["Records"]:
                shards[shard_id]["sequence"] = record["SequenceNumber"]
                print(f'[{shard_id}] {record["Data"].decode("utf-8")}')

            # Advance the shard, dropping it if it was closed by a reshard
            shards[shard_id]["iterator"] = response.get("NextShardIterator")
            if shards[shard_id]["iterator"] == None:
                print(f"Shard {shard_id} closed")
                del shards[shard_id]

        # Wait before polling again (avoid throttling)
        time.sleep(args.poll)

    print("No open shards remaining")

except KeyboardInterrupt:

    print("\nExiting")

except Exception as e:

    print(f"Error! Unable to monitor {response_stream_arn}: {e}")
    exit_code = 1

sys.exit(exit_code)
