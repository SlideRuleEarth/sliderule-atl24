import boto3
import time

# Assume the role
sts_client = boto3.client('sts')
assumed_role = sts_client.assume_role(
    RoleArn='arn:aws:iam::941673577314:role/nsidc-cumulus-uat_cross_provider_kinesis_role',
    RoleSessionName='AssumeRoleSession')

# Get temporary credentials
credentials = assumed_role['Credentials']

# Create kinesis client
kinesis_client = boto3.client(
    'kinesis',
    region_name='us-west-2',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'])

# Get the shard iterator
stream_name = "nsidc-cumulus-uat-ATLAS_sliderule_response"
response = kinesis_client.describe_stream(StreamName=stream_name)
shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]
shard_iterator_response = kinesis_client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="LATEST"  # Options: AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST
)
shard_iterator = shard_iterator_response["ShardIterator"]

# Read records in a loop
while shard_iterator:
    response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)

    # Process the records
    for record in response["Records"]:
        data = record["Data"].decode("utf-8")  # Decode bytes to string
        print(f"Received record: {data}")

    # Get the next shard iterator
    shard_iterator = response.get("NextShardIterator")

    # Wait before polling again (avoid throttling)
    time.sleep(1)