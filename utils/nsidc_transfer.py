# NOTES:
#   * The .aws/credentials file must be temporarily removed in order for this to work
#     This is because the NSIDC role associated with the kinesis stream allows only the
#     the silderules3 role associated with the EC2 instance to assume its role.


# import boto3
# import hashlib
#
# s3 = boto3.client("s3")
#
# response = s3.get_object(
#     Bucket="my-bucket",
#     Key="large-file.bin",
# )
#
# sha256 = hashlib.sha256()
#
# for chunk in iter(lambda: response["Body"].read(1024 * 1024), b""):
#     sha256.update(chunk)
#
# print(sha256.hexdigest())

import boto3
import json
import uuid
from datetime import datetime

## CNM records
cnm_records = [
    {
        "version": 1.3,
        "submissionTime": "",
        "identifier": "",
        "collection": "ATL24",
        "provider": "ICESat-2_sliderule",
        "responseStreamArn": "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response",
        "product": {
            "name": "ATL24_20181014082645_02400106_006_02_002_01.h5",
            "dataVersion": "002",
            "files": [
                {
                    "name": "ATL24_20181014082645_02400106_006_02_002_01.iso.xml",
                    "type": "metadata",
                    "uri": "s3://sliderule/data/ATL24r2/ATL24_20181014082645_02400106_006_02_002_01.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": "5f1c2a836eb2be1935aefb3387ad337e9fa0de359c847e626ee0ac1afd4febc5",
                    "size": 21522
                },
                {
                    "name": "ATL24_20181014082645_02400106_006_02_002_01.h5",
                    "type": "data",
                    "uri": "s3://sliderule/data/ATL24r2/ATL24_20181014082645_02400106_006_02_002_01.h5",
                    "checksumType": "SHA256",
                    "checksum": "c18c468c5e0060f94579ab33fce82fb3d634709bbed1192b5610fa4407904390",
                    "size": 54988498
                }
            ]
        }
    },
    {
        "version": 1.3,
        "submissionTime": "",
        "identifier": "",
        "collection": "ATL24",
        "provider": "ICESat-2_sliderule",
        "responseStreamArn": "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response",
        "product": {
            "name": "ATL24_20181014083516_02400107_006_02_002_01.h5",
            "dataVersion": "001",
            "files": [
                {
                    "name": "ATL24_20181014083516_02400107_006_02_002_01.iso.xml",
                    "type": "metadata",
                    "uri": "s3://sliderule/data/ATL24r2/ATL24_20181014083516_02400107_006_02_002_01.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": "a00f9319df8ceb373b606b84e2d16091fecc45f7a1e48bedb365a1cf70dfd80e",
                    "size": 21141
                },
                {
                    "name": "ATL24_20181014083516_02400107_006_02_002_01.h5",
                    "type": "data",
                    "uri": "s3://sliderule/data/ATL24r2/ATL24_20181014083516_02400107_006_02_002_01.h5",
                    "checksumType": "SHA256",
                    "checksum": "0091062f18011d8b37931e8f424e7f04281a91d555734eb5a7ca1a72701bf701",
                    "size": 192568906
                }
            ]
        }
    },
    {
        "version": 1.3,
        "submissionTime": "",
        "identifier": "",
        "collection": "ATL24",
        "provider": "ICESat-2_sliderule",
        "responseStreamArn": "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response",
        "product": {
            "name": "ATL24_20181014100103_02410106_006_02_002_01.h5",
            "dataVersion": "001",
            "files": [
                {
                    "name": "ATL24_20181014100103_02410106_006_02_002_01.iso.xml",
                    "type": "metadata",
                    "uri": "s3://sliderule/data/ATL24r2/ATL24_20181014100103_02410106_006_02_002_01.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": "feca761b8d0573d789442176b022625a04e4cd33d49b60a47237b94159c0c9d1",
                    "size": 21525
                },
                {
                    "name": "ATL24_20181014100103_02410106_006_02_002_01.h5",
                    "type": "data",
                    "uri": "s3://sliderule/data/ATL24r2/ATL24_20181014100103_02410106_006_02_002_01.h5",
                    "checksumType": "SHA256",
                    "checksum": "07c932756060dbd8e661fdf7bf85b690dc4a2c886b3f2dba76ae760fd3b904f3",
                    "size": 16295544
                }
            ]
        }
    }
]

# Update CNM records
for record in cnm_records:
    record["submissionTime"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000000")
    record["identifier"] = str(uuid.uuid4())

# Assume the role
sts_client = boto3.client('sts')
assumed_role = sts_client.assume_role(
    RoleArn='arn:aws:iam::024284894447:role/nsidc-ops-uat_cross_provider_kinesis_role',
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

# Post records to stream
for record in cnm_records:
    response = kinesis_client.put_record(
        StreamName='nsidc-ops-uat-ATLAS_sliderule_notification', # 'nsidc-cumulus-uat-ATLAS_sliderule_notification'
        Data=json.dumps(record),
        PartitionKey="SlideRule")
    print(f'{record["product"]["name"]}: {response}')


