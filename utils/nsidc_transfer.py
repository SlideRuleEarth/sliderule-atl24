# NOTES:
#   * The .aws/credentials file must be temporarily removed in order for this to work
#     This is because the NSIDC role associated with the kinesis stream allows only the
#     the silderules3 role associated with the EC2 instance to assume its role.

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
        "collection": "ATL24_D2C",
        "provider": "CISS",
        "responseStreamArn": "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response",
        "product": {
            "name": "ATL24_D2C_20181014082645_02400106_006_02_001_01.h5",
            "dataVersion": "001",
            "files": [
                {
                    "name": "ATL24_D2C_20181014082645_02400106_006_02_001_01.iso.xml",
                    "type": "metadata",
                    "uri": "s3://sliderule/data/C2C/ATL24_D2C_20181014082645_02400106_006_02_001_01.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": "cfcb0bd82ce3a940549684227b7994262957af3c3f3b72e5cd77421d2648178b",
                    "size": 21543
                },
                {
                    "name": "ATL24_D2C_20181014082645_02400106_006_02_001_01.h5",
                    "type": "data",
                    "uri": "s3://sliderule/data/C2C/ATL24_D2C_20181014082645_02400106_006_02_001_01.h5",
                    "checksumType": "SHA256",
                    "checksum": "ccb5eb4a478295141ffbeca2ce2b99f261d32e7be2ef3ff76891f31e6a620d96",
                    "size": 55219186
                }
            ]
        }
    },
    {
        "version": 1.3,
        "submissionTime": "",
        "identifier": "",
        "collection": "ATL24_D2C",
        "provider": "CISS",
        "responseStreamArn": "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response",
        "product": {
            "name": "ATL24_D2C_20181014083516_02400107_006_02_001_01.h5",
            "dataVersion": "001",
            "files": [
                {
                    "name": "ATL24_D2C_20181014083516_02400107_006_02_001_01.iso.xml",
                    "type": "metadata",
                    "uri": "s3://sliderule/data/C2C/ATL24_D2C_20181014083516_02400107_006_02_001_01.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": "6795600ab8c18395378378fd3f69eceb7cac7130607eea86d6f2506aa91dfa38",
                    "size": 21162
                },
                {
                    "name": "ATL24_D2C_20181014083516_02400107_006_02_001_01.h5",
                    "type": "data",
                    "uri": "s3://sliderule/data/C2C/ATL24_D2C_20181014083516_02400107_006_02_001_01.h5",
                    "checksumType": "SHA256",
                    "checksum": "b8dd2e7f5206b908bb8c4af47390187d35872e23b4e3e0c44b4a85e8f6d1b703",
                    "size": 192597644
                }
            ]
        }
    },
    {
        "version": 1.3,
        "submissionTime": "",
        "identifier": "",
        "collection": "ATL24_D2C",
        "provider": "CISS",
        "responseStreamArn": "arn:aws:kinesis:us-west-2:941673577314:stream/nsidc-cumulus-uat-external_response",
        "product": {
            "name": "ATL24_D2C_20181014100103_02410106_006_02_001_01.h5",
            "dataVersion": "001",
            "files": [
                {
                    "name": "ATL24_D2C_20181014100103_02410106_006_02_001_01.iso.xml",
                    "type": "metadata",
                    "uri": "s3://sliderule/data/C2C/ATL24_D2C_20181014100103_02410106_006_02_001_01.iso.xml",
                    "checksumType": "SHA256",
                    "checksum": "a5f67df1762651268932755c58563523c2a36b43bd4edd3ead7d7817720807df",
                    "size": 21546
                },
                {
                    "name": "ATL24_D2C_20181014100103_02410106_006_02_001_01.h5",
                    "type": "data",
                    "uri": "s3://sliderule/data/C2C/ATL24_D2C_20181014100103_02410106_006_02_001_01.h5",
                    "checksumType": "SHA256",
                    "checksum": "11870045fde96dc82f8d5e3b679f64650b3334ddf19a12c1443706e1dd1b62ea",
                    "size": 16308083
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

# Post records to stream
for record in cnm_records:
    response = kinesis_client.put_record(
        StreamName='nsidc-ops-uat-ATLAS_sliderule_notification', # 'nsidc-cumulus-uat-ATLAS_sliderule_notification'
        Data=json.dumps(record),
        PartitionKey="SlideRule")
    print(f'{record["product"]["name"]}: {response}')


