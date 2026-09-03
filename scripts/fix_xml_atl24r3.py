import argparse
import sys
import boto3

# command line arguments
parser = argparse.ArgumentParser(description="""ATL24 Platinum Run""")
parser.add_argument('--source_xml', type=str,   default="s3://sliderule-public/atl24r3/xml")
args = parser.parse_args()

# display raw
def display(s):
    sys.stdout.write(s)
    sys.stdout.flush()

# parse URL into bucket and subfolder
def parse_url(url):
    path = url.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])
    return bucket, subfolder

# load remote file from s3
def load_remote_file(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")

# store remote file to s3
def store_remote_file(bucket, key, content):
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))

# globals
s3 = boto3.client("s3")
bucket, subfolder = parse_url(args.source_xml)

# enumerate objects in s3 bucket
xml_filenames = []
is_truncated = True
continuation_token = None
while is_truncated:
    if continuation_token:  response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder, ContinuationToken=continuation_token)
    else:                   response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder)
    display("#")
    if 'Contents' in response:
        for obj in response['Contents']:
            resource = obj['Key'].split("/")[-1]
            xml_filenames.append(resource)
    is_truncated = response['IsTruncated']
    continuation_token = response.get('NextContinuationToken')
display("\n")

# process each file
print(f"Processing {len(xml_filenames)} files")
for xml_filename in xml_filenames:
    path = f"{subfolder}/{xml_filename}"
    content = load_remote_file(bucket, path)
    if content.find("<gco:CharacterString>001</gco:CharacterString>") >= 0:
        print(f"Fixing {path}")
        content = content.replace("<gco:DateTime>\"", "<gco:DateTime>").replace("\"</gco:DateTime>", "</gco:DateTime>").replace("<gco:CharacterString>001</gco:CharacterString>", "<gco:CharacterString>003</gco:CharacterString>")
        store_remote_file(bucket, path, content)
    else:
        print(f"Skipping {path}")
