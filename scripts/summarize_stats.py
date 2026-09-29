import os
import json
import traceback
import argparse
import boto3
import pandas as pd

try:

    # ########################
    # Command Line Arguments
    # ########################

    parser = argparse.ArgumentParser(description="""sliderule python job runner""")
    parser.add_argument('--result_output',  type=str,   default="/tmp/summary.txt")
    parser.add_argument('--summary_file',   type=str,   default="/tmp/atl24_v3_granule_collection.csv")
    args = parser.parse_args()

    # ########################
    # Globals
    # ########################

    s3 = boto3.client("s3")

    BEAMS = [
        "gt1l",
        "gt1r",
        "gt2l",
        "gt2r",
        "gt3l",
        "gt3r"
    ]

    # ########################
    # Helper Functions
    # ########################

    def parse_url(url):
        path = url.split("s3://")[-1]
        bucket = path.split("/")[0]
        key = '/'.join(path.split("/")[1:])
        return bucket, key

    def load_remote_file(url):
        bucket, key = parse_url(url)
        obj = s3.get_object(Bucket=bucket, Key=key)
        contents = obj["Body"].read().decode("utf-8")
        try:
            return json.loads(contents)
        except Exception:
            return contents

    def add_result(summary, result):
        if not summary:
            if os.path.exists(args.summary_file):
                summary = pd.read_csv(args.summary_file)
                summary = summary.to_dict('list')
            else:
                summary = {key: [] for key in result}
        for key in result:
            summary[key].append(result[key])
        return summary

    def write_csv_file(summary, filename):
        df = pd.DataFrame(summary)
        df.to_csv(filename, index=False)

    # ########################
    # Main
    # ########################

    summary = {}
    for line in open(args.result_output, "r"):
        output = json.loads(line)
        if output["status"] == "success":
            result = load_remote_file(f"s3://sliderule-public/{output['file']}")
            for beam in BEAMS:
                if beam in result:
                    summary = add_result(summary, result[beam])
    write_csv_file(summary, args.summary_file)

except Exception as e:

    # ########################
    # Errors
    # ########################

    print(f"Unhandled exception: {e}")
    traceback.print_exc()
