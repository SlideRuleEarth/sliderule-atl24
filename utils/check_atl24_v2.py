import argparse
import os
import sys
import json
import pandas as pd
import numpy as np
import boto3
from multiprocessing.pool import Pool
from h5coro import h5coro, s3driver, logger

# ################################################
# Command Line Arguments
# ################################################

parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--summary_file',   type=str,               default="data/atl24_v2_granule_check.csv")
parser.add_argument('--url_v2',         type=str,               default="s3://sliderule-public")
parser.add_argument('--url_v1',         type=str,               default="s3://sliderule/data/ATL24")
parser.add_argument("--loglvl" ,        type=str,               default="CRITICAL")
parser.add_argument("--cores",          type=int,               default=os.cpu_count())
parser.add_argument("--test" ,          type=str,               default=None) # ATL24_20181120020325_08010106_006_02_002_01.h5
args,_ = parser.parse_known_args()

# ################################################
# Constants
# ################################################

CSV = [
    ('granule',''),
    ('check','')
]

VARIABLES = [
    "class_ph",
    "confidence",
    "ellipse_h",
    "index_ph",
    "index_seg",
    "invalid_kd",
    "invalid_wind_speed",
    "lat_ph",
    "lon_ph",
    "low_confidence_flag",
    "night_flag",
    "ortho_h",
    "sensor_depth_exceeded",
    "sigma_thu",
    "sigma_tvu",
    "surface_h",
    "delta_time",
    "x_atc",
    "y_atc"
]

BEAMS = [
    "gt1l",
    "gt1r",
    "gt2l",
    "gt2r",
    "gt3l",
    "gt3r"
]


# ################################################
# Initialize
# ################################################

logger.config(args.loglvl)
credentials = {"role": True, "role":"iam"}
granules_already_processed = {} # [name]: True
granules_to_process = [] # (name, size)
granules_in_error = {} # [name]: True

# ################################################
# Open Summary File
# ################################################

# get existing granules
if os.path.exists(args.summary_file):
    summary_df = pd.read_csv(args.summary_file)
    for granule in summary_df["granule"]:
        granules_already_processed[granule] = True
    summary_file = open(args.summary_file, "a")
else:
    summary_file = open(args.summary_file, "w")
    csv_header = [entry[0] for entry in CSV]
    summary_file.write(','.join(csv_header) + "\n")

# report number of existing granules
print(f'Granules Already Processed: {len(granules_already_processed)}')

# ################################################
# List H5 Granules
# ################################################

if args.test == None:
    # initialize s3 client
    s3 = boto3.client('s3')

    # get bucket and subfolder from url
    path = args.url_v2.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])

    # read granules
    is_truncated = True
    continuation_token = None
    while is_truncated:
        # make request
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder)
        # display status
        print("#", end='')
        sys.stdout.flush()
        # parse contents
        if 'Contents' in response:
            for obj in response['Contents']:
                granule = obj['Key'].split("/")[-1]
                if granule.startswith("ATL24") and granule.endswith("002_01.h5"):
                    if granule not in granules_already_processed:
                        granules_to_process.append(granule)
        # check if more data is available
        is_truncated = response['IsTruncated']
        continuation_token = response.get('NextContinuationToken')
    print("") # new line
else:
    # populate granules to test with user supplied value
    granules_to_process = [args.test]

# report new granules
print(f'Granules Left to Process: {len(granules_to_process)}')

# ################################################
# Append Stats for Each Granule
# ################################################

def stat_worker(granule):

    # initialize arguments
    status = True

    try:
        # construct datasets
        datasets = [f'{beam}/{var}' for beam in BEAMS for var in VARIABLES]

        # read v1 granule
        path_v1 = args.url_v1.split("s3://")[-1] + "/" + granule.replace("002_01.h5", "001_01.h5")
        h5obj_v1 = h5coro.H5Coro(path_v1, s3driver.S3Driver, errorChecking=True, verbose=False, credentials=credentials, multiProcess=False)
        promise_v1 = h5obj_v1.readDatasets(datasets, block=True, enableAttributes=False)

        # read v2 granule
        path_v2 = args.url_v2.split("s3://")[-1] + "/" + granule
        h5obj_v2 = h5coro.H5Coro(path_v2, s3driver.S3Driver, errorChecking=True, verbose=False, credentials=credentials, multiProcess=False)
        promise_v2 = h5obj_v2.readDatasets(datasets, block=True, enableAttributes=False)

        # process each beam in the granule
        for beam in BEAMS:
            try:
                # build dataframes
                df_v2 = pd.DataFrame({var: promise_v2[f'{beam}/{var}'][:] for var in VARIABLES})
                df_v1 = pd.DataFrame({var: promise_v1[f'{beam}/{var}'][:] for var in VARIABLES})

                # check class_ph
                allowed_classifications = [0, 40, 41]
                if not df_v2["class_ph"].isin(allowed_classifications).all():
                    print(f'Error - {beam}/class_ph: unexpected classifications {set(df_v2["class_ph"]) - set(allowed_classifications)}')
                    status = False

                # check low_confidence_flag
                allowed_values = [0, 1]
                if not df_v2["low_confidence_flag"].isin(allowed_values).all():
                    print(f'Error - {beam}/low_confidence_flag: unexpected values {set(df_v2["low_confidence_flag"]) - set(allowed_values)}')
                    status = False
                flag_count = (df_v2["low_confidence_flag"] == 1).sum()
                bathy_count = (df_v2["class_ph"] == 40).sum()
                if flag_count > bathy_count:
                    print(f'Error - {beam}/low_confidence_flag: more low confidence photons than bathy photons, {flag_count} > {bathy_count}')
                    status = False

                # check exact
                for var in ["confidence", "ellipse_h", "ortho_h", "index_ph", "index_seg",
                            "invalid_kd", "invalid_wind_speed", "lat_ph", "lon_ph", "night_flag",
                            "sensor_depth_exceeded", "sigma_thu", "sigma_tvu", "surface_h",
                            "delta_time", "x_atc", "y_atc"]:
                    diffs = (df_v1[var] != df_v2[var])
                    if diffs.sum() > 0:
                        print(f'Error - {beam}/{var}: {diffs.sum()} mismatched values')
                        status = False

                # display progress
                print(".", end='')
                sys.stdout.flush()

            except TypeError as e:
                print(f"Error: {e}")
                pass # missing beam

        # close granules
        h5obj_v1.close()
        h5obj_v2.close()

    except Exception as e:
        print(f'\n{granule} - Error: {e}')
        status = False

    # return status
    row = {
        "granule": granule,
        "check": status,
    }
    return ','.join([f'{row[entry[0]]:{entry[1]}}' for entry in CSV]) + "\n"

# ################################################
# Start Process Workers
# ################################################

granule_step = 1000
for i in range(0, len(granules_to_process), granule_step):
    pool = Pool(args.cores)
    for result in pool.imap_unordered(stat_worker, granules_to_process[i:i+granule_step]):
        if args.test == None:
            summary_file.write(result)
            summary_file.flush()
    del pool
