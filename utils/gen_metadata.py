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
parser.add_argument('--summary_file',   type=str,   default="atl24_v2_granule_collection.csv")
parser.add_argument('--url',            type=str,   default="s3://sliderule-public")
parser.add_argument("--loglvl" ,        type=str,   default="CRITICAL")
parser.add_argument("--cores",          type=int,   default=os.cpu_count())
args,_ = parser.parse_known_args()

# ################################################
# Constants
# ################################################

STATS = [
    ('granule',''),
    ('beam',''),
    ('region',''),
    ('season',''),
    ('size',''),
    ('total_photons',''),
    ('sea_surface_photons',''),
    ('sea_surface_std','.3f'),
    ('bathy_photons',''),
    ('bathy_mean_depth','.3f'),
    ('bathy_min_depth','.3f'),
    ('bathy_max_depth','.3f'),
    ('bathy_std_depth','.3f'),
    ('bathy_above_sea_level', ''),
    ('bathy_below_sensor_depth', ''),
    ('histogram',''),
    ('polygon', ''),
    ('begin_time', ''),
    ('end_time', '')
]

VARIABLES = [
    "ortho_h",
    "class_ph"
]

BEAMS = [
    "gt1l",
    "gt1r",
    "gt2l",
    "gt2r",
    "gt3l",
    "gt3r"
]

MONTH_TO_SEASON = { #[is_north][month] --> 0: winter, 1: spring, 2: summer, 3: fall
    True: {
        1: 0,
        2: 0,
        3: 0,
        4: 1,
        5: 1,
        6: 1,
        7: 2,
        8: 2,
        9: 2,
        10: 3,
        11: 3,
        12: 3
    },
    False: {
        1: 2,
        2: 2,
        3: 2,
        4: 3,
        5: 3,
        6: 3,
        7: 0,
        8: 0,
        9: 0,
        10: 1,
        11: 1,
        12: 1
    }
}

RELEASE = "002"
VERSION = "01"

# ################################################
# Initialize
# ################################################

logger.config(args.loglvl)
credentials = {"role": True, "profile":"sliderule"}
granules_already_processed = {} # [name]: True"
granules_to_process = [] # (name, size)

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
    csv_header = [entry[0] for entry in STATS]
    summary_file.write(','.join(csv_header) + "\n")

# report number of existing granules
print(f'Granules Already Processed: {len(granules_already_processed)}')

# ################################################
# List H5 Granules
# ################################################

# initialize s3 client
s3 = boto3.client('s3')

# get bucket and subfolder from url
path = args.url.split("s3://")[-1]
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
            if granule.startswith("ATL24") and granule.endswith(RELEASE + "_" + VERSION + ".h5"):
                if granule not in granules_already_processed:
                    granules_to_process.append((granule, obj["Size"]))
    # check if more data is available
    is_truncated = response['IsTruncated']
    continuation_token = response.get('NextContinuationToken')
print("") # new line

# report new granules
print(f'Granules Left to Process: {len(granules_to_process)}')

# ################################################
# Append Stats for Each Granule
# ################################################

def stat_worker(arg):

    # initialize arguments
    granule, size = arg
    path = args.url.split("s3://")[-1] + "/" + granule
    csv_lines = ""

    try:
        # open granule
        h5obj = h5coro.H5Coro(path, s3driver.S3Driver, errorChecking=True, verbose=False, credentials=credentials, multiProcess=False)

        # read datasets
        datasets = [f'{beam}/{var}' for beam in BEAMS for var in VARIABLES]
        promise = h5obj.readDatasets(datasets + ['metadata/extent'], block=True, enableAttributes=False)

        # pull out metadata
        extent = json.loads(promise["metadata/extent"])

        # process each beam in the granule
        for beam in BEAMS:
            try:
                # get info
                month = int(granule[10:12])
                region = int(granule[27:29])

                # read columns
                columns = {var: promise[f'{beam}/{var}'][:] for var in VARIABLES}

                # build dataframes
                df = pd.DataFrame(columns)
                df_sea_surface = df[df["class_ph"] == 41]
                df_bathy = df[df["class_ph"] == 40]

                # build histogram
                values = df_bathy["ortho_h"].to_numpy()
                hist, bin_edges = np.histogram(values, bins=np.arange(-50, 1, 1)) # (-50.0,-49.0] (-49.0,-48.0], ..., (-1.0, 0.0]
                hist_str = ' '.join([f"{int(value)}" for value in hist])

                # build stats
                row = {
                    "granule": granule,
                    "beam": beam,
                    "region": region,
                    "season": MONTH_TO_SEASON[region<8][month],
                    "size": size,
                    "total_photons": len(df),
                    "sea_surface_photons": len(df_sea_surface),
                    "sea_surface_std": df_sea_surface["ortho_h"].std(),
                    "bathy_photons": len(df_bathy),
                    "bathy_mean_depth": df_bathy["ortho_h"].mean(),
                    "bathy_min_depth": df_bathy["ortho_h"].min(),
                    "bathy_max_depth": df_bathy["ortho_h"].max(),
                    "bathy_std_depth": df_bathy["ortho_h"].std(),
                    "bathy_above_sea_level": len(df_bathy[df_bathy["ortho_h"] > 0.0]),
                    "bathy_below_sensor_depth": len(df_bathy[df_bathy["ortho_h"] <= -50.0]),
                    "histogram": hist_str,
                    "polygon": extent["polygon"],
                    "begin_time": extent["begin_time"],
                    "end_time": extent["end_time"]
                }

                # display progress
                print(".", end='')
                sys.stdout.flush()

                # build csv line
                csv_lines += ','.join([f'{row[entry[0]]:{entry[1]}}' for entry in STATS]) + "\n"

            except TypeError:
                # missing beam
                pass

        # close granule
        h5obj.close()

        # return csv lines
        return csv_lines

    except Exception as e:
        print(f'\n{granule} - Error: {e}')
        return ''

# ################################################
# Start Process Workers
# ################################################

granule_step = 1000
for i in range(0, len(granules_to_process), granule_step):
    pool = Pool(args.cores)
    for result in pool.imap_unordered(stat_worker, granules_to_process[i:i+granule_step]):
        if result != None:
            summary_file.write(result)
            summary_file.flush()
    del pool
