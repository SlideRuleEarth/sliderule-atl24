import sys
import time
import json
import boto3
import numpy as np
import geopandas as gpd
from sliderule import icesat2

# arguments
granule = sys.argv[1]
result_file = sys.argv[2]

# globals
s3 = boto3.client("s3")

# list of beams
BEAMS = [
    "gt1l",
    "gt1r",
    "gt2l",
    "gt2r",
    "gt3l",
    "gt3r"
]

# beams to ground track lookup table
BEAM_TO_GT = {
    "gt1l": icesat2.GT1L,
    "gt1r": icesat2.GT1R,
    "gt2l": icesat2.GT2L,
    "gt2r": icesat2.GT2R,
    "gt3l": icesat2.GT3L,
    "gt3r": icesat2.GT3R
}

# handle serializing numpy types
def np_default(o):
    if isinstance(o, np.generic): # np.int64, np.float32, np.bool_, ...
        return o.item()
    if isinstance(o, np.ndarray):
        return o.tolist()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

# parse URL into bucket and subfolder
def parse_url(url):
    path = url.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])
    return bucket, subfolder

# get initialized row
def get_initialized_row(granule=None, beam=None):
    return {
        "granule": granule,
        "beam": beam,

        "photons": 0,
        "subaqueous": 0,

        "class_bathymetry": 0,                  # 40
        "class_sea_surface": 0,                 # 41
        "class_noise": 0,                       # 0
        "class_idk": 0,                         # 1
        "class_ground": 0,                      # 2

        "quality_nominal": 0,                   # 0
        "quality_afterpulse": 0,                # 1
        "quality_impulse": 0,                   # 2
        "quality_tep": 0,                       # 3
        "quality_burst": 0,                     # 4
        "quality_streak": 0,                    # 5
        "quality_part": 0,                      # 10
        "quality_part_afterpulse": 0,           # 11
        "quality_part_ir": 0,                   # 12
        "quality_part_burst": 0,                # 14
        "quality_part_streak": 0,               # 15
        "quality_full": 0,                      # 20
        "quality_full_afterpulse": 0,           # 21
        "quality_full_ir": 0,                   # 22
        "quality_full_burst": 0,                # 24
        "quality_full_streak": 0,               # 25

        "bathy_quality_nominal": 0,             # 0
        "bathy_quality_afterpulse": 0,          # 1
        "bathy_quality_impulse": 0,             # 2
        "bathy_quality_tep": 0,                 # 3
        "bathy_quality_burst": 0,               # 4
        "bathy_quality_streak": 0,              # 5
        "bathy_quality_part": 0,                # 10
        "bathy_quality_part_afterpulse": 0,     # 11
        "bathy_quality_part_ir": 0,             # 12
        "bathy_quality_part_burst": 0,          # 14
        "bathy_quality_part_streak": 0,         # 15
        "bathy_quality_full": 0,                # 20
        "bathy_quality_full_afterpulse": 0,     # 21
        "bathy_quality_full_ir": 0,             # 22
        "bathy_quality_full_burst": 0,          # 24
        "bathy_quality_full_streak": 0,         # 25

        "bathy_depth_mean": 0,
        "bathy_depth_min": 0,
        "bathy_depth_max": 0,
        "bathy_depth_std": 0,
        "sea_surface_std": 0
    }

# initialize result
result = {
    "status": True,
    "build": "local",
    "start": time.time(),
    "outputs": [],
    "messages": []
}

try:
    # read granule into GeoDataFrames
    if "ATL03" in granule: # convert to ATL24 granule name
        granule = granule.replace("ATL03", "ATL24").replace(".h5", "_003_01.parquet")
    result["messages"].append(f"Processing {granule}")
    gdf = gpd.read_parquet(f"s3://sliderule-public/atl24r3/parquet/{granule}")

    # process each beam
    for beam in BEAMS:

        # get row for beam all set to zeros
        row = get_initialized_row(granule, beam)

        # perform initial analysis on dataframe
        gdf["depth"] = gdf["surface_h"] - gdf["geoid_corr_h"]
        beam_gdf = gdf[gdf["gt"] == BEAM_TO_GT[beam]]
        class_ph_counts = beam_gdf["class_ph"].value_counts()
        quality_ph_counts = beam_gdf["quality_ph"].value_counts()
        bathy_gdf = beam_gdf[beam_gdf["class_ph"] == 40]
        bathy_quality_ph_counts = bathy_gdf["quality_ph"].value_counts()

        # set row elements
        row["photons"] = len(beam_gdf)
        row["subaqueous"] = (beam_gdf['geoid_corr_h'] < beam_gdf['surface_h']).sum()

        row["class_bathymetry"] = class_ph_counts.get(40, 0)
        row["class_sea_surface"] = class_ph_counts.get(41, 0)
        row["class_noise"] = class_ph_counts.get(0, 0)
        row["class_idk"] = class_ph_counts.get(1, 0)
        row["class_ground"] = class_ph_counts.get(2, 0)

        row["quality_nominal"] = quality_ph_counts.get(0,0)
        row["quality_afterpulse"] = quality_ph_counts.get(1,0)
        row["quality_impulse"] = quality_ph_counts.get(2,0)
        row["quality_tep"] = quality_ph_counts.get(3,0)
        row["quality_burst"] = quality_ph_counts.get(4,0)
        row["quality_streak"] = quality_ph_counts.get(5,0)
        row["quality_part"] = quality_ph_counts.get(10,0)
        row["quality_part_afterpulse"] = quality_ph_counts.get(11,0)
        row["quality_part_ir"] = quality_ph_counts.get(12,0)
        row["quality_part_burst"] = quality_ph_counts.get(14,0)
        row["quality_part_streak"] = quality_ph_counts.get(15,0)
        row["quality_full"] = quality_ph_counts.get(20,0)
        row["quality_full_afterpulse"] = quality_ph_counts.get(21,0)
        row["quality_full_ir"] = quality_ph_counts.get(22,0)
        row["quality_full_burst"] = quality_ph_counts.get(24,0)
        row["quality_full_streak"] = quality_ph_counts.get(25,0)

        row["bathy_quality_nominal"] = bathy_quality_ph_counts.get(0,0)
        row["bathy_quality_afterpulse"] = bathy_quality_ph_counts.get(1,0)
        row["bathy_quality_impulse"] = bathy_quality_ph_counts.get(2,0)
        row["bathy_quality_tep"] = bathy_quality_ph_counts.get(3,0)
        row["bathy_quality_burst"] = bathy_quality_ph_counts.get(4,0)
        row["bathy_quality_streak"] = bathy_quality_ph_counts.get(5,0)
        row["bathy_quality_part"] = bathy_quality_ph_counts.get(10,0)
        row["bathy_quality_part_afterpulse"] = bathy_quality_ph_counts.get(11,0)
        row["bathy_quality_part_ir"] = bathy_quality_ph_counts.get(12,0)
        row["bathy_quality_part_burst"] = bathy_quality_ph_counts.get(14,0)
        row["bathy_quality_part_streak"] = bathy_quality_ph_counts.get(15,0)
        row["bathy_quality_full"] = bathy_quality_ph_counts.get(20,0)
        row["bathy_quality_full_afterpulse"] = bathy_quality_ph_counts.get(21,0)
        row["bathy_quality_full_ir"] = bathy_quality_ph_counts.get(22,0)
        row["bathy_quality_full_burst"] = bathy_quality_ph_counts.get(24,0)
        row["bathy_quality_full_streak"] = bathy_quality_ph_counts.get(25,0)

        row["bathy_depth_mean"] = bathy_gdf["depth"].mean()
        row["bathy_depth_min"] = bathy_gdf["depth"].min()
        row["bathy_depth_max"] = bathy_gdf["depth"].max()
        row["bathy_depth_std"] = bathy_gdf["depth"].std()
        row["sea_surface_std"] = bathy_gdf["surface_h"].std()

        # add row
        result["messages"].append(f"Adding {beam} with {row["photons"]} photons to result")
        result[beam] = row

except Exception as e:

    # errors
    result["status"] = False
    result["messages"].append(f"Unhandled exception: {e}")

# finish
result["stop"] = time.time()
with open(result_file, "w") as file:
    json.dump(result, file, default=np_default)