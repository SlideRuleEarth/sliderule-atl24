import sys
import time
import json
import traceback
import boto3
import numpy as np
import pandas as pd
import shapely
import geopandas as gpd
from shapely.affinity import scale
from shapely.geometry.polygon import orient
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

# season map[is_north][month] --> 0: winter, 1: spring, 2: summer, 3: fall
MONTH_TO_SEASON = {
    True: { 1: 0, 2: 0, 3: 0, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 3, 11: 3, 12: 3 },
    False: { 1: 2, 2: 2, 3: 2, 4: 3, 5: 3, 6: 3, 7: 0, 8: 0, 9: 0, 10: 1, 11: 1, 12: 1 }
}

# along-track bin sizes (m) and invalid value used by atl24_v2_algorithms estimate_kd and estimate_surface_roughness
KD_BIN_SIZE = 500
ROUGHNESS_BIN_SIZE = 750
INVALID_VALUE = -24.0

# collapse a per-photon interpolated column to one value per along-track bin
def per_bin_values(df, col, x0, bin_size):
    valid = df[df[col] != INVALID_VALUE]
    bins = np.floor((valid["x_atc"] - x0) / bin_size)
    return valid.groupby(bins)[col].median()

# convert to json-native types with NaN/NaT as None; json's default hook can't do this since np.float64 subclasses float
def json_safe(o):
    if isinstance(o, dict):
        return {k: json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [json_safe(v) for v in o]
    if isinstance(o, np.ndarray):
        return json_safe(o.tolist())
    if pd.api.types.is_scalar(o) and pd.isna(o):
        return None
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    if isinstance(o, np.generic): # np.int64, np.float32, np.bool_, ...
        return o.item()
    return o

# parse URL into bucket and subfolder
def parse_url(url):
    path = url.split("s3://")[-1]
    bucket = path.split("/")[0]
    subfolder = '/'.join(path.split("/")[1:])
    return bucket, subfolder

# modify granule to point to parquet
if "ATL03" in granule: # convert to ATL24 granule name
    granule = granule.replace("ATL03", "ATL24").replace(".h5", "_003_01.parquet")

# initialize result
input_file = f"s3://sliderule-public/atl24r3/parquet/{granule}"
output_file = f"s3://sliderule-public/atl24r3/json/{granule.replace('.parquet', '.json')}"
result = {
    "status": True,
    "build": "local",
    "start": time.time(),
    "outputs": [],
    "messages": []
}

try:
    # read granule into GeoDataFrames
    result["messages"].append(f"Processing {granule}")
    gdf = gpd.read_parquet(input_file, columns=["geometry", "gt", "class_ph", "quality_ph", "surface_h", "geoid_corr_h", "x_atc", "kd", "surface_roughness"])
    gdf["depth"] = gdf["surface_h"] - gdf["geoid_corr_h"]

    # get polygon; longitudes are unwrapped in time order so antimeridian and polar crossings stay continuous (lon may exceed 180)
    coords = shapely.get_coordinates(gdf.geometry.values)[np.argsort(gdf.index.values, kind="stable")]
    coords[:, 0] = np.unwrap(coords[:, 0], period=360)
    if coords[:, 0].min() < -180:
        coords[:, 0] += 360
    hull = shapely.multipoints(coords).convex_hull
    buffers = []
    for lon, lat in shapely.get_coordinates(hull):
        lon_scale = 1.0 / max(np.cos(np.radians(lat)), 0.01)
        circle = shapely.Point(lon, lat).buffer(0.01)
        buffers.append(scale(circle, xfact=lon_scale, yfact=1.0, origin=(lon, lat)))
    poly = orient(shapely.GeometryCollection(buffers).convex_hull.simplify(0.005), sign=1.0)
    poly_str = ' '.join([f'{x:.6f} {y:.6f}' for x, y in poly.exterior.coords])

    # initialize output
    month = int(granule[10:12])
    region = int(granule[27:29])
    output = {
        "granule": {
            "name":         granule,
            "region":       region,
            "season":       MONTH_TO_SEASON[region<8][month],
            "polygon":      poly_str,
            "begin_time":   gdf.index.min(),
            "end_time":     gdf.index.max()
        }
    }

    # process each beam
    for beam in BEAMS:

        # perform initial analysis on dataframe
        beam_gdf = gdf[gdf["gt"] == BEAM_TO_GT[beam]]
        class_ph_counts = beam_gdf["class_ph"].value_counts()
        quality_ph_counts = beam_gdf["quality_ph"].value_counts()
        bathy_gdf = beam_gdf[beam_gdf["class_ph"] == 40]
        bathy_quality_ph_counts = bathy_gdf["quality_ph"].value_counts()
        sea_surface_gdf = beam_gdf[beam_gdf["class_ph"] == 41]
        subaqueous_gdf = beam_gdf[beam_gdf["class_ph"].isin([0, 1, 40]) & (beam_gdf['geoid_corr_h'] < beam_gdf['surface_h'])]
        x0 = beam_gdf["x_atc"].min()
        kd_bins = per_bin_values(subaqueous_gdf, "kd", x0, KD_BIN_SIZE)
        roughness_bins = per_bin_values(sea_surface_gdf, "surface_roughness", x0, ROUGHNESS_BIN_SIZE)

        # set beam elements
        output[beam] = {
            "beam":                     beam,
            "photons":                  len(beam_gdf),
            "subaqueous":               len(subaqueous_gdf),

            "class_bathymetry":         class_ph_counts.get(40, 0),
            "class_sea_surface":        class_ph_counts.get(41, 0),
            "class_noise":              class_ph_counts.get(0, 0),
            "class_idk":                class_ph_counts.get(1, 0),
            "class_ground":             class_ph_counts.get(2, 0),

            "quality_nominal":          quality_ph_counts.get(0,0) + quality_ph_counts.get(10,0) + quality_ph_counts.get(20,0),
            "quality_afterpulse":       quality_ph_counts.get(1,0) + quality_ph_counts.get(11,0) + quality_ph_counts.get(21,0),
            "quality_impulse":          quality_ph_counts.get(2,0) + quality_ph_counts.get(12,0) + quality_ph_counts.get(22,0),
            "quality_tep":              quality_ph_counts.get(3,0),
            "quality_burst":            quality_ph_counts.get(4,0) + quality_ph_counts.get(14,0) + quality_ph_counts.get(24,0),
            "quality_streak":           quality_ph_counts.get(5,0) + quality_ph_counts.get(15,0) + quality_ph_counts.get(25,0),

            "bathy_quality_nominal":    bathy_quality_ph_counts.get(0,0) + bathy_quality_ph_counts.get(10,0) + bathy_quality_ph_counts.get(20,0),
            "bathy_quality_afterpulse": bathy_quality_ph_counts.get(1,0) + bathy_quality_ph_counts.get(11,0) + bathy_quality_ph_counts.get(21,0),
            "bathy_quality_impulse":    bathy_quality_ph_counts.get(2,0) + bathy_quality_ph_counts.get(12,0) + bathy_quality_ph_counts.get(22,0),
            "bathy_quality_tep":        bathy_quality_ph_counts.get(3,0),
            "bathy_quality_burst":      bathy_quality_ph_counts.get(4,0) + bathy_quality_ph_counts.get(14,0) + bathy_quality_ph_counts.get(24,0),
            "bathy_quality_streak":     bathy_quality_ph_counts.get(5,0) + bathy_quality_ph_counts.get(15,0) + bathy_quality_ph_counts.get(25,0),

            "bathy_depth_mean":         bathy_gdf["depth"].mean(),
            "bathy_depth_median":       bathy_gdf["depth"].median(),
            "bathy_depth_min":          bathy_gdf["depth"].min(),
            "bathy_depth_max":          bathy_gdf["depth"].max(),
            "bathy_depth_std":          bathy_gdf["depth"].std(),

            "surface_roughness_mean":   roughness_bins.mean(),
            "surface_roughness_median": roughness_bins.median(),
            "surface_roughness_min":    roughness_bins.min(),
            "surface_roughness_max":    roughness_bins.max(),
            "surface_roughness_std":    roughness_bins.std(),

            "kd_mean":                  kd_bins.mean(),
            "kd_median":                kd_bins.median(),
            "kd_min":                   kd_bins.min(),
            "kd_max":                   kd_bins.max(),
            "kd_std":                   kd_bins.std(),
        }

        # status
        result["messages"].append(f"Adding {beam} with {output[beam]['photons']} photons to result")

    # set grid in output - 0.25 deg cells: row 0 = 90S, col 0 = 180W
    bathy_all = gdf[gdf["class_ph"] == 40]
    rows = np.clip(np.floor((bathy_all.geometry.y + 90) / 0.25).astype(int), 0, 719)
    cols = np.clip(np.floor((bathy_all.geometry.x + 180) / 0.25).astype(int), 0, 1439)
    cells, cnts = np.unique(rows * 1440 + cols, return_counts=True)
    output["grid"] = {"rows": cells // 1440, "cols": cells % 1440, "cnts": cnts}

    # write outputs
    bucket, key = parse_url(output_file)
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(json_safe(output), allow_nan=False), ContentType="application/json")
    result["outputs"].append(output_file)

except Exception:

    # errors
    result["status"] = False
    result["messages"].append(f"Unhandled exception: {traceback.format_exc()}")

# finish
result["stop"] = time.time()
with open(result_file, "w") as file:
    json.dump(json_safe(result), file, allow_nan=False)