import sys
import time
import json
import boto3
import numpy as np
import shapely
import geopandas as gpd
from shapely.affinity import scale
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

# modify granule to point to parquet
if "ATL03" in granule: # convert to ATL24 granule name
    granule = granule.replace("ATL03", "ATL24").replace(".h5", "_003_01.parquet")

# initialize result
input_file = f"s3://sliderule-public/atl24r3/parquet/{granule}"
output_file = f"s3://sliderule-public/atl24r3/json/{granule.replace(".parquet", ".json")}"
result = {
    "status": True,
    "build": "local",
    "start": time.time(),
    "outputs": [output_file],
    "messages": []
}

try:
    # read granule into GeoDataFrames
    result["messages"].append(f"Processing {granule}")
    gdf = gpd.read_parquet(input_file)

    # get polygon
    hull = gdf.geometry.union_all().convex_hull
    buffers = []
    for lon, lat in shapely.get_coordinates(hull):
        lon_scale = 1.0 / max(np.cos(np.radians(lat)), 0.01)
        circle = shapely.Point(lon, lat).buffer(0.01)
        buffers.append(scale(circle, xfact=lon_scale, yfact=1.0, origin=(lon, lat)))
    poly = shapely.GeometryCollection(buffers).convex_hull.simplify(0.005)
    poly_str = ' '.join([f'{x:.6f} {y:.6f}' for x, y in poly.exterior.coords])

    # initialize output
    grid = np.zeros((720, 1440), dtype=np.uint32)
    month = int(granule[10:12])
    region = int(granule[27:29])
    output = {
        "granule": {
            "name":         granule,
            "region":       region,
            "season":       MONTH_TO_SEASON[region<8][month],
            "polygon":      poly_str,
            "begin_time":   gdf.index.min().isoformat(),
            "end_time":     gdf.index.max().isoformat()
        }
    }

    # process each beam
    for beam in BEAMS:

        # perform initial analysis on dataframe
        gdf["depth"] = gdf["surface_h"] - gdf["geoid_corr_h"]
        beam_gdf = gdf[gdf["gt"] == BEAM_TO_GT[beam]]
        class_ph_counts = beam_gdf["class_ph"].value_counts()
        quality_ph_counts = beam_gdf["quality_ph"].value_counts()
        bathy_gdf = beam_gdf[beam_gdf["class_ph"] == 40]
        bathy_quality_ph_counts = bathy_gdf["quality_ph"].value_counts()
        sea_surface_gdf = beam_gdf[beam_gdf["class_ph"] == 41]
        subaqueous_gdf = beam_gdf[beam_gdf["class_ph"].isin([0, 1, 40]) & (beam_gdf['geoid_corr_h'] < beam_gdf['surface_h'])]

        # update grid
        # convert lat/lon to 0.25 degree grid indices
        # lat: -90 to 90 -> row 0 (south) to 719 (north)
        # lon: -180 to 180 -> col 0 (west) to 1439 (east)
        for lon, lat in zip(bathy_gdf.geometry.x, bathy_gdf.geometry.y):
            row = int((lat + 90) / 0.25)
            col = int((lon + 180) / 0.25)
            row = min(max(row, 0), 719)
            col = min(max(col, 0), 1439)
            grid[row, col] += 1

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

            "surface_roughness_mean":   sea_surface_gdf["surface_roughness"].mean(),
            "surface_roughness_median": sea_surface_gdf["surface_roughness"].median(),
            "surface_roughness_min":    sea_surface_gdf["surface_roughness"].min(),
            "surface_roughness_max":    sea_surface_gdf["surface_roughness"].max(),
            "surface_roughness_std":    sea_surface_gdf["surface_roughness"].std(),

            "kd_mean":                  subaqueous_gdf["kd"].mean(),
            "kd_median":                subaqueous_gdf["kd"].median(),
            "kd_min":                   subaqueous_gdf["kd"].min(),
            "kd_max":                   subaqueous_gdf["kd"].max(),
            "kd_std":                   subaqueous_gdf["kd"].std(),
        }

        # status
        result["messages"].append(f"Adding {beam} with {output[beam]["photons"]} photons to result")

    # set grid in output
    lons, lats = np.where(grid > 0) # Find all non-zero cells
    cnts = [grid[lon,lat] for lon,lat in zip(lons,lats)]
    output["grid"] = {
        "lons": lons,
        "lats": lats,
        "cnts": cnts
    }

    # write outputs
    bucket, key = parse_url(output_file)
    s3.put_object(Bucket=bucket, Key=key, Body=output)

except Exception as e:

    # errors
    result["status"] = False
    result["messages"].append(f"Unhandled exception: {e}")

# finish
result["stop"] = time.time()
with open(result_file, "w") as file:
    json.dump(result, file, default=np_default)