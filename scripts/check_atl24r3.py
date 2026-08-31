import json
import argparse
import boto3
import pandas as pd
import geopandas as gpd
import numpy as np
import pyarrow.parquet as pq
from pyproj import Transformer
from photon_refraction import photon_refraction
from h5coro import h5coro, s3driver
from sliderule import icesat2

try:
    import rasterio
except:
    pass

try:
    import matplotlib.pyplot as plt
except:
    pass

USE_CARTOPY = True
try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
except:
    pass
    USE_CARTOPY = False

#
# Command Line Arguments
#
parser = argparse.ArgumentParser(description="""ATL24""")
parser.add_argument('--database',           type=str,               default="data/atl24r3_database.json")
parser.add_argument('--plot_geometry',      type=str,               default=None) # "geoid_corr_h", plot the geometry column of a parquet file on a global map
parser.add_argument('--atl03_granule',      type=str,               default=None) # "ATL03_20191215112656_12150507_006_01.h5"
args,_ = parser.parse_known_args()

#
# Read Database
#
with open(args.database, "r") as file:
    database = json.load(file)

#
# Constants
#
VERSIONS = {
    "sliderule_version":    "v5.5.0",
    "atl24_plugin_version": "v3.0.2",
    "alt24_algo_version":   "b09eb09"
}

COLORS={
    0:['gray', 'unclassified'],
    1:['yellow','other'],
    41:['blue', 'sea_surface'],
    40:['red','bathymetry']
}

SPOT_TO_GRID = {
    1: (0,0),
    2: (1,0),
    3: (0,1),
    4: (1,1),
    5: (0,2),
    6: (1,2)
}

INFO_TO_SPOT = { # [SC_ORIENT][TRACK][PAIR]
    0: {            # SC_BACKWARD
        '1': {      # TRACK
            'l': 1, # PAIR
            'r': 2,
        },
        '2': {      # TRACK
            'l': 3, # PAIR
            'r': 4,
        },
        '3': {      # TRACK
            'l': 5, # PAIR
            'r': 6,
        }
    },
    1: {            # SC_FOWARD
        '1': {      # TRACK
            'l': 6, # PAIR
            'r': 5,
        },
        '2': {      # TRACK
            'l': 4, # PAIR
            'r': 3,
        },
        '3': {      # TRACK
            'l': 2, # PAIR
            'r': 1,
        }
    }
}

BEAMS = [
    "gt1l",
    "gt1r",
    "gt2l",
    "gt2r",
    "gt3l",
    "gt3r"
]

BEAM_TO_SPOT = { # [SC_ORIENT][BEAM]
    0: { # SC_BACKWARD
        'gt1l': 1,
        'gt1r': 2,
        'gt2l': 3,
        'gt2r': 4,
        'gt3l': 5,
        'gt3r': 6
    },
    1: { # SC_FOWARD
        'gt1l': 6,
        'gt1r': 5,
        'gt2l': 4,
        'gt2r': 3,
        'gt3l': 2,
        'gt3r': 1
    }
}

VARIABLES = [
    "class_ph",
    "confidence",
    "delta_time",
    "ellipse_h",
    "index_ph",
    "index_seg",
    "segment_id",
    "lat_ph",
    "lon_ph",
    "night_flag",
    "ortho_h",
    "sigma_thu",
    "sigma_tvu",
    "surface_h",
    "x_atc",
    "y_atc",
    "kd",
    "surface_roughness"
]

BEAMS = [
    "gt1l",
    "gt1r",
    "gt2l",
    "gt2r",
    "gt3l",
    "gt3r"
]

BEAM_TO_GT = {
    "gt1l": icesat2.GT1L,
    "gt1r": icesat2.GT1R,
    "gt2l": icesat2.GT2L,
    "gt2r": icesat2.GT2R,
    "gt3l": icesat2.GT3L,
    "gt3r": icesat2.GT3R
}

#
# Get H5 Filename
#
def get_h5_filename(atl03_granule):
    return f"sliderule-public/atl24r3/h5/{atl03_granule.replace("ATL03", "ATL24").replace(".h5", "_003_01.h5")}"

#
# Get Parquet Filename
#
def get_parquet_filename(atl03_granule):
    return f"s3://sliderule-public/atl24r3/parquet/{atl03_granule.replace("ATL03", "ATL24").replace(".h5", "_003_01.parquet")}"

#
# Read ATL24 H5
#
def read_atl24_h5(atl03_granule):
    atl24_h5_file = get_h5_filename(atl03_granule)
    h5obj = h5coro.H5Coro(atl24_h5_file, s3driver.S3Driver, errorChecking=True, verbose=False, credentials={"role": True, "role":"iam"}, multiProcess=False)
    promise = h5obj.readDatasets([f'{beam}/{var}' for beam in BEAMS for var in VARIABLES], block=False, enableAttributes=False)
    dfs = []
    for beam in BEAMS:
        if f'{beam}/{VARIABLES[0]}' in promise:
            data = {var: promise[f'{beam}/{var}'][:] for var in VARIABLES} | {"beam": [beam for i in range(len(promise[f'{beam}/{VARIABLES[0]}']))]}
            dfs.append(gpd.pd.DataFrame(data))
    promise = h5obj.readDatasets(['metadata/sliderule', 'metadata/atl24'], block=True, enableAttributes=False)
    metadata = {"sliderule": json.loads(promise['metadata/sliderule'][:]), "atl24": json.loads(promise['metadata/atl24'][:])}
    h5obj.close()
    return pd.concat(dfs, ignore_index=True), metadata

#
# Read ATL24 Parquet
#
def read_atl24_parquet(atl03_granule):
    atl24_parquet_file = get_parquet_filename(atl03_granule)
    return gpd.read_parquet(atl24_parquet_file)

#
# Compare Parquet to h5
#
def compare_parquet_to_h5(h5_df, parquet_df, beam):

    # create lat and lon columns
    parquet_df["lat_ph"] = parquet_df.geometry.y
    parquet_df["lon_ph"] = parquet_df.geometry.x
    parquet_df["ortho_h"] = parquet_df["geoid_corr_h"]
    parquet_df["delta_time"] = (parquet_df.index - np.datetime64('2018-01-01T00:00:00')) / np.timedelta64(1, 's')
    parquet_df["night_flag"] = ((parquet_df["processing_flags"] & 0x20) != 0).astype(np.int8)

    # check class_ph
    allowed_classifications = [0, 1, 2, 40, 41]
    if not h5_df["class_ph"].isin(allowed_classifications).all():
        raise RuntimeError(f'Error - {beam}/class_ph: unexpected classifications {set(h5_df["class_ph"]) - set(allowed_classifications)}')

    # check values (compared positionally; the two frames have different indexes)
    for var in VARIABLES:
        h5_vals = h5_df[var].to_numpy()
        parquet_vals = parquet_df[var].to_numpy()
        if len(h5_vals) != len(parquet_vals):
            raise RuntimeError(f'Error - {beam}/{var}: mismatched length {len(h5_vals)} != {len(parquet_vals)}')
        if np.issubdtype(h5_vals.dtype, np.floating) or np.issubdtype(parquet_vals.dtype, np.floating):
            rtol = 1e-6 if np.float32 in (h5_vals.dtype, parquet_vals.dtype) else 1e-12
            diffs = ~np.isclose(h5_vals, parquet_vals, rtol=rtol, atol=0.0, equal_nan=True)
        else:
            diffs = (h5_vals != parquet_vals)
        if diffs.sum() > 0:
            max_diff = np.max(np.abs(h5_vals[diffs].astype(np.float64) - parquet_vals[diffs].astype(np.float64)))
            raise RuntimeError(f'Error - {beam}/{var}: {diffs.sum()} mismatched values in {len(parquet_vals)} rows, max diff {max_diff}')

#
# Check Metadata
#
def check_metadata(metadata):
    print(metadata["atl24"])
    if metadata["atl24"]["plugin_version"] != VERSIONS["atl24_plugin_version"]:
        raise RuntimeError(f"Invalid plugin version: {metadata["atl24"]["plugin_version"]}")
    if metadata["atl24"]["sliderule_version"] != VERSIONS["sliderule_version"]:
        raise RuntimeError(f"Invalid sliderule version: {metadata["atl24"]["sliderule_version"]}")
    if metadata["atl24"]["plugin_algoinfo"] != VERSIONS["alt24_algo_version"]:
        raise RuntimeError(f"Invalid algorithm version: {metadata["atl24"]["plugin_algoinfo"]}")
    if metadata["sliderule"]["min_dem_delta"] != -100.0: # spot check
        raise RuntimeError(f"Invalid sliderule metadata")

#
# Analyze ATL24 Result
#
def analyze_atl24_result(atl03_granule):
#    try:
        h5_df_all, metadata = read_atl24_h5(atl03_granule)
        parquet_df_all = read_atl24_parquet(atl03_granule)
        check_metadata(metadata)
        for beam in BEAMS:
            h5_df = h5_df_all[h5_df_all["beam"] == beam]
            parquet_df = parquet_df_all[parquet_df_all["gt"] == BEAM_TO_GT[beam]] # .reset_index(drop=True)
            compare_parquet_to_h5(h5_df, parquet_df, beam)
        print(f"{atl03_granule} - successfully checked {len(parquet_df_all)} rows")
 #   except Exception as e:
 #       print(f"{atl03_granule} - exception: {e}")

#
# Plot Geometry
#
def plot_geometry(atl03_granule, column):

    atl24_parquet_file = get_parquet_filename(atl03_granule)
    parquet_file = pq.ParquetFile(atl24_parquet_file)
    table = parquet_file.read(columns=['lon_ph', 'lat_ph', column])
    lon_ph = np.concatenate([chunk.to_numpy() for chunk in table['lon_ph'].iterchunks()])
    lat_ph = np.concatenate([chunk.to_numpy() for chunk in table['lat_ph'].iterchunks()])
    column_array = np.concatenate([chunk.to_numpy() for chunk in table[column].iterchunks()])
    df = pd.DataFrame()
    df['lon_ph'] = lon_ph
    df['lat_ph'] = lat_ph
    df[column] = column_array
    geometry = gpd.points_from_xy(df["lon_ph"], df["lat_ph"])
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:32662')

    if USE_CARTOPY:
        fig, ax = plt.subplots(figsize=(16, 10), subplot_kw={'projection': ccrs.PlateCarree()})
        ax.set_title("ATL24")
        ax.add_feature(cfeature.LAND)
        ax.add_feature(cfeature.OCEAN)
        ax.add_feature(cfeature.BORDERS, linestyle=':')
        ax.add_feature(cfeature.COASTLINE)
        ax.set_xlim(-180,180)
        ax.set_ylim(-90,90)
        gdf.plot(ax=ax, transform=ccrs.PlateCarree(), color='blue')
    else:
        f, ax = plt.subplots()
        ax.set_title("ATL24")
        ax.set_aspect('equal')
        world = gpd.read_file('natural_earth/ne_110m_admin_0_countries.shp')
        world.plot(ax=ax, color='0.8', edgecolor='black')
        gdf.plot(ax=ax, column=column, cmap='viridis', markersize=2.0)
        ax.set_xlim(-180,180)
        ax.set_ylim(-90,90)
    plt.show()

#
# Function: check_h5_to_atl03
#
def check_h5_to_atl03():

    atl03 = h5py.File(args.atl03_granule)
    atl24 = h5py.File(args.h5_granule)

    # for each beam
    for beam in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:

        atl03_delta_time = atl03[beam]["heights"]["delta_time"][:]
        atl24_delta_time = atl24[beam]["delta_time"][:]
        atl24_index_ph = atl24[beam]["index_ph"][:]

        # check values
        for i in range(len(atl24_index_ph)):

            # delta_time
            dt_delta = abs(atl03_delta_time[atl24_index_ph[i]] - atl24_delta_time[i])
            if(dt_delta > 0.000001):
                print(f'MISMATCH VALUE - {beam} delta_time on row {i} at photon index {atl24_index_ph[i]}: {dt_delta}')

        print(f'Beam {beam} check complete')
#
# Function: compare_to_atl03
#
def compare_to_atl03():

    f = h5py.File(args.atl03_granule)
    df = pd.read_parquet(args.granule)
    sc_orient = f["orbit_info"]["sc_orient"][0]

    # check cmr polygon
    h5_bounding_polygon = {"lat": f["orbit_info"]["bounding_polygon_lat1"][:], "lon": f["orbit_info"]["bounding_polygon_lon1"][:]}
    parquet_file = pq.ParquetFile(args.granule)
    cmr_polygon = json.loads(parquet_file.metadata.metadata[b'cmr'])
    num_coords_in_poly = len(h5_bounding_polygon["lat"])
    if num_coords_in_poly < 4:
        print(f'INVALID VALUE - number of coordinates in h5 bounding polygon is too small: {num_coords_in_poly}')
    elif num_coords_in_poly != len(cmr_polygon["lat"]) or num_coords_in_poly != len(cmr_polygon["lon"]):
        print(f'MISMATCH VALUE - number of coordinates in h5 bounding polygon does not match ATL24 bounding polygon: {num_coords_in_poly} != {len(cmr_polygon["lat"])}, {len(cmr_polygon["lon"])}')
    for i in range(num_coords_in_poly):
        cmr_lat_delta = abs(h5_bounding_polygon["lat"][i] - cmr_polygon["lat"][i])
        if cmr_lat_delta > 0.00000000001:
            print(f'MISMATCH VALUE - h5 bounding polygon latitude {i} does not match: {h5_bounding_polygon["lat"][i]} != {cmr_polygon["lat"][i]}')
        cmr_lon_delta = abs(h5_bounding_polygon["lon"][i] - cmr_polygon["lon"][i])
        if cmr_lon_delta > 0.00000000001:
            print(f'MISMATCH VALUE - h5 bounding polygon longitude {i} does not match: {h5_bounding_polygon["lon"][i]} != {cmr_polygon["lon"][i]}')

    # for each beam
    for beam in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
        # get beam dataframe
        track = beam[2]
        pair = beam[3]
        spot = INFO_TO_SPOT[sc_orient][track][pair]
        beam_df = df[df["spot"] == spot]

        # get h5 arrays
        h5_lats = f[beam]["heights"]["lat_ph"][:]
        h5_lons = f[beam]["heights"]["lon_ph"][:]
        h5_distacross = f[beam]["heights"]["dist_ph_across"][:]
        h5_distalong = f[beam]["heights"]["dist_ph_along"][:]
        h5_distseg = f[beam]["geolocation"]["segment_dist_x"][:]
        h5_h = f[beam]["heights"]["h_ph"][:]
        h5_geoid = f[beam]["geophys_corr"]["geoid"][:]
        h5_solar = f[beam]["geolocation"]["solar_elevation"][:]

        # get df series
        df_lats = beam_df["lat_ph"]
        df_lons = beam_df["lon_ph"]
        df_ph = beam_df["index_ph"]
        df_seg = beam_df["index_seg"]
        df_class = beam_df["ensemble"]
        df_surface = beam_df["surface_h"]
        df_ortho = beam_df["ortho_h"]
        df_y_atc = beam_df["y_atc"]
        df_x_atc = beam_df["x_atc"]
        df_geoid = beam_df["geoid_corr_h"]
        df_ellipse = beam_df["ellipse_h"]
        df_flags = beam_df["processing_flags"]

        # initialize refraction stats
        lat_refraction_acc = 0.0
        lat_refraction_max = 0.0
        lon_refraction_acc = 0.0
        lon_refraction_max = 0.0

        # check values
        for i in range(len(df_ph)):

            # x_atc
            x_delta = abs(h5_distalong[df_ph.iloc[i]] + h5_distseg[df_seg.iloc[i]] - df_x_atc.iloc[i])
            if(x_delta != 0.0):
                print(f'MISMATCH VALUE - {beam} x_atc on row {i} at photon index {df_ph.iloc[i]}: {x_delta}')

            # y_atc
            y_delta = abs(h5_distacross[df_ph.iloc[i]] - df_y_atc.iloc[i])
            if(y_delta != 0.0):
                print(f'MISMATCH VALUE - {beam} y_atc on row {i} at photon index {df_ph.iloc[i]}: {y_delta}')

            # geoid_corr_h
            geoid_delta = abs((h5_h[df_ph.iloc[i]] - h5_geoid[df_seg.iloc[i]]) - df_geoid.iloc[i])
            if(geoid_delta != 0.0):
                print(f'MISMATCH VALUE - {beam} geoid on row {i} at photon index {df_ph.iloc[i]}: {geoid_delta}')

            # ellipse_h
            ellipse_delta = abs(h5_h[df_ph.iloc[i]] - df_ellipse.iloc[i])
            if df_geoid.iloc[i] > df_surface.iloc[i] or df_class.iloc[i] == 41:
                if(ellipse_delta != 0.0):
                    print(f'MISMATCH VALUE - {beam} ellipse on row {i} at photon index {df_ph.iloc[i]}: {h5_h[df_ph.iloc[i]]} != {df_ellipse.iloc[i]} || {df_geoid.iloc[i]} {df_surface.iloc[i]} {df_class.iloc[i]}')

            # night flag
            is_night = h5_solar[df_seg.iloc[i]] < 5.0
            night_flag = df_flags.iloc[i] & NIGHT_FLAG != 0
            if is_night != night_flag:
                print(f'MISMATCH VALUE - {beam} night_flag on row {i} at photon index {df_ph.iloc[i]} and segment index {df_seg.iloc[i]}: {df_flags.iloc[i]} {h5_solar[df_seg.iloc[i]]}')

            # lat_ph, lon_ph
            lat_delta = abs(h5_lats[df_ph.iloc[i]] - df_lats.iloc[i])
            lon_delta = abs(h5_lons[df_ph.iloc[i]] - df_lons.iloc[i])
            if df_geoid.iloc[i] > df_surface.iloc[i] or df_class.iloc[i] == 41:
                if(lat_delta != 0.0 or lon_delta != 0.0):
                    print(f'MISMATCH VALUE - {beam} lat,lon on row {i} at photon index {df_ph.iloc[i]}: {lat_delta}, {lon_delta}, {df_ortho.iloc[i] - df_surface.iloc[i]}, {df_class.iloc[i]}')
            else:
                lat_refraction_acc += lat_delta
                if lat_delta > lat_refraction_max:
                    lat_refraction_max = lat_delta
                lon_refraction_acc += lon_delta
                if lon_delta > lon_refraction_max:
                    lon_refraction_max = lon_delta

        print(f'{beam} horizontal refraction - lat.avg, lon.avg, lat.max, lon.max: {lat_refraction_acc / len(h5_lats)}, {lon_refraction_acc / len(h5_lats)}, {lat_refraction_max}, {lon_refraction_max}')

#
# Function: check_refraction
#
def check_refraction():
    f = h5py.File(args.atl03_granule)
    df = pd.read_parquet(args.granule)
    sc_orient = f["orbit_info"]["sc_orient"][0]

    # for each beam
    for beam in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
        # get beam dataframe
        track = beam[2]
        pair = beam[3]
        spot = INFO_TO_SPOT[sc_orient][track][pair]
        beam_df = df[df["spot"] == spot]
        h5_refel = f[beam]["geolocation"]["ref_elev"][:]
        h5_refaz = f[beam]["geolocation"]["ref_azimuth"][:]
        df_lats = beam_df["lat_ph"]
        df_lons = beam_df["lon_ph"]
        df_ph = beam_df["index_ph"]
        df_seg = beam_df["index_seg"]
        df_class = beam_df["ensemble"]
        df_surface = beam_df["surface_h"]
        df_ortho = beam_df["ortho_h"]
        df_geoid = beam_df["geoid_corr_h"]
        df_e = beam_df["x_ph"]
        df_n = beam_df["y_ph"]
        ######################
        # get water indices
        ######################
        ri_water = [0.0 for _ in range(len(df_lats))]
        with rasterio.open("/data/cop_rep_ANNUAL_meanRI_d00.tif") as dataset:
            band = dataset.read(1, resampling=rasterio.enums.Resampling.nearest)
            for i in range(len(df_lats)):
                row, col = rasterio.transform.rowcol(dataset.transform, df_lons.iloc[i], df_lats.iloc[i])
                ri_water[i] = band[row, col] # see comments in read_ri_water.py
        ri_water = np.array(ri_water)
        ######################
        # calculate refraction
        ######################
        in_refel = np.array([h5_refel[df_seg.iloc[i]] for i in range(len(df_ph))]).astype(np.float32)
        in_refaz = np.array([h5_refaz[df_seg.iloc[i]] for i in range(len(df_ph))]).astype(np.float32)
        dE, dN, dZ = photon_refraction(df_surface, df_geoid, in_refaz, in_refel, 1.00029, ri_water)
        refracted_h = df_geoid + dZ
        easting = df_e + dE
        northing = df_n + dN
#        parquet_file = pq.ParquetFile(args.granule)
#        metadata = json.loads(parquet_file.metadata.metadata[b'sliderule'])
#        utm_zone = metadata["utm_zone"]
        utm_zone = '326' + str(20)
        transformer = Transformer.from_crs(f"EPSG:{utm_zone}", "EPSG:4326", always_xy=True)
        ######################
        # check vertical
        ######################
        num_refracted_chk = 0
        acc_refracted_err = 0.0
        max_refracted_err = 0.0
        max_err_index = 0
        for i in range(len(df_geoid)):
            if df_geoid.iloc[i] < df_surface.iloc[i] and df_class.iloc[i] != 41:
                delta = abs(df_ortho.iloc[i] - refracted_h.iloc[i])
                acc_refracted_err += delta
                num_refracted_chk += 1
                if delta > 0.1:
                    print(f'MISMATCH REFRACTION {beam} on row {i} with segment index {df_seg.iloc[i]} and photon index {df_ph.iloc[i]}: {delta}')
                elif delta > max_refracted_err:
                    max_refracted_err = delta
                    max_err_index = i
        print(f'{beam} vertical refraction - avg, max, cnt, index: {acc_refracted_err / num_refracted_chk}, {max_refracted_err}, {num_refracted_chk}, {max_err_index}')
        ######################
        # check horizontal
        ######################
        num_refracted_chk = 0
        acc_refracted_err = 0.0
        max_refracted_err = 0.0
        max_err_index = 0
        for i in range(len(df_geoid)):
            if df_geoid.iloc[i] < df_surface.iloc[i] and df_class.iloc[i] != 41:
                longitude, latitude = transformer.transform(easting.iloc[i], northing.iloc[i])
                delta_lon = abs(longitude - df_lons.iloc[i])
                delta_lat = abs(latitude - df_lats.iloc[i])
                acc_refracted_err += delta_lon + delta_lat
                num_refracted_chk += 2
                if delta_lon > 0.1 or delta_lat > 0.1:
                    print(f'MISMATCH REFRACTION {beam} on row {i} with segment index {df_seg.iloc[i]} and photon index {df_ph.iloc[i]}: {delta_lon} {delta_lat}')
                elif delta_lon > max_refracted_err:
                    max_refracted_err = delta_lon
                    max_err_index = i
                elif delta_lat > max_refracted_err:
                    max_refracted_err = delta_lat
                    max_err_index = i
        print(f'{beam} horizontal refraction - avg, max, cnt, index: {acc_refracted_err / num_refracted_chk}, {max_refracted_err}, {num_refracted_chk}, {max_err_index}')


#
# Function: check_versions
#
def check_versions():
    global args

    # version comparison function
    def cmpver (key, value, granule):
        if VERSIONS[key] != value:
            print(f'INCORRECT version for {key} -> {value} != {VERSIONS[key]}: {granule}')

    # get list of files
    output_granules = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(args.bucket)
    for resource in bucket.objects.all():
        resource = str(resource.key)
        if resource.endswith(".parquet"):
            path_list = [args.bucket]
            if len(args.subfolder) > 0:
                path_list.append(args.subfolder)
            path_list.append(resource)
            file_path = '/'.join(path_list)
            output_granules.append("s3://" + file_path)

    # check each granule
    for granule in output_granules:
        print(f'checking granule: {granule}...')
        parquet_file = pq.ParquetFile(granule)
        sliderule = json.loads(parquet_file.metadata.metadata[b'sliderule'])
        # check all the versions
        cmpver("sliderule_version", sliderule["sliderule_version"], granule)


#############
# MAIN
#############

atl03_granules = args.atl03_granule and [args.atl03_granule] or database["granules"]
for atl03_granule in atl03_granules:
    analyze_atl24_result(atl03_granule)
    if args.plot_geometry != None:
        plot_geometry(atl03_granule, args.plot_geometry)