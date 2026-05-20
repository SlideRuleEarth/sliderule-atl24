from sliderule import sliderule

session = sliderule.create_session(domain="localhost", cluster=None, verbose=True)

gdf = sliderule.run("atl24kd", {
    "atl09_fields": ["low_rate/met_v10m", "low_rate/met_u10m"],
    "output": {
        "format": "geoparquet",
        "path": "ATL03_20241107234251_08052501_007_01_kd_v4.parquet",
        "open_on_complete": True
    }
}, resources=["ATL03_20241107234251_08052501_007_01.h5"], session=session)

print(gdf)