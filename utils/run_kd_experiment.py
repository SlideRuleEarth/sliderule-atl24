from sliderule import sliderule

session = sliderule.create_session(domain="localhost", cluster=None, verbose=True)

gdf = sliderule.run("atl24kd", {}, resources=["ATL03_20241107234251_08052501_007_01.h5"], session=session)

print(gdf)