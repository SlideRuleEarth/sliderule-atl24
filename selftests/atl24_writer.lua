local runner = require("test_executive")
local prettyprint = require("prettyprint")

-- Setup --

runner.authenticate()

-- Self Test --

runner.unittest("ATL24 HDF5 Writer", function()
    local parms = icesat2.parms({
        atl24 = {
            compact = false,
            class_ph = {"unclassified", "bathymetry", "sea surface"},
            anc_fields = {"index_ph", "index_seg"}
        }
    }, nil, "icesat2-atl24", "ATL24_20241107234251_08052501_006_01_001_01.h5")
    local atl24h5   = h5.object(parms["asset"], parms["resource"])
    local granule   = icesat2.atl24granule(parms, atl24h5, rspq)
    local gt1l      = icesat2.atl24x("gt1l", parms, atl24h5, rspq)
    local status    = gt1l:waiton(10000, rspq)
    runner.assert(status, "failed to create dataframe", true)
    local df = {gt1l=gt1l}
    local atl24_file = atl24.hdf5file(df)
    runner.assert(atl24_file:write("/tmp/atl24.h5"))
end)

-- Report Results --

runner.report()

