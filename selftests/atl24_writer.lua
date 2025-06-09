local runner = require("test_executive")
local prettyprint = require("prettyprint")

-- Self Test --

runner.unittest("ATL24 HDF5 Writer", function()
    local atl24_file = atl24.hdf5file()
    runner.check(atl24_file:write("/tmp/atl24.h5"))
end)

-- Report Results --

runner.report()

