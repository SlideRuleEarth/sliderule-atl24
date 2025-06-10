--
-- ENDPOINT:    /source/atl24g2
--

local json      = require("json")
local rqst      = json.decode(arg[1])

rqst["parms"]["atl24"] = {
    compact = false,
    anc_fields = {"index_ph", "index_seg"}
}

local parms     = icesat2.parms(rqst["parms"], rqst["key_space"], "icesat2-atl24", rqst["resource"])
local userlog   = msg.publish(rspq) -- for alerts
local atl24h5   = h5.object(parms["asset"], parms["resource"])
local granule   = icesat2.atl24granule(parms, atl24h5, rspq)

local df = {}
for _, beam in ipairs(parms["beams"]) do
    df[beam] = icesat2.atl24x(beam, parms, atl24h5, rspq)
    if not df[beam] then
        userlog:alert(core.CRITICAL, core.RTE_FAILURE, string.format("request <%s> on %s failed to create dataframe for beam %s", rspq, parms["resource"], beam))
    end
end

-- create and run framerunner to update classifications

-- create and run hdf5 writer

-- send to user