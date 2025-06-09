--
-- ENDPOINT:    /source/atl24g2
--

local json      = require("json")
local rqst      = json.decode(arg[1])
local parms     = icesat2.parms(rqst["parms"], rqst["key_space"], "icesat2-atl24", rqst["resource"])
local userlog   = msg.publish(rspq) -- for alerts
local df        = {}
local resource  = parms["resource"]
local atl24h5   = h5.object(parms["asset"], resource)

for _, beam in ipairs(parms["beams"]) do
    df[beam] = icesat2.atl24x(beam, parms, atl24h5, rspq)
    if not df[beam] then
        userlog:alert(core.CRITICAL, core.RTE_FAILURE, string.format("request <%s> on %s failed to create dataframe for beam %s", rspq, resource, beam))
    end
end

-- create and run framerunner to update classifications

-- create and run hdf5 writer

-- send to user