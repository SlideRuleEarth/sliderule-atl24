--
-- ENDPOINT:    /source/atl24g2
--
local json      = require("json")
local rqst      = json.decode(arg[1])

-- override atl24 request parameters
rqst["parms"]["atl24"] = {
    compact = false,
    class_ph = {"unclassified", "bathymetry", "sea_surface"},
    anc_fields = {"index_ph", "index_seg"}
}

-- create global objects
local start_time    = time.gps() -- for timeout handling
local userlog       = msg.publish(rspq) -- for alerts
local parms         = icesat2.parms(rqst["parms"], rqst["key_space"], "icesat2-atl24", rqst["resource"])
local atl24h5       = h5.object(parms["asset"], parms["resource"])

-- create atl24 dataframes from release 01 granules
local dataframes = {}
for _, beam in ipairs(parms["beams"]) do
    dataframes[beam] = icesat2.atl24x(beam, parms, atl24h5, rspq)
end

-- add frame runners to dataframes
for beam, df in pairs(dataframes) do
    df:run(core.TERMINATE)
end

-- create atl24 release 01 granule
local granule = icesat2.atl24granule(parms, atl24h5, rspq)

-- timeout helper function
local function ctimeout()
    local node_timeout = parms["node_timeout"]
    local current_timeout = (node_timeout * 1000) - (time.gps() - start_time)
    if current_timeout < 0 then current_timeout = 0 end
    local remaining_timeout = math.tointeger(current_timeout)
    return remaining_timeout
end

-- wait for dataframes to complete
for beam, df in pairs(dataframes) do
    local status = df:finished(ctimeout(), rspq)
    if status then
        if df:numrows() > 0 then
            userlog:alert(core.INFO, core.RTE_STATUS, string.format("request <%s> on %s generated dataframe [%s] with %d rows and %s columns", rspq, parms["resource"], beam, df:numrows(), df:numcols()))
        else
            userlog:alert(core.INFO, core.RTE_STATUS, string.format("request <%s> on %s generated empty dataframe [%s]", rspq, parms["resource"], beam))
        end
    else
        userlog:alert(core.ERROR, core.RTE_TIMEOUT, string.format("request <%s> on %s timed out waiting for dataframe [%s] to complete", rspq, parms["resource"], beam))
    end
end

-- wait for granule
 if not granule:waiton(ctimeout(), rspq) then
    userlog:alert(core.ERROR, core.RTE_TIMEOUT, string.format("request <%s> timed out waiting for granule to complete on %s", rspq, parms["resource"]))
    return
end

-- create atl24 release 02 granule
local tmp_filename = string.format("/tmp/atl24-%s.h5", rspq)
local atl24_file = atl24.hdf5file(parms, dataframes, granule)
atl24_file:write(tmp_filename)

-- send new atl24 releas 02 granule to user
arrow.send2user(tmp_filename, parms, rspq)