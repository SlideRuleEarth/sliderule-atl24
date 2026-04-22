-------------------------------------------------------
-- main
-------------------------------------------------------
local function main()
    local json          = require("json")
    local dataframe     = require("dataframe")
    local icesat2_utils = require("icesat2_utils")
    local bathy_utils   = require("bathy_utils")
    local rqst          = json.decode(arg[1])
    local parms         = bathy.parms(rqst["parms"], rqst["key_space"], "icesat2", rqst["resource"])
    local granule       = parms["granule"]
    local rdate         = string.format("%04d-%02d-%02dT00:00:00Z", granule["year"], granule["month"], granule["day"])
    local rgps          = time.gmt2gps(rdate)
    local channels      = 6 -- number of dataframes per resource
    -- proxy request
    dataframe.proxy("atl24kd", parms, rqst["parms"], _rqst.rspq, channels, function(userlog)
        local resource          = parms["resource"]
        local bathymask         = bathy.mask()
        local atl03h5           = h5coro.object(parms["asset"], resource)
        local atl09_granule,_   = icesat2_utils.find_atl09_granule(parms, userlog)
        local atl09h5           = h5coro.object("icesat2-atl09", atl09_granule)
        local atmo              = icesat2.atmo(parms, atl09h5)
        local kd490             = bathy_utils.get_viirs(parms, rgps)
        local kd_experiment     = atl24.kd_experiment(parms, kd490)
        local runners           = {atmo, kd_experiment}
        local dataframes        = {}
        for _, beam in ipairs(parms["beams"]) do
            dataframes[beam] = bathy.dataframe(beam, parms, bathymask, atl03h5, atl09h5, _rqst.rspq)
            if not dataframes[beam] then
                userlog:alert(core.CRITICAL, core.RTE_FAILURE, string.format("request <%s> on %s failed to create bathy dataframe for beam %s", _rqst.rspq, resource, beam))
            end
        end
        return dataframes, runners
    end)
end

-------------------------------------------------------
-- endpoint
-------------------------------------------------------
return {
    main = main,
    name = "Kd Experiment Dataframe",
    description = "Run the Kd experiment and generate results (x-series)",
    logging = core.CRITICAL,
    roles = {},
    signed = false,
    outputs = {"binary", "arrow"}
}