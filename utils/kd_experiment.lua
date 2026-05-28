-- initialization
local json          = require("json")
local aws_utils     = require("aws_utils")
local bathy_utils   = require("bathy_utils")

-- pull out arguments
local script = arg[1]
local resource, resource09 = arg[2]:match("([^,]+),([^,]+)")
local timeout = 600 * 1000

-- initialize results
local result = {
    info = string.format("executing %s, resource=%s, timeout=%d ms", script, resource, timeout),
    status = true,
    messages = {}
}

-- request structure
local rqst = {
    ["atl09_fields"] = {
        "low_rate/met_v10m",
        "low_rate/met_u10m"
    },
    ["output"] = {
        ["asset"] = "sliderule-stage",
        ["format"] = "geoparquet",
        ["path"] = string.format("%s.kd.v4.parquet", resource)
    }
}

repeat
    -- wait for NSIDC credentials
    if not aws_utils.wait_credentials("nsidc-cloud") then
        table.insert(result["messages"], "failed to get NSIDC credentials")
        result["status"] = false
        break
    end

    -- create objects used in processing granule
    local parms             = bathy.parms(rqst, nil, "icesat2", resource)
    local granule           = parms["granule"]
    local rdate             = string.format("%04d-%02d-%02dT00:00:00Z", granule["year"], granule["month"], granule["day"])
    local rgps              = time.gmt2gps(rdate)
    local bathymask         = bathy.mask()
    local atl03h5           = h5coro.object(parms["asset"], resource)
    local consoleq          = msg.subscribe("consoleq") -- prevents error posting to consoleq
    local atl09h5           = h5coro.object("icesat2-atl09", resource09)
    local atmo              = icesat2.atmo(parms, atl09h5)
    local kd490             = bathy_utils.get_viirs(parms, rgps)
    local kd_experiment     = atl24.kd_experiment(parms, kd490)
    local sender            = core.framesender("rspq", 0, timeout)
    local dataframe         = core.dataframe({}, {granule=resource, request=json.encode(rqst)})
    local dataframes        = {} -- holds beam dataframes

    -- build final dataframe from beam dataframes
    dataframe:receive("rspq", "consoleq", 6, timeout)
    for _, beam in ipairs(parms["beams"]) do
        local df = bathy.dataframe(beam, parms, bathymask, atl03h5, "consoleq")
        if df then
            df:run(atmo)
            df:run(kd_experiment)
            df:run(sender)
            df:run(core.TERMINATE)
            dataframes[beam] = df
        else
            table.insert(result["messages"], string.format("failed to create dataframe for beam %s", beam))
        end
    end

    -- wait for data to finish being read and deduplicated
    for beam, df in pairs(dataframes) do
        sys.log(core.CRITICAL, string.format("waiting for beam %s", beam))
        local status = df:finished(timeout)
        if status then
            table.insert(result["messages"], string.format("finished dataframe for beam %s", beam))
        else
            table.insert(result["messages"], string.format("failed to finish dataframe for beam %s", beam))
        end
    end

    -- send termination signal to final dataframe
    local rspq = msg.publish("rspq")
    rspq:sendstring("")

    -- wait for final dataframe (blocks until dataframe complete or timeout)
    if not dataframe:waiton(timeout) then
        table.insert(result["messages"], "failed to receive proxied dataframe")
        result["status"] = false
        break
    end

    -- check dataFrame constraints
    sys.log(core.CRITICAL, string.format("constructed final dataframe with %d rows, and %d columns", dataframe:numrows(), dataframe:numcols()))
    if dataframe:numrows() <= 0 or dataframe:numcols() <= 0 then
        table.insert(result["messages"], "produced an empty dataframe")
        result["status"] = false
        break
    end

    -- create arrow dataFrame
    local arrow_dataframe = arrow.dataframe(parms, dataframe)
    if not arrow_dataframe then
        table.insert(result["messages"], "failed to create arrow dataframe")
        result["status"] = false
        break
    end

    -- write dataFrame to parquet file
    local arrow_filename = arrow_dataframe:export()
    if not arrow_filename then
        table.insert(result["messages"], "failed to write dataframe")
        result["status"] = false
        break
    end

    -- send file to s3
    result["output"] = parms["output"]["path"]
    local status = core.send2user(arrow_filename, parms, "consoleq")
    if not status then
        table.insert(result["messages"], "failed to send dataframe")
        result["status"] = false
        break
    end
until true

-- return results
return json.encode(result), true