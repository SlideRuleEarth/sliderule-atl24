local runner = require("test_executive")
local aws_utils = require("aws_utils")

-- Configure Optional Tags
local tags = {}
for i=1,#arg do table.insert(tags, arg[i]) end
if #tags > 0 then
    runner.setscope(tags)
end

-- Configure Running In Cloud --
aws_utils.config_aws()

-- Execute Tests --
runner.script("atl24_writer.lua")
runner.script("atl24_uncertainty.lua")

-- Report Results --
local errors = runner.report()

-- Cleanup and Exit --
sys.quit( errors )
