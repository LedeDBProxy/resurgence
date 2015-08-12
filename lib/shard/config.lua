module("shard.config", package.seeall)

SHARD_VERSION = "0.1"

local utils = require("shard.utils")
local ENV_CONFIG_FILE = "SHARD_CONFIG_FILE"

local _sharding_table = {}
local _tableKeyColumns = {}

--- Query the value of the given key.
-- @return the value of the given configuration key. An error is thrown if no value has been set.
function get(key)
    assert(_sharding_table, "No configuration loaded.")
    return _sharding_table[key]
end

--- Get all configuration values.
-- @return a table with all configuration values.
function getAllTableKeyColumns()
    assert(_tableKeyColumns, "No configuration loaded.")
    return _tableKeyColumns
end

function getShardingTable()
    return _sharding_table
end
--- Test the given key.
-- @return true if the given key exists.
function contains(key)
    assert(_sharding_table, "No configuration loaded.")
    return _sharding_table[key] ~= nil
end

-- Load the configuration.
function _load()
    local configFile = _G["shardConfigFile"]
    if (not configFile) then
        configFile = os.getenv(ENV_CONFIG_FILE)
    end
    if (not configFile) then
        configFile = "/etc/sharding.lua"
    end
     utils.debug("Using config file '" .. configFile .. "'")
    local success, result = pcall(loadfile, configFile)
    assert(success and result ~= nil, 
      "Error loading configuration file '" .. configFile .. "': " .. (tostring(result) or ""))
     utils.debug("Configuration '" .. configFile .. "' loaded.")
    result()
    
    print("sharding num in config:" .. #config.sharding_list)

    for i = 1, #config.sharding_list do
        _sharding_table[config.sharding_list[i].table] = config.sharding_list[i]
        _tableKeyColumns[config.sharding_list[i].table] = config.sharding_list[i].pkey
    end
end

-- Load the configuration upon first import.
_load()
