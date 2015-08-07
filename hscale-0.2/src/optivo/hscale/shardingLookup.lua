module("optivo.hscale.shardingLookup", package.seeall)

local utils = require("optivo.common.utils")

local _shardingTable

function init(config)
    _shardingTable = config.getShardingTable()
    assert(_shardingTable, "Option 'sharding_list' missing.")
end


--- Query all available groups for a given table.
-- @return all sharding names.
function getAllShardingGroups(tableName)
    local tables = {}
    local num = #(_shardingTable[tableName].partitions)
    for i = 1, num do
        table.insert(tables, _shardingTable[tableName].partitions[i].group)
    end
    return tables
end

--- Query a sharding group .
-- @return the sharding group for the given partition key
function getShardingGroup(tableName, key)
    assert(_shardingTable[tableName], "Table '" .. tableName .. "' is not supported.")
    local partions = #(_shardingTable[tableName].partitions)
    local number = tonumber(key)
    if (not number) then
        number = utils.calculateSimpleChecksum(tostring(key))
    end
    
    local index = (number % partitions)

    return _shardingTable[tableName].partitions[index].group
end

function getShardingGroup(tableName, range)
    assert(_shardingTable[tableName], "Table '" .. tableName .. "' is not supported.")

    local shard_info = _shardingTable[tableName]

    local result={}
    if shard_info.shard_type ~= "int" then
        return result
    end

    local partitions = #shard_info.partitions
    local min_value = range:getRangeMinValue()
    local max_value = range:getRangeMaxValue()

    if (min_value < 0 and max_value < 0) then
        return result
    end

    for i = 1, partitions do
        local partition_max_value = shard_info.partitions[i].value
        if max_value >= 0 and max_value <= partition_max_value then
            table.insert(result, shard_info.partitions[i].group)
            break
        elseif min_value <= partition_max_value then
            table.insert(result, shard_info.partitions[i].group)
        end
    end

    return result
end
