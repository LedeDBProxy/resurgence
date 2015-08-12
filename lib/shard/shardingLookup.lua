module("shard.shardingLookup", package.seeall)

local utils = require("shard.utils")

local _shardingTable

function init(config)
    _shardingTable = config.getShardingTable()
    assert(_shardingTable, "Option 'sharding_list' missing.")
end


--- Query all available groups for a given table.
-- @return all sharding names.
function getAllShardingGroups(tableName, groups)
    local num = #(_shardingTable[tableName].partitions)
    for i = 1, num do
        groups.insert(tables, _shardingTable[tableName].partitions[i].group)
    end
end

--- Query a sharding group .
-- @return the sharding group for the given partition key
function getShardingGroupByHash(tableName, key)
    assert(_shardingTable[tableName], "Table '" .. tableName .. "' is not supported.")
    local partions = #(_shardingTable[tableName].partitions)
    local number = tonumber(key)
    if (not number) then
        number = utils.calculateSimpleChecksum(tostring(key))
    end
    
    local index = (number % partitions)

    return _shardingTable[tableName].partitions[index].group
end

function getShardingGroupByRange(tableName, range, groups)
    assert(_shardingTable[tableName], "Table '" .. tableName .. "' is not supported.")

    local shard_info = _shardingTable[tableName]

    if shard_info.shard_type ~= "int" then
        return 
    end

    local partitions = #shard_info.partitions
    local min_value = range:getRangeMinValue()
    local max_value = range:getRangeMaxValue()

    if (min_value < 0 and max_value < 0) then
        return 
    end

    for i = 1, partitions do
        local partition_max_value = shard_info.partitions[i].value
        if max_value >= 0 and max_value <= partition_max_value then
            table.insert(groups, shard_info.partitions[i].group)
            break
        elseif min_value <= partition_max_value then
            table.insert(groups, shard_info.partitions[i].group)
        end
    end
end
