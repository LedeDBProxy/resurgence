--[[
   Copyright (C) 2008 optivo GmbH

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
--]]

---	A partition lookup service based on modulus. <br />
--
-- This implementation just does a modulo on the partition value. If the value is anything but
-- a number then it will be converted to a string and a checksum will be calculated.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-23 12:03:59 +0200 (Mi, 23 Apr 2008) $ $Rev: 37 $

module("optivo.hscale.modulusPartitionLookup", package.seeall)

local utils = require("optivo.common.utils")

local _tableKeyColumns
local _tableKeyColumnRanges
local _partitionsPerTable
local _sharding_table

--- Initialize the module using the current configuration. <br />
-- The following configuration parameters are required:
-- <ul>
--     <li><code>tableKeyColumns</code> a map with table => column</li>
--     <li><code>partitionsPerTable</code> the maximum number of partitions per table
-- </ul>
-- @see optivo.hscale.config
function init(config)
    _tableKeyColumns = config.get("tableKeyColumns")
    _tableKeyColumnRanges = config.get("tableKeyColumnRanges")
    _partitionsPerTable = config.get("partitionsPerTable")
    _sharding_table = config.getShardingTable()
    assert(_tableKeyColumns, "Option 'tableKeyColumns' missing.")
    assert(_partitionsPerTable > 0, "Invalid value for option partitionsPerTable.")
end

--- Query the table / key combinations.
function getTableKeyColumns()
    return _tableKeyColumns
end

function getTableKeyColumnRanges()
    return _tableKeyColumnRanges
end
--- Query all available partitions for a given table.
-- @return all partition table names.
function getAllPartitionTables(tableName)
    local tables = {}
    for a = 0, _partitionsPerTable -1 do
        table.insert(tables, tableName .. "_" .. tostring(a))
    end
    return tables
end

--- Query a partition.
-- @return the partition table for the given partition key
function getPartitionTable(tableName, partitionKey)
    assert(_tableKeyColumns[tableName], "Table '" .. tableName .. "' is not supported.")
    local number = tonumber(partitionKey)
    if (not number) then
        number = utils.calculateSimpleChecksum(tostring(partitionKey))
    end
    if (number) then
        partitionTableSuffix = tostring(number % _partitionsPerTable)
    end

    return tableName .. "_" .. partitionTableSuffix
end

function getRangePartitionTable(tableName, ranges, num)
    assert(_sharding_table[tableName], "Table '" .. tableName .. "' is not supported.")

    local shard_info = _sharding_table[tableName]

    local result={}
    if shard_info.shard_type ~= "int" then
        return result
    end

    local partition_num = #shard_info.partitions

    for i = 1, num do
        local op = ranges:getOpType(i)
        local value = tonumber(ranges:getRangeValue(i))
        local group = 0
        local suffix 
        local hit_area = 0

        if op == "TK_LT" then
            value = value - 1 
            op = "TK_LE"
        elseif op == "TK_GT" then
            value = value + 1
            op = "TK_GE"
        end

        for j = 1, partition_num do
            if value <= shard_info.partitions[j].value then
                suffix = shard_info.partitions[j].suffix
                group = shard_info.partitions[j].group
                hit_area = j
                break
            end
        end
        if hit_area == 0 then
            return result
        end
        utils.debug("--- group value:" .. group)
        utils.debug("--- suffix:" .. suffix)
        utils.debug("--- op:" .. op)

        table.insert(result, tableName .. suffix)

        if op == "TK_GE" then
            for m = hit_area + 1, partition_num do
                if shard_info.partitions[m] ~= nil then
                    table.insert(result, tableName .. shard_info.partitions[m].suffix)
                end
            end
        elseif op == "TK_LE" then
            for m = 1, hit_area - 1 do
                if shard_info.partitions[m] ~= nil then
                    table.insert(result, tableName .. shard_info.partitions[m].suffix)
                end
            end
        end
    end

    return result
end
