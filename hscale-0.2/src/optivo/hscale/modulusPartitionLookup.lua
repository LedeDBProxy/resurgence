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
local _partitionsPerTable

--- Initialize the module using the current configuration. <br />
-- The following configuration parameters are required:
-- <ul>
--     <li><code>tableKeyColumns</code> a map with table => column</li>
--     <li><code>partitionsPerTable</code> the maximum number of partitions per table
-- </ul>
-- @see optivo.hscale.config
function init(config)
    _tableKeyColumns = config.get("tableKeyColumns")
    _partitionsPerTable = config.get("partitionsPerTable")
    assert(_tableKeyColumns, "Option 'tableKeyColumns' missing.")
    assert(_partitionsPerTable > 0, "Invalid value for option partitionsPerTable.")
end

--- Query the table / key combinations.
function getTableKeyColumns()
    return _tableKeyColumns
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
