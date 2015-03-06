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

--- Test the query analyzer. <br />
--
-- This module will simply return a result set containing the partition keys found.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-11 23:30:14 +0200 (Fr, 11 Apr 2008) $ $Rev: 30 $

local tokenizer = require("proxy.tokenizer")
require("optivo.hscale.queryAnalyzer")

-- Configuration
local TABLE_KEY_COLUMNS = {
    ["a"] = "category",
    ["b"] = "partition",
    ["c"] = "partition"};


function read_query( packet )
	if packet:byte() ~= proxy.COM_QUERY then
	else
        local query = packet:sub(2)
        local tokens = tokenizer.tokenize(query)
        local queryAnalyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(tokens, TABLE_KEY_COLUMNS)
        local success, errorMessage = pcall(queryAnalyzer.analyze, queryAnalyzer)
        local resultRows = {}
        if (success) then
            if (queryAnalyzer:isDdl()) then
                for _, tableName in pairs(queryAnalyzer:getAffectedTables()) do
                    table.insert(resultRows, {tableName, '---'})
                end
            elseif (queryAnalyzer:isFullPartitionScanNeeded()) then
                for _, tableName in pairs(queryAnalyzer:getAffectedTables()) do
                    table.insert(resultRows, {tableName, 'full scan'})
                end
            else
                for name, value in pairs(queryAnalyzer:getTableKeyValues()) do
                    table.insert(resultRows, {name, value})
                end
            end
            proxy.response.resultset = {
                fields = {
                    {type = proxy.MYSQL_TYPE_STRING, name = "table"},
                    {type = proxy.MYSQL_TYPE_STRING, name = "partition key"}
                },
                rows = resultRows
            }
        else
            -- An error has occurred
            -- print (">>>> ERROR: " .. errorMessage)
            proxy.response.resultset = {
                fields = {
                    {type = proxy.MYSQL_TYPE_STRING, name = "invalid query"},
                },
                rows = {nil}
            }
        end

        proxy.response.type = proxy.MYSQLD_PACKET_OK
        return proxy.PROXY_SEND_RESULT
    end
end

function disconnect_client()
end
