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

-- @class optivo.hscale.main
--- Main module.
--
-- This is the actual MySQL Proxy module. Each query received will be analyzed and rewritten
-- if nescessary. If a full partition scan is nescessary then for each partition a query
-- is sent to the server and the result set is combined afterwards. <br />
--
-- Administrative commands are recognized and executed too.
--
-- @see optivo.hscale.admin
-- @see optivo.hscale.queryAnalyzer
-- @see optivo.hscale.stats
-- @see optivo.hscale.config
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-23 23:16:17 +0200 (Mi, 23 Apr 2008) $ $Rev: 40 $

tokenizer = require("proxy.tokenizer")
utils = require("optivo.common.utils")

require("optivo.hscale.queryAnalyzer")

-- Load configuration.
config = require("optivo.hscale.config")
tableKeyColumns = config.getAllTableKeyColumns()

shardingLookup = require("optivo.hscale.shardingLookup")
shardingLookup.init(config)

-- Statistics
stats = require("optivo.hscale.stats")

-- Admin module
admin = require("optivo.hscale.admin")
admin.init(config)

utils.debug("NEW CONNECTION")

-- Local variable to hold result for multiple queries
local _combinedResultSet = {}
local _combinedNumberOfQueries = 0
local _combinedLimit = {}
local _query = nil
local _queryAnalyzer = nil

function read_query(packet)
    _combinedResultSet = {}
    _combinedResultSet.fields = nil
    _combinedResultSet.rows = nil
    _combinedResultSet.affected_rows = 0
    _combinedNumberOfQueries = 0
    _combinedLimit = {}
    _combinedLimit.rowsProcessed = 0
    _combinedLimit.rowsSent = 0
    _combinedLimit.from = 0
    _combinedLimit.rows = 0
    _query = nil

    proxy.connection.wait_clt_next_sql = 60000

    if packet:byte() == proxy.COM_QUERY then
        _query = packet:sub(2)
        stats.inc("queries")
        utils.debug(">>> Analyzing query '" .. _query .. "'")
        -- Quick test if the query contains partitioned tables - if not - don't parse it (HSCALE-31)
        local queryLower = _query:lower()
        local doScan = string.find(queryLower, "hscale") ~= nil
        if (not doScan) then
            for tableName, _ in pairs(tableKeyColumns) do
                if (string.find(queryLower, tableName:lower())) then
                    doScan = true
                    break
                end
            end
        end
        if (doScan) then
            local tokens = tokenizer.tokenize(_query)
            _queryAnalyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(tokens, tableKeyColumns)
            local success, errorMessage = pcall(_queryAnalyzer.analyze, _queryAnalyzer)
            if (success) then
                if (_queryAnalyzer:isPartitioningNeeded()) then
                    if (_queryAnalyzer:isMisc()) then
                        stats.inc("rewrittenMisc")
                        -- TODO Send misc queries to the first partition
                        proxy.queries:append(1, string.char(proxy.COM_QUERY) .. _query, 
                        { resultset_is_needed = true })
                    elseif (_queryAnalyzer:isDdl()) then
                        stats.inc("rewrittenDdl")
                        -- We have a dtd query -> send it to all groups
                        for _, tableName in pairs(_queryAnalyzer:getAffectedTables()) do
                            for _, group in pairs(shardingLookup.getAllShardingGroups(tableName)) do
                                _combinedNumberOfQueries = _combinedNumberOfQueries + 1
                                proxy.queries:append(_combinedNumberOfQueries, 
                                string.char(proxy.COM_QUERY) .._query, { resultset_is_needed = true })
                            end
                        end
                    else
                        stats.inc("rewrittenDml")
                        utils.debug("--- check full scan '" .. _query .. "'")
                        if (_queryAnalyzer:isFullPartitionScanNeeded()) then
                            utils.debug("--- full scan '" .. _query .. "'")
                            local tableName = _queryAnalyzer:getAffectedTables()[1]
                            stats.incFullPartitionScans(tableName)
                            _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                            for _, group in pairs(shardingLookup.getAllShardingGroups(tableName)) do
                                _combinedNumberOfQueries = _combinedNumberOfQueries + 1
                                proxy.queries:append(_combinedNumberOfQueries, 
                                      string.char(proxy.COM_QUERY) .._query, { resultset_is_needed = true })
                             end
                        else
                            utils.debug("--- not full scan '" .. _query .. "'")
                            local tableMapping = {}
                            if _queryAnalyzer:getShardType() == 0 then
                                for tableName, key in pairs(_queryAnalyzer:getTableKeyValues()) do
                                    tableMapping[tableName] = shardingLookup.getShardingGroup(tableName, key)
                                end
                                proxy.queries:append(1, string.char(proxy.COM_QUERY) .. _query, 
                                      { resultset_is_needed = true })
                            else
                                utils.debug("--- range sharding '" .. _query .. "'")
                                _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                                for tableName, range in pairs(_queryAnalyzer:getTableKeyRange()) do
                                    tableMapping = shardingLookup.getShardingGroup(tableName, range)
                                    utils.debug("<<< table map size:" .. #tableMapping)
                                    -- if not found sharding table, what to do ? full scan ?
                                    for _, group in pairs(tableMapping) do
                                        utils.debug("<<< tableName: '" .. tableName .. "'")
                                        utils.debug("<<< group: '" .. group .. "'")
                                        _combinedNumberOfQueries = _combinedNumberOfQueries + 1
                                        proxy.queries:append(_combinedNumberOfQueries, 
                                        string.char(proxy.COM_QUERY) .. _query, { resultset_is_needed = true})
                                    end
                                end
                            end
                        end
                    end
                    return proxy.PROXY_SEND_QUERY
                else
                    local result = admin.execAdmin(tokens)
                    if (result) then
                        proxy.response = {
                            type = proxy.MYSQLD_PACKET_OK,
                            resultset = result
                        }
                        return proxy.PROXY_SEND_RESULT
                    else
                        return proxy.PROXY_SEND_QUERY
                    end
                end
            end
            if (not success) then
                stats.inc("invalidQueries")
                proxy.response = {
                    type     = proxy.MYSQLD_PACKET_ERR,
                    errmsg   = "HSCALE-1000: " .. errorMessage .. " - query was '" .. _query .."'"
                }
                return proxy.PROXY_SEND_RESULT
            end
        end
    end
end

--- Send the result back to the client.
function read_query_result(inj)
    utils.debug("Got result for query '" .. _query .. "'")
    local success, result = pcall(_buildUpCombinedResultSet, inj)
    if (not success) then
        stats.inc("invalidResults")
        proxy.response = {
            type     = proxy.MYSQLD_PACKET_ERR,
            errmsg   = "HSCALE-1001: Error assembling result set: " .. result .. " Query: '" .. _query .. "'"
        }
        _combinedNumberOfQueries = 0
        return proxy.PROXY_SEND_RESULT
    end
    if (result) then
        return result
    end
end

-- Extract the "fields" part out of the result set.
-- @return nil if there is no field set
function _getFields(resultSet)
    local newFields = nil
    local fieldCount = 1
    local fields = resultSet.fields
    if (fields) then
        newFields = {}
        while fields[fieldCount] do
            table.insert(
                newFields,
                {
                    type = fields[fieldCount].type,
                    name = fields[fieldCount].name
                }
            )

            fieldCount = fieldCount + 1
        end
    end
    return newFields
end

-- Aggregate the different result sets.
function _buildUpCombinedResultSet(inj)
    utils.debug("_combinedNumberOfQueries:'" .. _combinedNumberOfQueries .. "'", 1)
    if (_combinedNumberOfQueries > 0) then
        local resultSet = assert(inj.resultset, "Something went terribly wrong, got NULL result set.")
        if (resultSet.fields) then
            assert(#(resultSet.fields) > 0, "Something went terribly wrong, got zero length fields.")
            -- We have a result set
            if (not _combinedResultSet.fields) then
                -- Build up the fields part
                _combinedResultSet.rows = {}
                _combinedResultSet.fields = _getFields(resultSet)
            end
            -- Add result respecting LIMIT constraints
            if (resultSet.rows) then
                for row in resultSet.rows do
                    if (
                        (_combinedLimit.rows < 0 or _combinedLimit.rowsSent < _combinedLimit.rows)
                        and
                        (_combinedLimit.from < 0 or _combinedLimit.rowsProcessed >= _combinedLimit.from)
                    ) then
                        table.insert(_combinedResultSet.rows, row)
                        _combinedLimit.rowsSent = _combinedLimit.rowsSent + 1
                    end
                    _combinedLimit.rowsProcessed = _combinedLimit.rowsProcessed + 1
                end
                -- Shortcut - if the LIMIT clause has been fullfilled - don't send any further queries.
                if (_combinedLimit.rows > 0 and _combinedLimit.rowsSent >= _combinedLimit.rows) then
                    _combinedNumberOfQueries = inj.id
                    proxy.queries:reset()
                end
            end
        else
            utils.debug("resultSet.fields is null", 1)
        end
        if (resultSet.affected_rows) then
            _combinedResultSet.affected_rows = _combinedResultSet.affected_rows + 
                                                tonumber(resultSet.affected_rows)
        end

        if (resultSet.query_status and (resultSet.query_status < 0)) then
            _combinedNumberOfQueries = inj.id
            proxy.queries:reset()
        end

        utils.debug("_combinedLimit.rows:" .. _combinedLimit.rows)
        utils.debug("affected_rows:" .. _combinedResultSet.affected_rows)
        utils.debug("_combinedNumberOfQueries in result" .. _combinedNumberOfQueries)
        utils.debug("inj id " .. inj.id)
        if (inj.id == _combinedNumberOfQueries) then
            -- This has been the last result set - send all back to client
            if (_combinedResultSet.fields) then
                proxy.response.type = proxy.MYSQLD_PACKET_OK
                proxy.response.resultset = _combinedResultSet
            else
                proxy.response.type = proxy.MYSQLD_PACKET_RAW;
                proxy.response.packets = {
                    "\000" .. -- fields
                    string.char(_combinedResultSet.affected_rows) ..
                    "\000" .. -- insert_id
                    inj.resultset.raw:sub(4)
                }
            end
            _combinedNumberOfQueries = 0
            return proxy.PROXY_SEND_RESULT
        end
        -- Ignore all result sets until we are at the last one
        return proxy.PROXY_IGNORE_RESULT
    end
end

