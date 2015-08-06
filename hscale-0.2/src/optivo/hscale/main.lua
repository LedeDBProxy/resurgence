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
-- @see optivo.hscale.queryRewriter
-- @see optivo.hscale.stats
-- @see optivo.hscale.config
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-23 23:16:17 +0200 (Mi, 23 Apr 2008) $ $Rev: 40 $

tokenizer = require("proxy.tokenizer")
utils = require("optivo.common.utils")

require("optivo.hscale.queryAnalyzer")
require("optivo.hscale.queryRewriter")

-- Load configuration.
config = require("optivo.hscale.config")
tableKeyColumns = config.get("tableKeyColumns")

-- Instantiate the partition lookup module
partitionLookup = require(config.get("partitionLookupModule"))
partitionLookup.init(config)

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
-- A table containing rewrite rules for each column of a result set
local _resultSetReplace = nil
-- A table containing patterns for each column of a result set. If all patterns match for a row then the row will be removed.
local _resultSetRemove = nil

--- Analyze and rewrite queries.
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
    _resultSetReplace = nil
    _resultSetRemove = nil
    _query = nil

proxy.connection.wait_clt_next_sql = 30000
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
                        -- Send misc queries to the first partition
                        for _, tableName in pairs(_queryAnalyzer:getAffectedTables()) do
                            local partitionTable = partitionLookup.getAllPartitionTables(tableName)[1]
                            local rewriter = optivo.hscale.queryRewriter.QueryRewriter.create(tokens, {[tableName] = partitionTable})
                            local rewrittenQuery = rewriter:rewriteQuery()
                            proxy.queries:append(1, string.char(proxy.COM_QUERY) .. rewrittenQuery, { resultset_is_needed = true })
                            if (_queryAnalyzer:getStatementType() == "SHOW CREATE TABLE") then
                                _resultSetReplace = {
                                    {[partitionTable] = tableName},
                                    {["^CREATE TABLE (`?)" .. partitionTable] = "CREATE TABLE %1" .. tableName}
                                }
                            end
                            if (_queryAnalyzer:getStatementType() == "SHOW INDEX") then
                                _resultSetReplace = {
                                    {[partitionTable] = tableName}
                                }
                            end
                        end
                    elseif (_queryAnalyzer:isDdl()) then
                        stats.inc("rewrittenDdl")
                        -- We have a dtd query -> send it to all tables
                        for _, tableName in pairs(_queryAnalyzer:getAffectedTables()) do
                            for _, partitionTable in pairs(partitionLookup.getAllPartitionTables(tableName)) do
                                local rewriter = optivo.hscale.queryRewriter.QueryRewriter.create(tokens, {[tableName] = partitionTable})
                                local rewrittenQuery = rewriter:rewriteQuery()
                                 utils.debug("<<< Rewritten query #" .. _combinedNumberOfQueries .. ": '" .. rewrittenQuery .. "'")
                                _combinedNumberOfQueries = _combinedNumberOfQueries + 1
                                proxy.queries:append(_combinedNumberOfQueries, string.char(proxy.COM_QUERY) .. rewrittenQuery, { resultset_is_needed = true })
                             end
                        end
                    else
                        stats.inc("rewrittenDml")
                        utils.debug("--- check full partition scan '" .. _query .. "'")
                        if (_queryAnalyzer:isFullPartitionScanNeeded()) then
                            local tableName = _queryAnalyzer:getAffectedTables()[1]
                            stats.incFullPartitionScans(tableName)
                             utils.debug("--- Full partition scan needed for table '" 
                                   .. tableName .. "' and query '" .. _query .. "'")
                            _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                            for _, partitionTable in pairs(partitionLookup.getAllPartitionTables(tableName)) do
                                local rewriter = optivo.hscale.queryRewriter.QueryRewriter.create(tokens, {[tableName] = partitionTable})
                                rewriter:setStripLimitClause(true)
                                local rewrittenQuery = rewriter:rewriteQuery()
                                 utils.debug("<<< Full partition scan: rewritten query #" .. _combinedNumberOfQueries .. ": '" .. rewrittenQuery .. "'")
                                _combinedNumberOfQueries = _combinedNumberOfQueries + 1
                                proxy.queries:append(_combinedNumberOfQueries, string.char(proxy.COM_QUERY) .. rewrittenQuery, { resultset_is_needed = true })
                             end
                        else
                            local tableMapping = {}
                            if _queryAnalyzer:getShardType() == 0 then
                                for tableName, partitionValue in pairs(_queryAnalyzer:getTableKeyValues()) do
                                    tableMapping[tableName] = partitionLookup.getPartitionTable(tableName, partitionValue)
                                end
                                local rewriter = optivo.hscale.queryRewriter.QueryRewriter.create(tokens, tableMapping)
                                local rewrittenQuery = rewriter:rewriteQuery()
                                utils.debug("<<< Rewritten query: '" .. rewrittenQuery .. "'")
                                proxy.queries:append(1, string.char(proxy.COM_QUERY) .. rewrittenQuery, { resultset_is_needed = true })
                            else
                                _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                                for tableName, ranges in pairs(_queryAnalyzer:getTableKeyRange()) do
                                    tableMapping = partitionLookup.getRangePartitionTable(tableName, ranges, ranges:getRangeNum())
                                    utils.debug("<<< table map size:" .. #tableMapping)
                                    for _, partitionTable in pairs(tableMapping) do
                                        utils.debug("<<< tableName: '" .. tableName .. "'")
                                        utils.debug("<<< partitionTable: '" .. partitionTable .. "'")
                                        local rewriter = optivo.hscale.queryRewriter.QueryRewriter.create(tokens, {[tableName] = partitionTable})
                                        local rewrittenQuery = rewriter:rewriteQuery()
                                        utils.debug("<<< Rewritten query: '" .. rewrittenQuery .. "'")
                                        _combinedNumberOfQueries = _combinedNumberOfQueries + 1
                                        utils.debug("<<< _combinedNumberOfQueries: '" .. _combinedNumberOfQueries .. "'")
                                        proxy.queries:append(_combinedNumberOfQueries, string.char(proxy.COM_QUERY) .. rewrittenQuery, { resultset_is_needed = true })

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
    elseif (_resultSetReplace or _resultSetRemove) then
        -- Rewrite the result set if needed
         utils.debug("Rewriting result for query '" .. _query .. "'", 1)
        local resultSet = proxy.response.resultset
        if (not resultSet) then
            resultSet = inj.resultset
        end
        if (resultSet and resultSet.rows and resultSet.fields) then
            local rows = resultSet.rows
            local newRows = {}
            local newFields = _getFields(resultSet)
            for row in rows do
                local newRow = {}
                local removeRow = nil
                for col = 1, #newFields do
                    value = row[col]
                    if (value ~= nil) then
                        if (_resultSetReplace and col <= #_resultSetReplace) then
                            for expr, repl in pairs(_resultSetReplace[col]) do
                                 utils.debug("Applying replacement (" .. expr .. " => " .. repl .. " on col #" .. col .. " => " .. value, 2)
                                value = string.gsub(tostring(value), expr, repl)
                            end
                        end
                        if (_resultSetRemove and col <= #_resultSetRemove) then
                            for pattern in pairs(_resultSetRemove) do
                                local match = string.find(tostring(value), pattern)
                                 utils.debug("Remove row pattern '" .. pattern .. "' on value '" .. value .. " => " .. match, 2)
                                if (not match) then
                                    removeRow = false
                                elseif (removeRow == nil) then
                                    removeRow = true
                                end
                            end
                        end
                    end
                    table.insert(newRow, col, value)
                end
                if (removeRow ~= true) then
                    table.insert(newRows, newRow)
                end
            end
            proxy.response.type = proxy.MYSQLD_PACKET_OK
            proxy.response.resultset = {
                fields = newFields,
                rows = newRows
            }
            return proxy.PROXY_SEND_RESULT
        end
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
                    utils.debug("we have row here")
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
            _combinedResultSet.affected_rows = _combinedResultSet.affected_rows + tonumber(resultSet.affected_rows)
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
