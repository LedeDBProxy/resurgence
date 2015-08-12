module("shard.stats", package.seeall)

-- Value holder
proxy.global.shardingStats = {}
values = proxy.global.shardingStats
values.queries = 0
values.rewrittenMisc = 0
values.rewrittenShow = 0
values.rewrittenDdl = 0
values.rewrittenDml = 0
values.fullPartitionScans = {}
values.invalidQueries = 0
values.invalidResults = 0
values.adminQueries = 0


--- Increment a statistic key. An error is thrown if the key is unknown.
function inc(key)
    assert(values[key] ~= nil, "Unknown statistic key '" .. key .. "'.")
    values[key] = values[key] + 1
end

--- Signal a full partition scan
function incFullPartitionScans(tableName)
    local value = values.fullPartitionScans[tableName]
    if (value == nil) then
        value = 0
    end
    values.fullPartitionScans[tableName] = value + 1
end

--- Create a result set representation with all status information.
-- @return a table which can be used as the "rows part" of a proxy result set.
function getStatusAsResultTable()
    local result = {}
    local fullPartitionScans = 0
    for tab, count in pairs(values.fullPartitionScans) do
        fullPartitionScans = fullPartitionScans + count
    end
    table.insert(result, {"Hscale_admin_commands", values.adminQueries})
    table.insert(result, {"Hscale_full_partition_scans", fullPartitionScans})
    table.insert(result, {"Hscale_invalid_commands", values.invalidQueries})
    table.insert(result, {"Hscale_invalid_results", values.invalidResults})
    table.insert(result, {"Hscale_queries_analyzed", values.queries})
    table.insert(result, {"Hscale_rewritten_ddl_commands", values.rewrittenDdl})
    table.insert(result, {"Hscale_rewritten_dml_commands", values.rewrittenDml})
    table.insert(result, {"Hscale_rewritten_misc_commands", values.rewrittenMisc})
    table.insert(result, {"Hscale_rewritten_show_commands", values.rewrittenShow})
    return result
end

--- Create a result set representation with detailed full partition scan values.
-- @return a table which can be used as the "rows part" of a proxy result set.
function getFullPartitionScansAsResultTable()
    local result = {}
    for tab, count in pairs(values.fullPartitionScans) do
        table.insert(result, {tab, count})
    end
    return result
end

--- Create a string representation of the status.
-- @return all statistics formatted as string
function getStatusAsString()
    local tab = getStatusAsResultTable()
    local result = ""
    for _, v in ipairs(tab) do
        result = result .. v[1] .. " = " .. v[2] .. "\n"
    end
    return result
end
