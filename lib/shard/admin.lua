module("shard.admin", package.seeall)

local stats = require("shard.stats")
local utils = require("shard.utils")

-- Configuration given using the init function.
local _config = nil

-- Internal function.
local function _execShowVariables()
    assert(_config, "No configuration given.")
    local rows = {}
    for k, v in pairs(_config.getAll()) do
        local value = v
        if (value ~= nil) then
            if (type(value) == "table") then
                value = utils.convertTableToString(v, 2, "=", ", ")
            end
            if (type(value) == "function") then
                value = nil
            end
        end
        table.insert(rows, {tostring(k), tostring(value)})
    end
    return
    {
        fields = {
            {type = proxy.MYSQL_TYPE_STRING, name = "variable_name"},
            {type = proxy.MYSQL_TYPE_STRING, name = "value"}
        },
        rows = rows
    }
end

-- Internal function.
local function _execShowStatus()
    return
    {
        fields = {
            {type = proxy.MYSQL_TYPE_STRING, name = "variable_name"},
            {type = proxy.MYSQL_TYPE_STRING, name = "value"}
        },
        rows = stats.getStatusAsResultTable()
    }
end

-- Internal function.
local function _execShowFullPartitionScans()
    return
    {
        fields = {
            {type = proxy.MYSQL_TYPE_STRING, name = "table"},
            {type = proxy.MYSQL_TYPE_STRING, name = "scans"}
        },
        rows = stats.getFullPartitionScansAsResultTable()
    }
end

--- Parse the given tokens for administrative sql statements.
-- @param an sql statement as tokens
-- @return a table representing a proxy resultset or nil if the given tokens did not denote an admin query.
function execAdmin(tokens)
    local isAdmin = false
    local result = nil
    local isShow = false

    local tokenName = nil
    for i = 1, #tokens do
        local token = tokens[i]
	    tokenName = token.token_name
	    tokenText = token.text:upper()
		 utils.debug("Token: " .. tokenName .. " = '" .. token.text .. "'")
		if (tokenName ~= "TK_COMMENT" and tokenName ~= "TK_UNKNOWN") then
		    if (tokenName == "TK_LITERAL" and tokenText == "SHARDING") then
		         utils.debug("Admin query recognized.", 1)
		        isAdmin = true
		    elseif (isAdmin) then
                if (tokenName == "TK_SQL_SHOW") then
    		         utils.debug("SHOW query recognized.", 1)
                    isShow = true
                elseif (isShow and tokenName == "TK_LITERAL") then
                    if (tokenText == "VARIABLES") then
        		         utils.debug("Executing SHOW VARIABLES", 1)
                        result = _execShowVariables()
                    elseif (tokenText == "STATUS") then
        		         utils.debug("Executing SHOW STATUS", 1)
                        result = _execShowStatus()
                    elseif (tokenText == "FULL_PARTITION_SCANS") then
        		         utils.debug("Executing SHOW FULL_PARTITION_SCANS", 1)
                        result = _execShowFullPartitionScans()
                    end
                end
		    end
		end
	end
	if (result ~= nil) then
	    stats.inc("adminQueries")
	end
	return result
end

function init(config)
    _config = config
end
