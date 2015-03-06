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

--- Admin module recognizing and executing administrative commands. <br />
-- 
-- Supported commands so far:
-- <ul>
--     <li>HSCALE SHOW STATUS</li>
--     <li>HSCALE SHOW VARIABLES</li>
-- </ul>
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-11 23:30:14 +0200 (Fr, 11 Apr 2008) $ $Rev: 30 $

module("optivo.hscale.admin", package.seeall)

local stats = require("optivo.hscale.stats")
local utils = require("optivo.common.utils")

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
		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. token.text .. "'")
		if (tokenName ~= "TK_COMMENT" and tokenName ~= "TK_UNKNOWN") then
		    if (tokenName == "TK_LITERAL" and tokenText == "HSCALE") then
		        -- NO_DEBUG utils.debug("Admin query recognized.", 1)
		        isAdmin = true
		    elseif (isAdmin) then
                if (tokenName == "TK_SQL_SHOW") then
    		        -- NO_DEBUG utils.debug("SHOW query recognized.", 1)
                    isShow = true
                elseif (isShow and tokenName == "TK_LITERAL") then
                    if (tokenText == "VARIABLES") then
        		        -- NO_DEBUG utils.debug("Executing SHOW VARIABLES", 1)
                        result = _execShowVariables()
                    elseif (tokenText == "STATUS") then
        		        -- NO_DEBUG utils.debug("Executing SHOW STATUS", 1)
                        result = _execShowStatus()
                    elseif (tokenText == "FULL_PARTITION_SCANS") then
        		        -- NO_DEBUG utils.debug("Executing SHOW FULL_PARTITION_SCANS", 1)
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

--- Initialize the module and give the current config object.
-- @see optivo.hscale.config
function init(config)
    _config = config
end
