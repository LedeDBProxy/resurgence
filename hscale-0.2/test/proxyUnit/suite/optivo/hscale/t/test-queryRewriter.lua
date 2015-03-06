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

--- Test the rewrite module.<br />
--
-- This module will simply return a result set containing the query that has been rewritten.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-11 23:30:14 +0200 (Fr, 11 Apr 2008) $ $Rev: 30 $

local tokenizer = require("proxy.tokenizer")
require("optivo.hscale.queryRewriter")

-- Replacement configuration
local TABLE_MAPPING = {
    ["a"] = "newA",
    ["b"] = "newB"
}

function print_debug(msg)
	if DEBUG > 0 then
		print(msg)
	end
end

function read_query( packet )
	if packet:byte() ~= proxy.COM_QUERY then
	else
        local query = packet:sub(2)
        local success, tokens = pcall(tokenizer.tokenize, query)
        local result
        if (not success) then
            result = ">>> Error tokenizing query '" .. query .. "': " .. tokens
        else
            local rewriter = optivo.hscale.queryRewriter.QueryRewriter.create(tokens, TABLE_MAPPING)
            rewriter:setStripLimitClause(true)
            success, result = pcall(rewriter.rewriteQuery, rewriter)
            if (not success) then
                result = "ERROR " .. result
            end

            proxy.response.type = proxy.MYSQLD_PACKET_OK
            proxy.response.resultset = {
                fields = {
                    {type = proxy.MYSQL_TYPE_STRING, name = result},
                },
                rows = {}
            }
        end
        return proxy.PROXY_SEND_RESULT
    end
end

function disconnect_client()
end
