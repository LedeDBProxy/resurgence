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

---	Rewrite a query and replace all occurrences of given table names.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-21 01:16:39 +0200 (Mo, 21 Apr 2008) $ $Rev: 35 $

module("optivo.hscale.queryRewriter", package.seeall)

local utils = require("optivo.common.utils")

QueryRewriter = {}
QueryRewriter.__index = QueryRewriter

--- Constructor
-- @param tableMapping all keys MUST be lower case
function QueryRewriter.create(tokens, tableMapping)
    local self = {}
    setmetatable(self, QueryRewriter)

    -- member variables
    self._tokens = tokens
    self._tableMapping = tableMapping
    self._stripLimitClause = false

    return self
end

--- If set to true then any LIMIT x [, y] will be stripped off the final result.
function QueryRewriter:setStripLimitClause(bool)
    self._stripLimitClause = bool
end

--- Rewrite the given tokens and replace the table names according to the given table mapping
-- @return the reqritten query as string.
function QueryRewriter:rewriteQuery()

	-- Variables used within the loop
	local lastTokenName
	local lastTokenText
	local lastTokenTextLc
	local tokenName
	local tokenText
	local tokenTextLc
	local newTokenText
	local isValidStatementType = false
	local inTableList = false
	local tablesFound = {}
	local ignoreToken
	local skipTokens = 0

	-- This table holds the resulting query
	local newQuery = {}

	for pos, token in ipairs(self._tokens) do
		tokenName = token.token_name
		tokenText = token.text
		tokenTextLc = tokenText:lower()
		newTokenText = tokenText

		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. tokenText .. "'")

        -- Skip tokens that are not needed to rewrite the query.
		ignoreToken = (
		    tokenName == "TK_COMMENT"
		    or tokenName == "TK_UNKNOWN"
		    or tokenName == "TK_SQL_IF"
		    or tokenName == "TK_SQL_NOT"
		    or tokenName == "TK_SQL_EXISTS"
		    or tokenName == "TK_SQL_SPATIAL"
		    or tokenName == "TK_SQL_UNIQUE"
		    or tokenName == "TK_SQL_FULLTEXT"
		)
        if (tokenName == "TK_SQL_LIMIT" and self._stripLimitClause) then
            skipTokens = 2
            local limitToken = self._tokens[pos + 1]
            local middleToken = self._tokens[pos + 2]
            local limit2Token = self._tokens[pos + 3]
            if (
                middleToken
                and (
                    middleToken.token_name == "TK_COMMA" or middleToken.token_name == "OFFSET"
                ) and limit2Token.token_name == "TK_INTEGER"
            ) then
                skipTokens = 4
            end
		elseif (
		    tokenName == "TK_SQL_SELECT"
		    or tokenName == "TK_SQL_INSERT"
		    or tokenName == "TK_SQL_REPLACE"
		    or tokenName == "TK_SQL_UPDATE"
		    or tokenName == "TK_SQL_DELETE"
		    or tokenName == "TK_SQL_DESCRIBE"
		    or tokenName == "TK_SQL_DESC"
            or (tokenName == "TK_LITERAL" and tokenTextLc == "truncate") -- TRUNCATE
            or (lastTokenName == "TK_LITERAL" and lastTokenTextLc == "columns" and tokenName == "TK_SQL_FROM") -- SHOW COLUMNS FROM tbl_name
            or (lastTokenName == "TK_SQL_INDEX" and tokenName == "TK_SQL_FROM") -- SHOW INDEX FROM tbl_name
            or (lastTokenName == "TK_SQL_KEYS" and tokenName == "TK_SQL_FROM") -- SHOW KEYS FROM tbl_name
		    or tokenName == "TK_SQL_INFILE"
		) then
		    isValidStatementType = true
		    -- NO_DEBUG utils.debug("Statement type is " .. tokenName, 1)
		elseif (
            (tokenName == "TK_SQL_TABLE" or tokenName == "TK_SQL_INDEX")
            and (
                lastTokenName == "TK_SQL_CREATE"
                or lastTokenName == "TK_SQL_DROP"
                or lastTokenName == "TK_SQL_ALTER"
            )
        ) then
		    isValidStatementType = true
		    -- NO_DEBUG utils.debug("Statement type is " .. tokenName, 1)
		elseif (isValidStatementType) then
		    -- We are within a CRUD statement
			if (tokenName == "TK_LITERAL") then
				if (
				    lastTokenName == "TK_SQL_FROM"
				    or lastTokenName == "TK_SQL_JOIN"
				    or lastTokenName == "TK_SQL_INTO"
				    or lastTokenName == "TK_SQL_UPDATE"
				    or lastTokenName == "TK_SQL_DESCRIBE"
				    or lastTokenName == "TK_SQL_DESC"
				    or (lastTokenName == "TK_LITERAL" and lastTokenTextLc == "truncate")
				    or lastTokenName == "TK_SQL_TABLE"
				    or lastTokenName == "TK_SQL_ON"
				    or inTableList) then

                    -- NO_DEBUG utils.debug("Found possible table '" .. tokenText .. "'", 1)
        			local destTable = self._tableMapping[tokenTextLc]
        			if (destTable) then
                        -- NO_DEBUG utils.debug("Found table '" .. tokenText .. "' and rewriting it to '" .. destTable .. "'", 1)
        			    tablesFound[tokenTextLc] = true
        			    newTokenText = destTable
        			end

        			-- Multiple tables may follow separated by TK_COMMA
        			inTableList = true
				end
            elseif (tokenName == "TK_DOT" and tablesFound[lastTokenTextLc]) then
                -- Replace columns prefixed by the table name (like in SELECT * FROM A WHERE A.col = 10)
                newQuery[#newQuery] = self._tableMapping[lastTokenTextLc]
			    inTableList = false
			elseif (tokenName ~= "TK_COMMA") then
			    -- We are definitely out of a table list
			    inTableList = false
			end
		end
		if (skipTokens == 0) then
            if (
                #newQuery > 0
                and tokenName ~= "TK_COMMA"
                and tokenName ~= "TK_DOT"
                and tokenName ~= "TK_CBRACE"
                and tokenName ~= "TK_OBRACE"
                and lastTokenName ~= "TK_OBRACE"
                and lastTokenName ~= "TK_DOT") then

                table.insert(newQuery, " ")
            end
            if (tokenName == "TK_STRING") then
                local s = string.format('%q', newTokenText)
                s = string.gsub(s, "\\\\", "\\")
                table.insert(newQuery, s)
            elseif (tokenName == "TK_COMMENT") then
                table.insert(newQuery, "/*" .. newTokenText .. "*/")
            else
                table.insert(newQuery, newTokenText)
            end
            -- Ignore comments while evaluating
            if (not ignoreToken) then
                lastTokenName = tokenName
                lastTokenText = tokenText
                lastTokenTextLc = tokenTextLc
            end
        else
            skipTokens = skipTokens - 1
        end
	end
	return table.concat(newQuery)
end
