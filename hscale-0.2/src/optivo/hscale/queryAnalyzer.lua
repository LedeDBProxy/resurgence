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

---	Analyze a query to find the partition values for all partitioned tables. <br />
--
-- Invalid queries unless a hint is provided:
-- <ul>
--     <li>Only one alias or the table name itself can be used per table, i.e.
--         <code>SELECT * FROM table AS a LEFT JOIN table AS b ...</code> is not possible.</i>
--     <li>Update a partition key like in <code>UPDATE a SET a.partiton = 3</code></li>
--     <li><code>INSERT INTO a SELECT ...</code></li>
--     <li><code>INSERT INTO a VALUES(...)</code> - such queries are bad anyway and should not be used</li>
--     <li>Statements including database names are not handled very well</li>
-- </ul>
-- Hinting: <br />
-- Add comments to your query to force certain partition behaviours. USE WITH CAUTION!<br />
-- <ol>
--     <li>
--        Override the partition key for a table:<br />
--        <code>/* hscale.partitionKey(table) = 'key' */</code>
--     </li>
--     <li>
--        force full partition scan over all(!) tables - this will only work if the partition lookup:
--        <code>/* hscale.forceFullPartitionScan() */</code>
--     </li>
-- </ol>
-- Note that multiple hints must placed in separate comment blocks.
-- Whitespaces are ignored but the single quotes are mandatory.
-- Hints override any other partition key already found.
-- <br />
-- Query types:
-- <ol>
--     </li>DDL: Data Definition Language - <a href="http://dev.mysql.com/doc/refman/5.0/en/data-definition.html">CREATE, DROP, ALTER etc</a></li>
--     </li>DML: Data Manipulation Language - <a href="http://dev.mysql.com/doc/refman/5.0/en/data-manipulation.html">SELECT, UPDATE, DELETE etc</a></li>
--     </li>Misc: DESCRIBE, SHOW COLUMNS, SHOW CREATE TABLE, SHOW INDEX FROM, SHOW KEYS FROM</li>
--     </li>Show: <a href="http://dev.mysql.com/doc/refman/5.0/en/show.html">SHOW TABLE STATUS, SHOW TABLES, SHOW OPEN TABLES</a></li>
-- </ol>
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-21 01:16:39 +0200 (Mo, 21 Apr 2008) $ $Rev: 35 $

module("optivo.hscale.queryAnalyzer", package.seeall)

local utils = require("optivo.common.utils")

QueryAnalyzer = {}
QueryAnalyzer.__index = QueryAnalyzer

--- Constructor.
-- @param tokens the query to analyze as a sequence of tokens
-- @param tableKeyColumns all keys and values MUST be lower case
function QueryAnalyzer.create(tokens, tableKeyColumns)
    local self = {}
    setmetatable(self, QueryAnalyzer)

    -- member variables
    self._tokens = tokens
    self._tableKeyColumns = tableKeyColumns
    self._tableKeyValues = {}
    self._tableHints = {}

    self._statementType = nil
    -- Is the statement SELECT, UPDATE, INSERT, REPLACE or DELETE?
    self._isDml = false
    -- Is the statement CREATE, ALTER, DROP?
    self._isDdl = false
    -- Is the statement DESCRIBE
    self._isMisc = false
    -- Is the statement SHOW ...
    self._isShow = false
    -- Are we behind a JOIN or WHERE condidtion
    self._isPastJoinOrWhere = false
    self._inTableList = false
    -- table name -> table alias
    self._tableToAlias = {}
    -- The same map as above the other way around
    self._aliasToTable = {}
    self._tablesFound = 0 
    self._hasWhere = false
    self._hasOrderBy = false
    self._hasGroupBy = false
    self._hasIntoOutFile = false
    self._hasFunction = false
    self._limitFrom = -1
    self._limitRows = -1
    self._executedBefore = false
    self._isForceFullPartitionScan = false

    return self
end

--- 
-- @return true if the query analyzed should be handled (i.e. a partition table has been found)
function QueryAnalyzer:isPartitioningNeeded()
    return #self:getAffectedTables() > 0
end

---
-- @return the statement type (one of INSERT, REPLACE, LOAD DATA INFILE, SELECT, UPDATE, DELETE, TRUNCATE, ALTER TABLE, CREATE TABLE, DROP TABLE)
function QueryAnalyzer:getStatementType()
    return self._statementType
end

---
-- @return true if query was DML (SELECT, INSERT, REPLACE, LOAD DATA INFILE, UPDATE, DELETE, TRUNCATE)
function QueryAnalyzer:isDml()
    return self._isDml
end

---
-- @return true if query was DDL (CREATE TABLE, DROP TABLE, ALTER TABLE)
function QueryAnalyzer:isDdl()
    return self._isDdl
end

---
-- @return true if query was a misc query like DESCRIBE tbl_name or SHOW CREATE TABLE tbl_name
function QueryAnalyzer:isMisc()
    return self._isMisc
end

---
-- @return true if query was a SHOW query like SHOW TABLES
function QueryAnalyzer:isShow()
    return self._isShow
end

---
-- @return the table / parition value combination (call only if isDml() is true)
function QueryAnalyzer:getTableKeyValues()
    return self._tableKeyValues
end

---
-- @return an array of all partioned tables affected by the query
function QueryAnalyzer:getAffectedTables()
    local result = {}
    for tableName, _ in pairs(self._tableToAlias) do
        if (self._tableKeyColumns[tableName]) then
            table.insert(result, tableName)
        end
    end
    return result
end

---
-- @return true if the query analyzed contained a WHERE clause.
function QueryAnalyzer:hasWhere()
    return self._hasWhere
end

---
-- @return true if the query analyzed contained an ORDER BY clause.
function QueryAnalyzer:hasOrderBy()
    return self._hasOrderBy
end

---
-- @return true if the query analyzed contained any function call (like COUNT(*) etc).
function QueryAnalyzer:hasFunction()
    return self._hasFunction
end

---
-- @return true if the query analyzed contained INTO OUTFILE ...
function QueryAnalyzer:hasIntoOutFile()
    return self._hasIntoOutFile
end

---
-- @return true if the query analyzed contained a GROUP BY clause.
function QueryAnalyzer:hasGroupBy()
    return self._hasGroupBy
end

---
-- @return true if the query analyzed contained a LIMIT clause.
function QueryAnalyzer:hasLimit()
    return self._limitRows >= 0
end

---
-- @return from, numberOfRows of the LIMIT clause (-1 if not found)
function QueryAnalyzer:getLimit()
    return self._limitFrom, self._limitRows
end

function QueryAnalyzer:isForceFullPartitionScan()
    return self._isForceFullPartitionScan
end

--- Full partition scan is only possible and needed if there is only one table in query
-- no ORDER BY and no GROUP BY and no "INTO OUTFILE" and no function and it is a SELECT or an UPDATE or DELETE with no LIMIT clause.
-- @return true if the query needs to be run against all partitions.
function QueryAnalyzer:isFullPartitionScanNeeded()
    return
        not self._hasOrderBy
        and
        not self._hasGroupBy
        and
        not self._hasIntoOutFile
        and
        not self._hasFunction
        and (
            self._isForceFullPartitionScan
            or (
                self._tablesFound == 1
                and self._tableKeyValues[self:getAffectedTables()[1]] == nil
                and (
                    self._statementType == "SELECT"
                    or (
                        not self:hasLimit()
                        and (
                            self._statementType == "TRUNCATE"
                            or
                            self._statementType == "UPDATE"
                            or
                            self._statementType == "DELETE"
                        )
                    )
                )
            )
        )
end

--- Analyze the query and populate the result which can be obtained using the other methods.
-- Note: This methods throws exceptions (using assert(...)).
function QueryAnalyzer:analyze()

    -- Prevent calling this method twice
    assert(not self._executedBefore, "You cannot call this method twice on the same instance.")
    self._executedBefore = true

	-- Variables used within the loop
	local lastTokenName
	local lastTokenText
	local lastTokenTextLc
	local tokenName
	local tokenText
	local tokenTextLc
    local isPastFromOrInto = false

    -- NO_DEBUG utils.debug("BEGIN analyze ----------------------------------------------------------------------------")
    self:_normalizeQueryAndParseHints()
	for pos, token in ipairs(self._tokens) do
		tokenName = token.token_name
		tokenText = token.text
		tokenTextLc = tokenText:lower()
		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. tokenText .. "'")

        -- Find out comments and unknown tokens
		if (tokenName == "TK_SQL_JOIN" or tokenName == "TK_SQL_WHERE") then
		    self._isPastJoinOrWhere = true
		end
		if (tokenName == "TK_SQL_WHERE") then
            self._hasWhere = true
		elseif (tokenName == "TK_SQL_ORDER") then
            self._hasOrderBy = true
		elseif (tokenName == "TK_FUNCTION") then
            self._hasFunction = true
		elseif (tokenName == "TK_SQL_GROUP") then
            self._hasGroupBy = true
		elseif (tokenName == "TK_SQL_OUTFILE") then
            self._hasIntoOutFile = true
		elseif (tokenName == "TK_SQL_LIMIT") then
		    self:_parseLimit(pos)
		end
		if (not self._statementType and tokenName == "TK_SQL_SHOW") then
		    self:_parseShow()
		    break
		elseif ((lastTokenName == "TK_SQL_DESC" or lastTokenName == "TK_SQL_DESCRIBE") and tokenName == "TK_LITERAL") then
		    self._statementType = "DESCRIBE"
		    self._isMisc = true
		    self:_setTableFound(tokenText, tokenText)
		    break
		elseif (
		    tokenName == "TK_SQL_CREATE"
		    or tokenName == "TK_SQL_DROP"
		    or tokenName == "TK_SQL_ALTER"
		    or tokenName == "TK_SQL_RENAME") then
		    -- This is a DDL statement
		    self:_parseDdl()
		    break
		elseif (
		    tokenName == "TK_SQL_SELECT"
		    or tokenName == "TK_SQL_INSERT"
		    or tokenName == "TK_SQL_REPLACE"
            or (tokenName == "TK_LITERAL" and tokenTextLc == "truncate")
		    or tokenName == "TK_SQL_UPDATE"
		    or tokenName == "TK_SQL_DELETE") then
		    -- Now we know the type of the statement
		    self._statementType = tokenText:upper()
		    self._isDml = true
		    if (self._statementType == "INSERT" or self._statementType == "REPLACE") then
                -- Insert statement of the form INSERT INTO table (...) VALUES (...) differs
                -- too much so we handle it separately once we have found the table name
                if (self:_parseInsert()) then
                    -- The statement has been successfully parsed. Otherwise we simply follow
                    -- the main loop since INSERT INTO table SET ... follows the same rules
                    break
                end
            end
        elseif (tokenName == "TK_SQL_INFILE") then
            -- This is LOAD DATA INFILE
            self._statementType = "LOAD DATA INFILE"
            self._isDml = true
        elseif (tokenName == "TK_SQL_FROM" or tokenName == "TK_SQL_INTO") then
            isPastFromOrInto = true
		elseif (
		    self._isDml
		    and (isPastFromOrInto or self._statementType == "UPDATE" or self._statementType == "TRUNCATE")
		    and not self._hasOrderBy
		) then
		    -- We are within a CRUD statement
			if (tokenName == "TK_LITERAL") then
				if (
				    lastTokenName == "TK_SQL_FROM"
				    or lastTokenName == "TK_SQL_JOIN"
				    or lastTokenName == "TK_SQL_INTO"
				    or lastTokenName == "TK_SQL_TABLE"
				    or lastTokenName == "TK_SQL_UPDATE"
				    or (lastTokenName == "TK_LITERAL" and lastTokenTextLc == "truncate")
				    or self._inTableList) then

                    -- Found a table name
        			if (self._tableKeyColumns[tokenTextLc]) then
        			    -- One of our tables is used - verify that there is only one alias of a table
        			    self:_setTableFound(tokenTextLc, tokenTextLc)

        			    -- Test for short alias notation "SELECT * FROM table alias WHERE ..."
        			    local nextToken = self._tokens[pos + 1]
        			    if (nextToken and nextToken.token_name == "TK_LITERAL") then
        			        self:_setTableFound(tokenTextLc, nextToken.text:lower())
        			    end

        			end
        			self._tablesFound = self._tablesFound + 1

        			-- Multiple tables may follow separated by TK_COMMA
        			self._inTableList = true
        		elseif (lastTokenName ~= "TK_DOT") then
        		    -- NO_DEBUG utils.debug("Possible column w/o prefix: " .. tokenText, 1)
        		    -- We must have a column here that is not prefixed by a table alias
        		    for tableName, column in pairs(self._tableKeyColumns) do
                        -- NO_DEBUG utils.debug("Testing table/column " .. tableName .. "/" .. column, 2)
        		        if (tokenTextLc == column and self._tableToAlias[tableName]) then
        		            -- We have found a partition key if the next token is TK_EQ and the following is TK_LITERAL
                            -- NO_DEBUG utils.debug("#self._tokens = " .. pos .. "/" .. #self._tokens, 3)
                            assert(#self._tokens >= pos + 2, "Invalid query near '" .. tokenText .. "' - too few tokens.")
                            local valueToken = self._tokens[pos + 2]
                            -- NO_DEBUG utils.debug("Next two tokens: " .. self._tokens[pos + 1].token_name .. " - " .. valueToken.token_name, 3)
                            if (self._tokens[pos + 1].token_name == "TK_EQ") then
                                -- NO_DEBUG utils.debug("Found partition key '" .. valueToken.text .. "' for table '" .. tableName .. "'", 4)
                                self:_setPartitionTableKey(tableName, valueToken, self._tokens[pos + 3])
                            end
        		        end
        		    end
        	    end
            elseif (tokenName == "TK_SQL_AS") then
                -- Handle table alias by doin a forward lookup
                assert(#self._tokens >= pos + 2 and #self._tokens > 1, "Invalid query - too few tokens.")
                if (self._tableKeyColumns[lastTokenTextLc]) then
                    -- This is a table we know
                    local tableAlias = self._tokens[pos + 1].text
                    self:_setTableFound(lastTokenTextLc, tableAlias)
                    -- NO_DEBUG utils.debug("Table alias found for table '" .. lastTokenText .. "' = '" .. tableAlias .. "'", 1)
                end
            elseif (tokenName == "TK_DOT" and lastTokenName == "TK_LITERAL") then
                -- A dot might mean that we are accessing an aliased column
                -- NO_DEBUG utils.debug("Found a dot - testing for alias.keyColumn = keyValue", 1)
                if (#self._tokens >= pos + 3 and #self._tokens > 1) then
                    local possibleTable = self._aliasToTable[lastTokenTextLc]
                    local valueToken = self._tokens[pos + 3]
                    local columnToken = self._tokens[pos + 1]
                    -- NO_DEBUG utils.debug("Examining possible table alias/name '" .. lastTokenText .. "/" .. (possibleTable or "") .. "'", 1)
                    -- NO_DEBUG utils.debug("Possible column '" .. columnToken.text .. "'", 1)
                    if (
                        lastTokenName == "TK_LITERAL"
                        and possibleTable
                        and self._tableKeyColumns[possibleTable] == columnToken.text:lower()
                        and self._tokens[pos + 2].token_name == "TK_EQ"
                    ) then
                        -- The last token was a valid alias / table name and the next one is an equals sign - we have a key
                        -- NO_DEBUG utils.debug("Found partition key '" .. valueToken.text .. "' for table '" .. possibleTable .. "' (by alias)", 1)
                        self:_setPartitionTableKey(possibleTable, valueToken, self._tokens[pos + 3])
                    end
                end
			elseif (tokenName ~= "TK_COMMA") then
			    -- We are definitely out of a table list
			    self._inTableList = false
			end
		end
            lastTokenName = tokenName
            lastTokenText = tokenText
            lastTokenTextLc = tokenTextLc
        end

    -- NO_DEBUG utils.debug("END analyze ----------------------------------------------------------------------------")

    -- Hints take precedence
    for tableName, partitionKey in pairs(self._tableHints) do
        self._tableKeyValues[tableName] = partitionKey
    end

    self:_verifyResult()
end

-- Verify the analyzation result. Throws an error (via assert) if something is wrong.
function QueryAnalyzer:_verifyResult()
	-- Ensure that for every (partitionable) table that has been found a partition key has been found, too
	if (self._isDml and not self:isFullPartitionScanNeeded()) then
        for tableName in pairs(self._tableToAlias) do
            assert(self._tableKeyValues[tableName], "No partition key found for table '" .. tableName .. "'.")
        end
    end

    -- Reject unsupported DDL statements
    if (self._isDdl and #self:getAffectedTables() > 0) then
        -- Reject RENAME TABLE
        if (self._statementType == "RENAME TABLE") then
            for tableName in pairs(self._tableToAlias) do
                assert(not self._tableKeyColumns[tableName], "RENAME of partitioned tables is not allowed.")
            end
        end
    end
end

-- Parse a limit clause.
function QueryAnalyzer:_parseLimit(pos)
    local limitToken = self._tokens[pos + 1]
    local middleToken = self._tokens[pos + 2]
    local limit2Token = self._tokens[pos + 3]
    assert(limitToken, "Invalid query. Not enough tokens to parse LIMIT clause.")
    assert(limitToken.token_name == "TK_INTEGER", "Invalid query. LIMIT must be followed by an integer.")
    self._limitRows = tonumber(limitToken.text)
    if (middleToken and middleToken.token_name == "TK_COMMA" and limit2Token.token_name == "TK_INTEGER") then
        self._limitFrom = self._limitRows
        self._limitRows = tonumber(limit2Token.text)
    end
    if (middleToken and middleToken.token_name == "TK_OFFSET" and limit2Token.token_name == "TK_INTEGER") then
        self._limitFrom = tonumber(limit2Token.text)
    end
end

-- Parse a DDL statement like [CREATE | ALTER | RENAME | DROP] [TABLE | INDEX] - all others are ignored.
function QueryAnalyzer:_parseDdl()
	local tokenName
	local tokenText
	local tokenTextLc
	local lastTokenName
	local lastTokenText
	local lastTokenTextLc

    local tableName

    -- NO_DEBUG utils.debug("Parsing ddl")
	for pos, token in ipairs(self._tokens) do
		tokenName = token.token_name
		tokenText = token.text
		tokenTextLc = tokenText:lower()
		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. tokenText .. "'", 1)
        if (
            (lastTokenName == "TK_SQL_TABLE")
            or
            (lastTokenName == "TK_SQL_ON" and string.match(self._statementType, "INDEX"))
        ) then
            -- We are done
            tableName = tokenTextLc
            self:_setTableFound(tableName, tableName)
            self._isDdl = true
            if (self._statementType == "RENAME TABLE") then
                -- Handle RENAME t1 TO t2 [, t3 TO t4 ...]
                repeat
                    local token = self._tokens[pos]
                    local nextToken = self._tokens[pos + 1]
                    local nextTableToken = self._tokens[pos + 2]
                    local commaToken = self._tokens[pos + 3]
                    if (nextToken and nextToken.token_name == "TK_SQL_TO" and nextTableToken and nextTableToken.token_name == "TK_LITERAL") then
                        self:_setTableFound(token.text, token.text)
                        self:_setTableFound(nextTableToken.text, nextTableToken.text)
                    end
                    pos = pos + 4
                until not (commaToken and commaToken.token_name == "TK_COMMA")
            end
		    -- NO_DEBUG utils.debug("Found table " .. tableName, 2)
            break
        elseif (
            (tokenName == "TK_SQL_TABLE" or tokenName == "TK_SQL_INDEX")
            and (
                lastTokenName == "TK_SQL_CREATE"
                or lastTokenName == "TK_SQL_DROP"
                or lastTokenName == "TK_SQL_ALTER"
                or lastTokenName == "TK_SQL_RENAME"
            )
        ) then
            self._statementType =  string.sub(lastTokenName, 8) .. " " .. tokenText:upper()
		    -- NO_DEBUG utils.debug("Statement type = '" .. self._statementType .. "'", 2)
        end
        lastTokenName = tokenName
        lastTokenText = tokenText
        lastTokenTextLc = tokenTextLc
    end
end

-- Parse a SHOW ... statements
function QueryAnalyzer:_parseShow()
	local tokenName
	local tokenText
	local tokenTextLc
	local lastTokenName
	local lastTokenText
	local lastTokenTextLc

    local tableName

    -- NO_DEBUG utils.debug("Parsing show")
	for pos, token in ipairs(self._tokens) do
		tokenName = token.token_name
		tokenText = token.text
		tokenTextLc = tokenText:lower()
		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. tokenText .. "'", 1)
		if (
		    (lastTokenName == "TK_SQL_CREATE" and tokenName == "TK_SQL_TABLE") -- SHOW CREATE TABLE
		    or (lastTokenName == "TK_LITERAL" and lastTokenTextLc == "columns" and tokenName == "TK_SQL_FROM") -- SHOW COLUMNS FROM
		    or (lastTokenName == "TK_SQL_INDEX" and tokenName == "TK_SQL_FROM") -- SHOW INDEX FROM
		    or (lastTokenName == "TK_SQL_KEYS" and tokenName == "TK_SQL_FROM") -- SHOW KEYS FROM
		) then
		    local nextToken = self._tokens[pos + 1]
		    if (nextToken and nextToken.token_name == "TK_LITERAL") then
                self:_setTableFound(nextToken.text, nextToken.text)
                self._statementType =  "SHOW " .. lastTokenText:upper()
                if (tokenName ~= "TK_SQL_FROM") then
                    self._statementType = self._statementType .. " " .. tokenText:upper()
                end
                if (self._statementType == "SHOW KEYS") then
                    self._statementType = "SHOW INDEX"
                end
                self._isMisc = true
                break
            end
        elseif (lastTokenName == "TK_SQL_TABLE" and tokenName == "TK_LITERAL" and tokenTextLc == "status") then
            self._isShow = true
            self._statementType = "SHOW TABLE STATUS"
            break
        elseif (tokenName == "TK_LITERAL" and tokenTextLc == "open" and tokenName == "TK_LITERAL" and tokenTextLc == "tables") then
            self._isShow = true
            self._statementType = "SHOW OPEN TABLES"
            break
        elseif (tokenName == "TK_LITERAL" and tokenTextLc == "tables") then
            self._isShow = true
            self._statementType = "SHOW TABLES"
            break
        end
        lastTokenName = tokenName
        lastTokenText = tokenText
        lastTokenTextLc = tokenTextLc
    end
end

-- Parse statements of the form INSERT INTO table (...) VALUES (...)
-- @return true if the statement has been parsed correctly
function QueryAnalyzer:_parseInsert()
    local isPastInto
	local tokenName
	local tokenText
	local tokenTextLc
	local lastTokenName
	local lastTokenText
	local lastTokenTextLc

    local tableName
	local isInColumns = false
	local currentColumn = 0
	local partitionColumnPos = -1
	local isInValues = false

	local success = false

	for pos, token in ipairs(self._tokens) do
		tokenName = token.token_name
		tokenText = token.text
		tokenTextLc = tokenText:lower()
		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. tokenText .. "'")
        if (lastTokenName == "TK_SQL_INTO") then
            tableName = tokenTextLc
        elseif (tokenName == "TK_OBRACE") then
            if (not isInColumns) then
                isInColumns = true
            else
                isInValues = true
            end
            currentColumn = 0
        elseif (tokenName == "TK_COMMA") then
            currentColumn = currentColumn + 1
        elseif (isInColumns and tokenName == "TK_LITERAL") then
            -- Is this the partition column name?
            if (tokenTextLc == self._tableKeyColumns[tableName]) then
                partitionColumnPos = currentColumn
            end
        elseif (isInValues and partitionColumnPos == currentColumn and tableName) then
            -- We are at the value position
            -- NO_DEBUG utils.debug("Testing value '" .. tokenText .. "'", 1)
            assert(tableName, "No table specified")
            self:_setPartitionTableKey(tableName, token, self._tokens[pos + 1])
            self:_setTableFound(tableName, tableName)
            -- We are done
            success = true
            break
        end
            lastTokenName = tokenName
            lastTokenText = tokenText
            lastTokenTextLc = tokenTextLc
        end
    return success
end

-- Set the partition value (valueToken.text) for the given tableName and assert that the value expression
-- is valid.
function QueryAnalyzer:_setPartitionTableKey(tableName, valueToken, nextToken)
    local valueTokenName = valueToken.token_name

    -- Assure that we are not trying to update the partition key
    assert(
        not (self._statementType == "UPDATE" and not self._isPastJoinOrWhere),
        "You cannot update the partition key for table '" .. tableName .. "'."
    )

    -- Assert the value token type
    if (
        valueTokenName == "TK_STRING"
        or valueTokenName == "TK_INTEGER"
        or valueTokenName == "TK_FLOAT"
        or valueTokenName == "TK_DATE"
    ) then
        -- Assert that the value is not part of a multi term expression like "1 + 3"
        assert(not self._tableKeyValues[tableName], "Multiple partition values specified for table '" .. tableName .. "'")
        if (nextToken) then
            local nextTokenName = nextToken.token_name
            assert(
                nextTokenName ~= "TK_STAR"
                and nextTokenName ~= "TK_PLUS"
                and nextTokenName ~= "TK_PLUS"
                and nextTokenName ~= "TK_MINUS"
                and nextTokenName ~= "TK_DIV",
                "Partition value for table '" .. tableName .. "' is an expression. Only scalar values are supported"
            )
        end
        self._tableKeyValues[tableName] = valueToken.text
    end
end

-- Set the table and alias mapping
function QueryAnalyzer:_setTableFound(tableName, tableAlias)
    local currentAlias = self._tableToAlias[tableName]
    assert(not currentAlias or currentAlias == tableName, "More than one alias for table '" .. tableName .. "'.")

    self._tableToAlias[tableName] = tableAlias
    self._aliasToTable[tableAlias] = tableName
end

-- Parse the tokens for hints in the comments.
-- Additionally all comment, unknown and other tokens not needed are filtered out of self._tokens.
function QueryAnalyzer:_normalizeQueryAndParseHints()
    local result = {}
    local tokenName = nil
    -- NO_DEBUG utils.debug("Parsing tokens for comments and hints...")
	for pos, token in ipairs(self._tokens) do
	    tokenName = token.token_name
		-- NO_DEBUG utils.debug("Token: " .. tokenName .. " = '" .. token.text .. "'", 1)
		if (tokenName == "TK_COMMENT") then
            local _, _, tableName, partitionKey = string.find(token.text, "hscale.partitionKey%s*%(%s*(.-)%s*%)%s*=%s'(.*)'")
            if (tableName and partitionKey) then
                -- NO_DEBUG utils.debug("Found hint for table '" .. tableName .. "': '" .. partitionKey .. "'", 2)
                self._tableHints[tableName:lower()] = partitionKey
            end
            if (string.match(token.text, "hscale.forceFullPartitionScan%(%)")) then
                self._isForceFullPartitionScan = true
            end
		elseif (
		    tokenName == "TK_UNKNOWN"
		    or tokenName == "TK_SQL_LOW_PRIORITY"
            or tokenName == "TK_SQL_IGNORE"
            or tokenName == "TK_SQL_UNIQUE"
            or tokenName == "TK_SQL_SPATIAL"
            or tokenName == "TK_SQL_FULLTEXT"
            or (tokenName == "TK_LITERAL" and token.text:upper() == "TEMPORARY")
            or (tokenName == "TK_LITERAL" and token.text:upper() == "FULL")
            or tokenName == "TK_SQL_IF"
            or tokenName == "TK_SQL_EXISTS"
        ) then
            -- Ignore these token (and filter them out)		    
        else
		    table.insert(result, token)
		end
	end
	self._tokens = result
end