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

--- Unit test the analyzer.
-- Only the basics are tested here. More detailed tests can befound within the proxyUnit package.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-11 23:30:14 +0200 (Fr, 11 Apr 2008) $ $Rev: 30 $

require('luaunit')
require("optivo.hscale.queryAnalyzer")

TestQueryAnalyzer = {}

local TABLE = "table"
local COLUMN = "partition"
local PARTITION_VALUE = "partValue"

local TABLE_KEY_COLUMNS = {
    [TABLE] = COLUMN
}

local SELECT_TOKENS = {
    {["token_name"] = "TK_SQL_SELECT", ["text"] = "SELECT"},
    {["token_name"] = "TK_STAR", ["text"] = "*"},
    {["token_name"] = "TK_SQL_FROM", ["text"] = "FROM"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_WHERE", ["text"] = "WHERE"},
    {["token_name"] = "TK_LITERAL", ["text"] = COLUMN},
    {["token_name"] = "TK_EQ", ["text"] = "="},
    {["token_name"] = "TK_STRING", ["text"] = PARTITION_VALUE}
}

local SELECT_TOKENS_LIMIT_1 = {
    {["token_name"] = "TK_SQL_SELECT", ["text"] = "SELECT"},
    {["token_name"] = "TK_STAR", ["text"] = "*"},
    {["token_name"] = "TK_SQL_FROM", ["text"] = "FROM"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_LIMIT", ["text"] = "LIMIT"},
    {["token_name"] = "TK_INTEGER", ["text"] = "1"}
}

local SELECT_TOKENS_LIMIT_2 = {
    {["token_name"] = "TK_SQL_SELECT", ["text"] = "SELECT"},
    {["token_name"] = "TK_STAR", ["text"] = "*"},
    {["token_name"] = "TK_SQL_FROM", ["text"] = "FROM"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_LIMIT", ["text"] = "LIMIT"},
    {["token_name"] = "TK_INTEGER", ["text"] = "1"},
    {["token_name"] = "TK_COMMA", ["text"] = ","},
    {["token_name"] = "TK_INTEGER", ["text"] = "50"},
}

local SELECT_TOKENS_LIMIT_3 = {
    {["token_name"] = "TK_SQL_SELECT", ["text"] = "SELECT"},
    {["token_name"] = "TK_STAR", ["text"] = "*"},
    {["token_name"] = "TK_SQL_FROM", ["text"] = "FROM"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_LIMIT", ["text"] = "LIMIT"},
    {["token_name"] = "TK_INTEGER", ["text"] = "50"},
    {["token_name"] = "TK_OFFSET", ["text"] = "OFFSET"},
    {["token_name"] = "TK_INTEGER", ["text"] = "1"},
}

local UPDATE_TOKENS = {
    {["token_name"] = "TK_SQL_UPDATE", ["text"] = "UPDATE"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_SET", ["text"] = "SET"},
    {["token_name"] = "TK_LITERAL", ["text"] = "anotherTable"},
    {["token_name"] = "TK_EQ", ["text"] = "="},
    {["token_name"] = "TK_STRING", ["text"] = "some value"},
    {["token_name"] = "TK_SQL_WHERE", ["text"] = "WHERE"},
    {["token_name"] = "TK_LITERAL", ["text"] = COLUMN},
    {["token_name"] = "TK_EQ", ["text"] = "="},
    {["token_name"] = "TK_STRING", ["text"] = PARTITION_VALUE}
}

local DELETE_TOKENS = {
    {["token_name"] = "TK_SQL_DELETE", ["text"] = "DELETE"},
    {["token_name"] = "TK_SQL_FROM", ["text"] = "FROM"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_WHERE", ["text"] = "WHERE"},
    {["token_name"] = "TK_LITERAL", ["text"] = COLUMN},
    {["token_name"] = "TK_EQ", ["text"] = "="},
    {["token_name"] = "TK_STRING", ["text"] = PARTITION_VALUE}
}

local INSERT_TOKENS_1 = {
    {["token_name"] = "TK_SQL_INSERT", ["text"] = "INSERT"},
    {["token_name"] = "TK_SQL_INTO", ["text"] = "INTO"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_SET", ["text"] = "SET"},
    {["token_name"] = "TK_LITERAL", ["text"] = COLUMN},
    {["token_name"] = "TK_EQ", ["text"] = "="},
    {["token_name"] = "TK_STRING", ["text"] = PARTITION_VALUE}
}

local INSERT_TOKENS_2 = {
    {["token_name"] = "TK_SQL_INSERT", ["text"] = "INSERT"},
    {["token_name"] = "TK_SQL_INTO", ["text"] = "INTO"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_OBRACE", ["text"] = "("},
    {["token_name"] = "TK_LITERAL", ["text"] = "someColumn1"},
    {["token_name"] = "TK_COMMA", ["text"] = ","},
    {["token_name"] = "TK_LITERAL", ["text"] = COLUMN},
    {["token_name"] = "TK_COMMA", ["text"] = ","},
    {["token_name"] = "TK_LITERAL", ["text"] = "someColumn2"},
    {["token_name"] = "TK_CBRACE", ["text"] = ")"},
    {["token_name"] = "TK_SQL_VALUES", ["text"] = "VALUES"},
    {["token_name"] = "TK_OBRACE", ["text"] = "("},
    {["token_name"] = "TK_STRING", ["text"] = "someValue1"},
    {["token_name"] = "TK_COMMA", ["text"] = ","},
    {["token_name"] = "TK_STRING", ["text"] = PARTITION_VALUE},
    {["token_name"] = "TK_COMMA", ["text"] = ","},
    {["token_name"] = "TK_STRING", ["text"] = "someValue2"},
    {["token_name"] = "TK_CBRACE", ["text"] = ")"}
}

local CREATE_TABLE_TOKENS = {
    {["token_name"] = "TK_SQL_CREATE", ["text"] = "CREATE"},
    {["token_name"] = "TK_SQL_TABLE", ["text"] = "TABLE"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_OBRACE", ["text"] = "("},
    {["token_name"] = "TK_LITERAL", ["text"] = "someColumn1"},
    {["token_name"] = "TK_LITERAL", ["text"] = "INT"},
    {["token_name"] = "TK_CBRACE", ["text"] = ")"}
}

local ALTER_TABLE_TOKENS = {
    {["token_name"] = "TK_SQL_ALTER", ["text"] = "ALTER"},
    {["token_name"] = "TK_SQL_TABLE", ["text"] = "TABLE"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_SQL_ADD", ["text"] = "ADD"},
    {["token_name"] = "TK_LITERAL", ["text"] = "someColumn1"},
    {["token_name"] = "TK_LITERAL", ["text"] = "INT"}
}

local DROP_TABLE_TOKENS = {
    {["token_name"] = "TK_SQL_DROP", ["text"] = "DROP"},
    {["token_name"] = "TK_SQL_TABLE", ["text"] = "TABLE"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE}
}

local DROP_TABLE_IF_EXISTS_TOKENS = {
    {["token_name"] = "TK_SQL_DROP", ["text"] = "DROP"},
    {["token_name"] = "TK_SQL_TABLE", ["text"] = "TABLE"},
    {["token_name"] = "TK_SQL_IF", ["text"] = "IF"},
    {["token_name"] = "TK_SQL_EXISTS", ["text"] = "EXISTS"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE}
}

local LOAD_DATA_INFILE = {
    {["token_name"] = "TK_SQL_LOAD", ["text"] = "LOAD"},
    {["token_name"] = "TK_SQL_DATA", ["text"] = "DATA"},
    {["token_name"] = "TK_SQL_INFILE", ["text"] = "INFILE"},
    {["token_name"] = "TK_STRING", ["text"] = "testFile"},
    {["token_name"] = "TK_SQL_INTO", ["text"] = "INTO"},
    {["token_name"] = "TK_SQL_TABLE", ["text"] = "TABLE"},
    {["token_name"] = "TK_LITERAL", ["text"] = TABLE},
    {["token_name"] = "TK_COMMENT", ["text"] = "hscale.partitionKey(" .. TABLE .. ") = '" .. PARTITION_VALUE .. "'"}
}

function TestQueryAnalyzer:setUp()
end

function TestQueryAnalyzer:testCreateTable()
    self:_runDdlWithTokens(CREATE_TABLE_TOKENS, "CREATE TABLE")
end

function TestQueryAnalyzer:testAlterTable()
    self:_runDdlWithTokens(ALTER_TABLE_TOKENS, "ALTER TABLE")
end

function TestQueryAnalyzer:testDropTable()
    self:_runDdlWithTokens(DROP_TABLE_TOKENS, "DROP TABLE")
end

function TestQueryAnalyzer:testDropTableIfExists()
    self:_runDdlWithTokens(DROP_TABLE_IF_EXISTS_TOKENS, "DROP TABLE")
end

function TestQueryAnalyzer:testSelect()
    self:_runDmlWithTokens(SELECT_TOKENS, "SELECT")
end

function TestQueryAnalyzer:testUpdate()
    self:_runDmlWithTokens(UPDATE_TOKENS, "UPDATE")
end

function TestQueryAnalyzer:testDelete()
    self:_runDmlWithTokens(DELETE_TOKENS, "DELETE")
end

function TestQueryAnalyzer:testLimit()
    local analyzer = self:_runDmlWithTokens(SELECT_TOKENS, "SELECT")
    assertEquals(false, analyzer:hasLimit())
    local from, to = analyzer:getLimit()
    assertEquals(-1, from)
    assertEquals(-1, to)

    local analyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(SELECT_TOKENS_LIMIT_1, TABLE_KEY_COLUMNS)
    analyzer:analyze()
    assertEquals(true, analyzer:hasLimit())
    local from, rows = analyzer:getLimit()
    assertEquals(-1, from)
    assertEquals(1, rows)

    local analyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(SELECT_TOKENS_LIMIT_2, TABLE_KEY_COLUMNS)
    analyzer:analyze()
    assertEquals(true, analyzer:hasLimit())
    local from, rows = analyzer:getLimit()
    assertEquals(1, from)
    assertEquals(50, rows)

    local analyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(SELECT_TOKENS_LIMIT_3, TABLE_KEY_COLUMNS)
    analyzer:analyze()
    assertEquals(true, analyzer:hasLimit())
    local from, rows = analyzer:getLimit()
    assertEquals(1, from)
    assertEquals(50, rows)
end

function TestQueryAnalyzer:testLoadDataInfile()
    local queryAnalyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(LOAD_DATA_INFILE, TABLE_KEY_COLUMNS)
    queryAnalyzer:analyze()
    assertEquals(true, queryAnalyzer:isDml())
    assertEquals(false, queryAnalyzer:isDdl())
    assertEquals(true, queryAnalyzer:isPartitioningNeeded())
    assertEquals("LOAD DATA INFILE", queryAnalyzer:getStatementType())
    assertEquals(PARTITION_VALUE, queryAnalyzer:getTableKeyValues()[TABLE])
    assertEquals(1, #queryAnalyzer:getAffectedTables())
    assertEquals(TABLE, queryAnalyzer:getAffectedTables()[1])
end

function TestQueryAnalyzer:testInsertReplace()
    -- This test is a bit more complicated but we want to handle INSERT and REPLACE within one test
    -- since both are almost similar.
    local queries = {
        ["INSERT INTO SET ..."] = INSERT_TOKENS_1,
        ["INSERT INTO () VALUES () ..."] = INSERT_TOKENS_2,
        ["REPLACE INTO SET ..."] = INSERT_TOKENS_1,
        ["REPLACE INTO () VALUES () ..."] = INSERT_TOKENS_2
    }
    for queryName, query in ipairs(queries) do
        local expectedStatementType = "INSERT"
        if (queryName:find("REPLACE")) then
            query[1].token_name = "TK_SQL_REPLACE"
            query[1].text = "REPLACE"
            expectedStatementType = "REPLACE"
        end
        print("Type: " .. queryName)
        self:_runDmlWithTokens(query, expectedStatementType)
    end
end

-- Common method to test basic DML tokens.
function TestQueryAnalyzer:_runDmlWithTokens(tokens, expectedStatementType)
    local queryAnalyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(tokens, TABLE_KEY_COLUMNS)
    queryAnalyzer:analyze()
    assertEquals(true, queryAnalyzer:isDml())
    assertEquals(false, queryAnalyzer:isDdl())
    assertEquals(true, queryAnalyzer:isPartitioningNeeded())
    assertEquals(expectedStatementType, queryAnalyzer:getStatementType())
    assertEquals(PARTITION_VALUE, queryAnalyzer:getTableKeyValues()[TABLE])
    assertEquals(1, #queryAnalyzer:getAffectedTables())
    assertEquals(TABLE, queryAnalyzer:getAffectedTables()[1])
    return queryAnalyzer
end

-- Common method to test basic DTD tokens.
function TestQueryAnalyzer:_runDdlWithTokens(tokens, expectedStatementType)
    local queryAnalyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(tokens, TABLE_KEY_COLUMNS)
    queryAnalyzer:analyze()
    assertEquals(false, queryAnalyzer:isDml())
    assertEquals(true, queryAnalyzer:isDdl())
    assertEquals(true, queryAnalyzer:isPartitioningNeeded())
    assertEquals(expectedStatementType, queryAnalyzer:getStatementType())
    assertEquals(nil, queryAnalyzer:getTableKeyValues()[TABLE])
    assertEquals(1, #queryAnalyzer:getAffectedTables())
    assertEquals(TABLE, queryAnalyzer:getAffectedTables()[1])
    return queryAnalyzer
end