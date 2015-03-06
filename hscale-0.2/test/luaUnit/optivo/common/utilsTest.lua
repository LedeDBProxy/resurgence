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

--- Unit tests.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-08 16:05:25 +0200 (Di, 08 Apr 2008) $ $Rev: 23 $

require('luaunit')
local utils = require("optivo.common.utils")

TestUtils = {}

function TestUtils:setUp()
end

function TestUtils:testConvertTableToString()
    assertEquals("key = value", utils.convertTableToString({["key"] = "value"}))
    assertEquals(
        "key1 = value1\nkey2 = value2",
        utils.convertTableToString({
            ["key1"] = "value1",
            ["key2"] = "value2"
        })
    )
    local complexTable =
        {
            ["key1"] = "value1",
            ["key2"] = {
                ["key2_1"] = "value2_1",
                ["key2_2"] = {
                    ["key3_1"] = "value3_1",
                    ["key3_2"] = "value3_2"
                }
            }
        }
    assertEquals(
        "key1 = value1\nkey2 = {\n  key2_1 = value2_1\n  key2_2 = {\n    key3_2 = value3_2\n    key3_1 = value3_1\n  }\n}",
        utils.convertTableToString(complexTable, 2)
    )
    assertEquals(
        "key1 = value1\nkey2 = {\n  key2_1 = value2_1\n  key2_2 = [... more table data ...]\n}",
        utils.convertTableToString(complexTable, 1)
    )
end

function TestUtils:testSimpleChecksum()
    assertEquals(448, utils.calculateSimpleChecksum("test"))
    assertEquals(48, utils.calculateSimpleChecksum("test", 200))
end