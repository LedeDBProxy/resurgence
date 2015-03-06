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

--[[
    Just run all tests available.

    @author $Author: peter.romianowski $
    @version $Date: 2008-04-08 02:53:18 +0200 (Di, 08 Apr 2008) $ $Rev: 17 $
--]]

require("luaunit")
require("optivo.hscale.queryAnalyzerTest")
require("optivo.common.utilsTest")

-- We are used to expressions like assertEquals(expected, actual)
USE_EXPECTED_ACTUAL_IN_ASSERT_EQUALS = false

LuaUnit:run()