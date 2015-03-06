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

--- Configuration file for the test environment.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-05-06 21:30:04 +0200 (Di, 06 Mai 2008) $ $Rev: 45 $

config = {}

-- Define the partitioning lookup module to be used.
-- This module tells the application how to split your tables.
-- The Lua file must be found within your LUA_PATH.
config.partitionLookupModule = "optivo.hscale.modulusPartitionLookup"

-- Configure the tables to be partitioned (a, b) with the partition columns ("category", "folder").
config.tableKeyColumns = {
    ["a"] = "category",
    ["b"] = "folder"
}

-- Used for optivo.hscale.modulusPartitionLookup only - the number of partitions per table (i.e. the modulo value)
config.partitionsPerTable = 3

return config