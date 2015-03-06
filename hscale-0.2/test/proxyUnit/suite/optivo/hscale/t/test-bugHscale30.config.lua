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

--- Regression test for issue http://jira.hscale.org/browse/HSCALE-30
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-05-06 21:30:04 +0200 (Di, 06 Mai 2008) $ $Rev: 45 $

config = {}
config.partitionLookupModule = "optivo.hscale.modulusPartitionLookup"
config.tableKeyColumns = {
    ["a"] = "category",
    ["b"] = "folder"
}
config.partitionsPerTable = 11

return config