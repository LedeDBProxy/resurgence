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

--- Test the queryAnalyzer only.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-23 23:16:17 +0200 (Mi, 23 Apr 2008) $

tokenizer = require("proxy.tokenizer")

require("optivo.hscale.queryAnalyzer")

-- Load configuration.
config = require("optivo.hscale.config")
tableKeyColumns = config.get("tableKeyColumns")

-- Instantiate the partition lookup module
partitionLookup = require(config.get("partitionLookupModule"))
partitionLookup.init(config)

--- Analyze and rewrite queries.
function read_query(packet)
   if packet:byte() == proxy.COM_QUERY then
       local query = packet:sub(2)
       local tokens = tokenizer.tokenize(query)
       local queryAnalyzer = optivo.hscale.queryAnalyzer.QueryAnalyzer.create(tokens, tableKeyColumns)
       local success, errorMessage = pcall(queryAnalyzer.analyze, queryAnalyzer)
   end
end
