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

--- Just test the performance overhead of the mysql-proxy tokenizer
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-23 23:16:17 +0200 (Mi, 23 Apr 2008) $

tokenizer = require("proxy.tokenizer")

function read_query(packet)
    if packet:byte() == proxy.COM_QUERY then
        tokenizer.tokenize(packet:sub(2))
    end
end
