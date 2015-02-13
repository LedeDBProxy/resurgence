--[[ $%BEGINLICENSE%$
 Copyright (c) 2009, 2012, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ --]]
local proto = require("mysql.proto")

---
-- Bug #46141 
--
--   .prepend() does handle the 3rd optional parameter
--
-- which leads to a not-working read_query_result() for queries that
-- got prepended to the query-queue
--
function read_query(packet)
	-- pass on everything that is not on the initial connection

	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	if packet:sub(2) == "SELECT 1" then
		proxy.queries:prepend(1, packet, { resultset_is_needed = true })

		return proxy.PROXY_SEND_QUERY
	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "(bug_41991-test) >" .. packet:sub(2) .. "<"
		}
		return proxy.PROXY_SEND_RESULT
	end
end

function read_query_result(inj)
	if inj.id == 1 then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "1", type = proxy.MYSQL_TYPE_STRING },
				},
				rows = {
					{ "2" }
				}
			}
		}
		return proxy.PROXY_SEND_RESULT
	end
end
