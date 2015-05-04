--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, 2012, Oracle and/or its affiliates. All rights reserved.

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


function set_error(errmsg) 
	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = errmsg or "error"
	}
end

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		set_error("[admin] we only handle text-based queries (COM_QUERY)")
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2)

	local rows = { }
	local fields = { }

	if query:lower() == "select * from backends" then
		fields = { 
			{ name = "backend_ndx", 
			  type = proxy.MYSQL_TYPE_LONG },

			{ name = "address",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "state",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "type",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "uuid",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "connected_clients", 
			  type = proxy.MYSQL_TYPE_LONG },
		}

		for i = 1, #proxy.global.backends do
			local states = {
				"unknown",
				"up",
				"down"
			}
			local types = {
				"unknown",
				"rw",
				"ro"
			}
			local b = proxy.global.backends[i]

			rows[#rows + 1] = {
				i,
				b.dst.name,          -- configured backend address
				states[b.state + 1], -- the C-id is pushed down starting at 0
				types[b.type + 1],   -- the C-id is pushed down starting at 0
				b.uuid,              -- the MySQL Server's UUID if it is managed
				b.connected_clients  -- currently connected clients
			}
		end
    elseif query:lower() == "select version" then
        fields = { 
            { name = "version",
            type = proxy.MYSQL_TYPE_STRING },
        }   
        rows[#rows + 1] = { "1.0.0" }
	elseif query:lower() == "select * from help" then
		fields = { 
			{ name = "command", 
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "description", 
			  type = proxy.MYSQL_TYPE_STRING },
		}
		rows[#rows + 1] = { "SELECT * FROM help", "shows this help" }
		rows[#rows + 1] = { "SELECT * FROM backends", "lists the backends and their state" }
		rows[#rows + 1] = { "ADD SLAVE $backend", "add MySQL instance to the slave list" }
		rows[#rows + 1] = { "ADD MASTER $backend", "add MySQL instance to the master list" }
		rows[#rows + 1] = { "REMOVE BACKEND $backend_id", "remove one MySQL instance" }
        rows[#rows + 1] = { "SELECT VERSION", "display the version of MySQL proxy" }
	elseif string.find(query:lower(), "add slave") then
        local server = string.match(query:lower(), "add slave%s+(.+)$")
        proxy.global.backends.slave_add = server
        fields = {
            { name = "status", 
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "please use 'SELECT * FROM backend' to check if it succeeded " }
    elseif string.find(query:lower(), "add master") then
        local server = string.match(query:lower(), "add master%s+(.+)$")
        proxy.global.backends.master_add = server
        fields = {
            { name = "status", 
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "please use 'SELECT * FROM backend' to check if it succeeded " }
    elseif string.find(query:lower(), "remove backend") then
        local server_id = tonumber(string.match(query:lower(), "remove backend%s+(.+)$"))
        if server_id <= 0 or server_id > #proxy.global.backends then
            set_error("invalid backend_id")
            return proxy.PROXY_SEND_RESULT
        else
            proxy.global.backends.backend_remove = server_id - 1
            fields = {
                { name = "status", 
                type = proxy.MYSQL_TYPE_STRING },
            }
            rows[#rows + 1] = { "please use 'SELECT * FROM backend' to check if it succeeded " }
        end
	else
		set_error("use 'SELECT * FROM help' to see the supported commands")
		return proxy.PROXY_SEND_RESULT
	end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_OK,
		resultset = {
			fields = fields,
			rows = rows
		}
	}
	return proxy.PROXY_SEND_RESULT
end
