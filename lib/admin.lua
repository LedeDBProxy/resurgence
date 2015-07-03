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
    if packet:byte() == proxy.COM_INIT_DB or packet:byte() == proxy.COM_QUIT then
        proxy.response = {
            type = proxy.MYSQLD_PACKET_OK,
            "omit command"
        }
        return proxy.PROXY_SEND_RESULT
    end
	if packet:byte() ~= proxy.COM_QUERY then
		set_error("[admin] we only handle text-based queries (COM_QUERY)")
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2)

	local rows = { }
	local fields = { }
	local query_lower = query:lower()

	if query_lower == "select * from backends" then
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

		-- used in the loop.
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

		for i = 1, #proxy.global.backends do
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
    elseif query_lower == "select version" then
        fields = {
            { name = "version",
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "1.0.0" }
	elseif query_lower == "select * from help" then
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
		rows[#rows + 1] = { "INSERT INTO backends values ('x.x.x.x:yyyy', 'ro|rw')", "add MySQL instance to the backends list" }
		rows[#rows + 1] = { "REMOVE BACKEND $backend_id", "remove one MySQL instance" }
		rows[#rows + 1] = { "DELETE FROM BACKENDS where backend_ndx = %d", "remove one MySQL instance" }
		rows[#rows + 1] = { "DELETE FROM BACKENDS where address = 'x.x.x.x:yyyy'", "remove one MySQL instance" }
		rows[#rows + 1] = { "UPDATE BACKENDS set type = 'rw|ro' where conditions", "update MySQL instance type" }
        rows[#rows + 1] = { "SELECT VERSION", "display the version of MySQL proxy" }
	elseif string.find(query_lower, "select conn_num from backends where") then
        local parameters = string.match(query_lower, "select conn_num from backends where (.+)$")
        local backend_id, user = string.match(parameters, "backend_ndx = (.+) and user = \"(.+)\"")

        if backend_id == nil  or user == nil then
            set_error("sql format is wrong")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
			{ name = "connection_num",
			  type = proxy.MYSQL_TYPE_LONG },
		}
        local id = tonumber(backend_id)
        if id > 0 and id <= #proxy.global.backends then
            local b = proxy.global.backends[id]
            local pool = b.pool

            rows[#rows + 1] = {
                pool.users[user].cur_idle_connections -- currently connected clients
            }
        else
            rows[#rows + 1] = {
                "",
                0
            }
        end

	elseif string.find(query_lower, "add slave") then
        local server = string.match(query_lower, "add slave%s+(.+)$")
        proxy.global.backends.slave_add = server
        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "please use 'SELECT * FROM backends' to check if it succeeded " }
    elseif string.find(query_lower, "add master") then
        local server = string.match(query_lower, "add master%s+(.+)$")
        proxy.global.backends.master_add = server
        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "please use 'SELECT * FROM backends' to check if it succeeded " }
	elseif string.find(query_lower, "insert into backends") then
		local nodeaddr, nodetype = string.match(query_lower, [[%(['"]([0-9:.]+)['"],['"](r[ow])['"]%)]])
		if not nodeaddr or not nodetype then
            set_error("invalid values to insert")
            return proxy.PROXY_SEND_RESULT
		end
		if nodetype == "ro" then
			proxy.global.backends.slave_add = nodeaddr;
			rows[#rows + 1] = { "add readonly node "..nodeaddr }
		elseif nodetype == "rw" then
			proxy.global.backends.master_add = nodeaddr;
			rows[#rows + 1] = { "add readwrite node "..nodeaddr }
		end
			
		fields = {
			{ name = "status",
			type = proxy.MYSQL_TYPE_STRING },
		}

		if #rows == 0 then
			rows[#rows + 1] = { "nothing to do." }
		end

	elseif string.find(query_lower, "update backends set type") then
		local nodetype = string.match(query_lower, " type[ ]?=[ ]?['\"](r[ow])['\"] ")
		local nodeaddr = string.match(query_lower, "where address[ ]?=[ ]?['\"]([0-9.:]+)['\"]")
		local node_ndx = tonumber(string.match(query_lower, "where backend_ndx[ ]?=[' \"]?(%d+)['\"]?"))
		if not (nodeaddr or node_ndx) and not nodetype then
            set_error("Try to use update backends set type = \"ro/rw\" where address = \"x.x.x.x:yyyy\"")
            return proxy.PROXY_SEND_RESULT
		end
		fields = {
			{ name = "status",
			type = proxy.MYSQL_TYPE_STRING },
		}
	
		if not node_ndx then
			for k=1,  #proxy.global.backends do
				local b = proxy.global.backends[k]
				if b.dst.name == nodeaddr then
					node_ndx = k
					break;
				end
			end
		end

		local types_2_int = {ro = 2, rw = 1, unkown=0}
		local types = {
			"unknown",
			"rw",
			"ro"
		}
	
		if node_ndx and proxy.global.backends[node_ndx] then
			local b = proxy.global.backends[node_ndx]
			if not nodeaddr then
				nodeaddr = b.dst.name
			end

			if types_2_int[nodetype] == b.type then
				rows[#rows + 1] = { "backends ".. node_ndx .." is not changed." }
			else
				rows[#rows + 1] = { "update backends ".. node_ndx .." from ".. 
						types[b.type + 1] .." to ".. nodetype }
				proxy.global.backends.backend_remove = node_ndx - 1

				if nodetype == "ro" then -- BACKEND_TYPE_RO
					proxy.global.backends.slave_add = nodeaddr
				elseif nodetype == "rw" then
					proxy.global.backends.master_add = nodeaddr
				end
			end
		else
            set_error("invalid backend_ndx")
            return proxy.PROXY_SEND_RESULT
		end
		if #rows == 0 then
			rows[#rows + 1] = { "no rows matched for update." }
		end
    elseif string.find(query_lower, "remove backend") or 
			string.find(query_lower, "delete from backend") then
        local server_id = tonumber(string.match(query_lower, "remove backend%s+(.+)$"))
		if not server_id then 
			server_id = tonumber(string.match(query_lower, "where backend_ndx[ ]?=[' \"]?(%d+)['\"]?"))
		end
		if not server_id then
			local nodeaddr = string.match(query_lower, "where address[ ]?=[ ]?['\"]([0-9.:]+)['\"]")
			if nodeaddr then
				for k=1,  #proxy.global.backends do
					local b = proxy.global.backends[k]
					if b.dst.name == nodeaddr then
						server_id = k
						break;
					end
				end
			end
		end

        if not server_id or (server_id <= 0 or server_id > #proxy.global.backends) then
            set_error("invalid backend_id")
            return proxy.PROXY_SEND_RESULT
        else
            proxy.global.backends.backend_remove = server_id - 1
            fields = {
                { name = "status",
                type = proxy.MYSQL_TYPE_STRING },
            }
            rows[#rows + 1] = { "please use 'SELECT * FROM backends' to check if it succeeded " }
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
