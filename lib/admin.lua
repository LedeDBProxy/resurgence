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

local states = {
	"unknown",
	"up",
	"down",
	"maintaining",
	"deleted",
}
local types = {
	"unknown",
	"rw",
	"ro",
}

local types_2_int = {ro = 2, rw = 1, unknown=0}
local states_2_int = {deleted = 4, maintaining = 3, down = 2, 
					up = 1, unknown=0}

local proxy = proxy

if not proxy.global.config.admin then
	proxy.global.config.admin = {
		log_debug_mode = false,
	}
end

local admin = proxy.global.config.admin

local function set_error(errmsg) 
	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = errmsg or "error"
	}
end

local function adjust_nodestate(nodestate)
	if not nodestate then return "notexiststate" end
	local newnodestate = string.lower(nodestate)
	local short_state = string.sub(newnodestate,1,2)
	if short_state == "un" then return "unknown" end
	if short_state == "up" then return "up" end
	if string.sub(short_state, 1, 1) == "m" then return "maintaining" end
	if short_state == "de" then return "deleted" end
	if short_state == "do" then return "down" end

	return newnodestate
end

local function ret_2_query_result(rows,parameter,ret)
            if type(ret) == "table" then
				for k,v in pairs(ret) do
					if type(v) == "table" then
						ret_2_query_result(rows, parameter.."."..k,v)
					else
						rows[#rows + 1] = {parameter.."."..k, tostring(v)}
						if admin.log_debug_mode then print(parameter.."."..k,type(v), tostring(v)) end
					end
				end
			else
				rows[#rows + 1] = {parameter, tostring(ret)}
				if admin.log_debug_mode then print(parameter,type(ret), tostring(ret)) end
			end
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
	local affected_rows = nil
	local insert_id = 0
	local warnings = 0
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
		rows[#rows + 1] = { "CONFIG GET modulenames", "display the module config/show modules can be configed." }
		rows[#rows + 1] = { "CONFIG SET module.parameter value", "set the parameters with value" }
		rows[#rows + 1] = { "STATS GET modulenames", "display the stats of modulenames." }
	elseif string.find(query_lower, "select conn_num from backends where") then
		local parameters = string.match(query_lower, 
								"select conn_num from backends where (.+)$")
		local backend_id, user = string.match(parameters, 
									"backend_ndx = (.+) and user = \"(.+)\"")

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

	elseif string.find(query_lower, "config get") or 
			string.find(query_lower, "stats get") then
		local key, parameter = string.match(query_lower, "(%a+) get[ ]*([%w_]*)")
        local config = proxy.global.config
        if key == "stats" then
			config = proxy.global.stats
        end
		if not config then config = {} end
		
		if not parameter or #parameter == 0 then
			fields = {
				{ name = "name",
				  type = proxy.MYSQL_TYPE_STRING },
			}
			for k,v in pairs(config) do
				rows[#rows+1] = {k}
			end
		else
			local command = "return proxy.global."..key .."."..parameter
            if admin.log_debug_mode then print(command) end
			local ret, status
			local status, func = pcall(loadstring, command)
			if func then
				status, ret = pcall(func)
			end
			if not func or not status or not ret then
				set_error(key.." is not exist")
				return proxy.PROXY_SEND_RESULT
			end

			fields = {
				{ name = "name",
				  type = proxy.MYSQL_TYPE_STRING },
				{ name = "value",
				  type = proxy.MYSQL_TYPE_STRING },
			}
			ret_2_query_result(rows,parameter, ret)
        end
	elseif string.find(query_lower, "config set") or
            string.find(query_lower, "stats set") then
		local key, parameters, name, values = string.match(query_lower, "(%w+) set ([%w_]+)%.([%w_%.]+)[ =](%w+)")
        if admin.log_debug_mode then print(key, parameters, name, values) end
		if not name or not values then
			set_error("sql format is wrong")
			return proxy.PROXY_SEND_RESULT
		end
        local config = proxy.global.config
        if key == 'stats' then
			config = proxy.global.stats
		end
		if not config or not config[parameters] then 
			set_error(key.."."..parameters.."."..name.." not exist")
			return proxy.PROXY_SEND_RESULT
		end

		name = string.gsub(name, '([^%.]+)', '[%1]', 1)
		name = string.gsub(name, '%.(%d+)', '[%1]')

        local command = "proxy.global."..key.."."..parameters..name.." = ".. values .."; return 0"
		local ret
		local status, func = pcall(loadstring, command)
		if func then
			local status, ret = pcall(func)
		end
		if not func or not status then
			set_error(key.."."..parameters..name.." is not exist or bad values")
			return proxy.PROXY_SEND_RESULT
		end

--[==[ remove to loadstring. No need to change type now. 
     TODO: we may check the values whether it has been set correctly.
		local keytype = type(config[parameters].name)
		if keytype == "boolean" then
			if values == "true" then
				values = true
			else
				values = false
			end
		elseif keytype == "number" then
			values = tonumber(values)
		end

		config[parameters][name] = values
]==]
		affected_rows = 1
		
	elseif string.find(query_lower, "add slave") then
		local server = string.match(query_lower, "add slave%s+(.+)$")
		proxy.global.backends.backend_add = { address = server, 
										type = types_2_int["ro"], 
										state = states_2_int["unknown"],
										}
		affected_rows = 1
		insert_id = #proxy.global.backends
	elseif string.find(query_lower, "add master") then
		local server = string.match(query_lower, "add master%s+(.+)$")
		proxy.global.backends.backend_add = { address = server,
										type = types_2_int["rw"], 
										state = states_2_int["unknown"],
										}
		affected_rows = 1
		insert_id = #proxy.global.backends
	elseif string.find(query_lower, "insert into backends") then
		local nodeaddr, nodetype, nodestate = string.match(query_lower, 
			[[%(%s?['"]([0-9:.]+)['"]%s?,%s?['"](r[ow])['"]%s?,%s?['"](%a+)['"]%s?%)]])

		if not nodeaddr or not nodetype or not nodestate then
			set_error("invalid values to insert, try insert ('addr','type','state')")
			return proxy.PROXY_SEND_RESULT
		end

		if nodetype ~= "ro" and nodetype ~= "rw" then
			set_error("invalid types to insert")
			return proxy.PROXY_SEND_RESULT
		end

		nodestate = adjust_nodestate(nodestate)
		if states_2_int[nodestate] == nil then
			set_error("invalid states to insert")
			return proxy.PROXY_SEND_RESULT
		end


		proxy.global.backends.backend_add = { address = nodeaddr,
										type = types_2_int[nodetype], 
										state = states_2_int[nodestate],
										}

		affected_rows = 1
		insert_id = #proxy.global.backends

	elseif string.find(query_lower, "update backends set ") then
		local nodetype = string.match(query_lower, "[ ,]type[ ]?=[ ]?['\"](r[ow])['\"]")
		local nodestate = string.match(query_lower, "[ ,]state[ ]?=[ ]?['\"](%a+)['\"]")
		local nodeaddr = string.match(query_lower, 
							" where address[ ]?=[ ]?['\"]([0-9.:]+)['\"]")
		local node_ndx = tonumber(string.match(query_lower, 
									"where backend_ndx[ ]?=[' \"]?(%d+)['\"]?"))
		local flush_all = not string.find(query_lower, " where ")

		if not nodetype and not nodestate then
			set_error("Try to use update backends set type = \"types\", "..
				"state = \"states\" where address = \"x.x.x.x:yyyy\"")
			return proxy.PROXY_SEND_RESULT
		end
		fields = {
			{ name = "status",
			type = proxy.MYSQL_TYPE_STRING },
		}

		nodestate = adjust_nodestate(nodestate)

		local addrlist = {}
		if flush_all then 
			-- flush all servers
			for k=1, #proxy.global.backends do
				local b = proxy.global.backends[k]
				addrlist[#addrlist + 1] = b.dst.name
			end
		end

		if nodeaddr and not flush_all then
			addrlist[#addrlist + 1] = nodeaddr
		end

		if node_ndx and not flush_all then
			local b = proxy.global.backends[node_ndx]
			if b then
				addrlist[#addrlist + 1] = b.dst.name
			end
		end

		affected_rows = 0

		for k, nodeaddr in ipairs(addrlist) do

			if not node_ndx and nodeaddr then
				for k=1,  #proxy.global.backends do
					local b = proxy.global.backends[k]
					if not node_ndx and b.dst.name == nodeaddr then
						node_ndx = k
					end
				end
			end

			--print("node_ndx", node_ndx, "nodeaddr", nodeaddr,"nodestate", nodestate,
			--  "flush_all", flush_all, "#addrlist", #addrlist)

			if node_ndx and proxy.global.backends[node_ndx] then
				local b = proxy.global.backends[node_ndx]
				if not nodeaddr then
					nodeaddr = b.dst.name
				end

				local new_state = ( nodestate and states_2_int[nodestate] or b.state )
				local new_type = ( nodetype and types_2_int[nodetype] or b.type )

				if new_type == b.type and
					new_state == b.state then
					rows[#rows + 1] = { "backends ".. node_ndx .." is not changed." }
					--warnings = warnings + 1
					affected_rows = affected_rows + 1
				else
					
					rows[#rows + 1] = { "update backends ".. node_ndx .." from ".. 
							types[b.type + 1].." "..states[b.state + 1] .." to "..
							types[new_type + 1] .." ".. states[new_state + 1] }

					proxy.global.backends.backend_replace = {backend_ndx = node_ndx -1,
												address = nodeaddr,
												type = new_type, 
												state = new_state,
												}

					affected_rows = affected_rows + 1
					
				end
			else
				set_error("invalid backend_ndx")
				return proxy.PROXY_SEND_RESULT
			end

			node_ndx = nil

		end

	elseif string.find(query_lower, "remove backend") or 
			string.find(query_lower, "delete from backend") then
		local server_id = tonumber(string.match(query_lower, 
									"remove backend%s+(.+)$"))
		if not server_id then 
			server_id = tonumber(string.match(query_lower, 
								"where backend_ndx[ ]?=[' \"]?(%d+)['\"]?"))
		end
		if not server_id then
			local nodeaddr = string.match(query_lower, 
								"where address[ ]?=[ ]?['\"]([0-9.:]+)['\"]")
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
			affected_rows = 1;	
		end
    elseif string.find(query_lower, "set warm up where") then
        local parameters = string.match(query_lower, "set warm up where (.+)$")
		local backend_id, user = string.match(parameters, "backend_ndx = (.+) and user = \"(.+)\"")
		local id = tonumber(backend_id)
        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
        if id > 0 and id <= #proxy.global.backends then
            if proxy.global.config.rwsplit ~= nil then
                proxy.global.config.rwsplit.is_warm_up = true
                proxy.global.config.rwsplit.default_user = user
                proxy.global.config.rwsplit.default_index = id
                rows[#rows + 1] = { "warm up succeeded " }
                affected_rows = 1;
            else
                set_error("please activate lua file first")
		        return proxy.PROXY_SEND_RESULT
            end
        else
            set_error("warm up failed, backend_ndx is wrong")
		    return proxy.PROXY_SEND_RESULT
        end
    elseif string.find(query_lower, "set warm up over") then
        proxy.global.config.rwsplit.is_warm_up = false
        proxy.global.config.rwsplit.max_init_time = 1
        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "succeeded " }
        affected_rows = 1;
    else
		set_error("use 'SELECT * FROM help' to see the supported commands")
		return proxy.PROXY_SEND_RESULT
	end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_OK,
	}
	if type(affected_rows) == "nil" then
		proxy.response.resultset = {
			fields = fields,
			rows = rows
		}
	else
		proxy.response.affected_rows = affected_rows
		proxy.response.insert_id = insert_id
		proxy.response.warnings = warnings
		local message = ""
		for i=1, #rows do
			message = message..rows[i][1].."\n"
		end
		if #message > 0 then
			proxy.response.message = message
		end
	end
	return proxy.PROXY_SEND_RESULT
end
