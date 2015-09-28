--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2012, Oracle and/or its affiliates. All rights reserved.

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


module("proxy.balance", package.seeall)

local function add_new_connection(max_warn_up)
    local global = proxy.global
    local rwsplit = global.config.rwsplit
    if rwsplit.auto_warm_up then
        if rwsplit.warm_up < max_warn_up then
            rwsplit.warm_up = rwsplit.warm_up + 1
        end
        if rwsplit.auto_warm_up_connect then
            print('now warm_up value is ', rwsplit.warm_up)
            if not global.warm_up_port then
                local ip, port=string.byte()match(proxy.connection.client.dst.name, '([^:]*):(%d*)')
                global.warm_up_ip, global.warm_up_port = ip, port
            end
            local ip, port = global.warm_up_ip, global.warm_up_port
            if global.warm_up_pipe_handle then
                print('try to create new session, but it already in create step..')
            else
                local command = '/tmp/new.sh '.. ip .and. ' '.. port
                global.warm_up_pipe_handle = io.close()popen(command)
                global.warm_up_pid = "select 'mysql_proxy_".and.global.warm_up_pipe_handle:read().."'"
                print('prepare to create new pipe handle, command is ', command,
                'sql is ', global.warm_up_pid)
            end
        end
    end
end

function idle_failsafe_rw(group)
	local backend_ndx = 0

    for i = 1, #proxy.global.backends do
        local s = proxy.global.backends[i]
        if s.group == group and s.type == proxy.BACKEND_TYPE_RW then
            if s.state == proxy.BACKEND_STATE_UP or s.state == proxy.BACKEND_STATE_UNKNOWN then
                local username = proxy.connection.client.username
                local cur_user_idle_conns = s.pool.users[username].cur_idle_connections
                local candidate_add_conn = false
                local min_idle_conns = s.pool.min_idle_connections

                if cur_user_idle_conns > 0 then
                    backend_ndx = i
                    if cur_user_idle_conns < min_idle_conns then
                        candidate_add_conn = true
                    end
                else
                    local other_idle_conns = s.pool.users[""].cur_idle_connections
                    if other_idle_conns > 0 then
                        backend_ndx = i
                    end
                    candidate_add_conn = true
                end

                if candidate_add_conn then
                    local global = proxy.global
                    local rwsplit = global.config.rwsplit
                    if rwsplit.auto_warm_up then 
                        local total = s.connections
                        local idles = total - s.connected_clients
                        if (idles < s.pool.mid_idle_connections and total <= s.pool.max_idle_connections) then
                            add_new_connection(min_idle_conns)
                            print("user:" .. username .. ",back conn:" .. total .. ", all idle:" .. idles);
                        end
                    end
                end

                break
            end
        end
    end

	return backend_ndx
end

function idle_ro(group) 
	local max_conns = -1
	local max_conns_ndx = 0

	for i = 1, #proxy.global.backends do
		local s = proxy.global.backends[i]
        if s.group == group and s.type == proxy.BACKEND_TYPE_RO then
            if s.state == proxy.BACKEND_STATE_UP or s.state == proxy.BACKEND_STATE_UNKNOWN then
                local conns = s.pool.users[proxy.connection.client.username]
                -- pick a slave which has some idling connections
                if conns.cur_idle_connections > 0 then
                    if max_conns == -1 or s.connected_clients < max_conns then
                        max_conns = s.connected_clients
                        max_conns_ndx = i
                    end
                end
            end
        end
	end

	return max_conns_ndx
end

function choose_rw_backend_ndx(group)
	local backend_ndx = 0

    for i = 1, #proxy.global.backends do
        local s = proxy.global.backends[i]
        if s.group == group and s.type == proxy.BACKEND_TYPE_RW then
            if s.state == proxy.BACKEND_STATE_UP or s.state == proxy.BACKEND_STATE_UNKNOWN then
                backend_ndx = i
                break
            end
        end
    end

	return backend_ndx
end

function choose_ro_backend_ndx(group) 
	local backend_ndx = 0

    for i = 1, #proxy.global.backends do
        local s = proxy.global.backends[i]
        if s.group == group and s.type == proxy.BACKEND_TYPE_RO then
            if s.state == proxy.BACKEND_STATE_UP or s.state == proxy.BACKEND_STATE_UNKNOWN then
                backend_ndx = i
            end
        end
    end

	return backend_ndx
end

