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

---
-- a flexible statement based load balancer with connection pooling
--
-- * build a connection pool of min_idle_connections for each backend and maintain
--   its size
-- * 
-- 
-- 

local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local auto_config = require("proxy.auto-config")

--- config
--
-- connection pool
if not proxy.global.config.rwsplit then
	proxy.global.config.rwsplit = {
        min_idle_connections = 1,
        mid_idle_connections = 2,
        max_idle_connections = 4,

		is_debug = true,
		is_slave_write_forbidden_set = false
	}
end

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_in_transaction       = false
local is_prepared             = false
local is_backend_conn_keepalive = true
local use_pool_conn = false

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server() 
	local is_debug = true
	-- local is_debug = proxy.global.config.rwsplit.is_debug
	-- make sure that we connect to each backend at least ones to 
	-- keep the connections to the servers alive
	--
	-- on read_query we can switch the backends again to another backend

	if is_debug then
		print("[connect_server] " .. proxy.connection.client.src.name)
	end

	local rw_ndx = 0

	-- init all backends 
	for i = 1, #proxy.global.backends do
		local s        = proxy.global.backends[i]
		local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
		local cur_idle = pool.users[""].cur_idle_connections

		pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
		pool.mid_idle_connections = proxy.global.config.rwsplit.mid_idle_connections
		pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections
		
		if is_debug then
			print("  [".. i .."].connected_clients = " .. s.connected_clients)
			print("  [".. i .."].pool.cur_idle     = " .. cur_idle)
			print("  [".. i .."].pool.max_idle     = " .. pool.max_idle_connections)
			print("  [".. i .."].pool.mid_idle     = " .. pool.mid_idle_connections)
			print("  [".. i .."].pool.min_idle     = " .. pool.min_idle_connections)
			print("  [".. i .."].type = " .. s.type)
			print("  [".. i .."].state = " .. s.state)
		end

		-- prefer connections to the master 
        if s.type == proxy.BACKEND_TYPE_RW and
            s.state ~= proxy.BACKEND_STATE_DOWN and
            ((cur_idle < pool.min_idle_connections and s.connected_clients < pool.max_idle_connections)
            or cur_idle > 0) then
            proxy.connection.backend_ndx = i
            break
        elseif s.type == proxy.BACKEND_TYPE_RO and
            s.state ~= proxy.BACKEND_STATE_DOWN and
            ((cur_idle < pool.min_idle_connections and s.connected_clients < pool.max_idle_connections)
            or cur_idle > 0) then
            proxy.connection.backend_ndx = i
            break
        elseif s.type == proxy.BACKEND_TYPE_RW and
            s.state ~= proxy.BACKEND_STATE_DOWN and
            rw_ndx == 0 then
            if cur_idle == 0 and s.connected_clients >= pool.max_idle_connections then
                print("pool:" .. i .. " is full")
                is_backend_conn_keepalive = false
            end
            rw_ndx = i
        end
	end

	if proxy.connection.backend_ndx == 0 then
		if is_debug then
			print("  [" .. rw_ndx .. "] taking master as default")
        end
        proxy.connection.backend_ndx = rw_ndx
    else
        is_backend_conn_keepalive = true
    end

	-- pick a random backend
	--
	-- we someone have to skip DOWN backends

	-- ok, did we got a backend ?

	if proxy.connection.server then 
		if is_debug then
			print("  using pooled connection from: " .. proxy.connection.backend_ndx)
		end

        use_pool_conn = true

		-- stay with it
		return proxy.PROXY_IGNORE_RESULT
	end

	if is_debug then
		print("  [" .. proxy.connection.backend_ndx .. "] idle-conns below min-idle")
	end

	-- open a new connection 
end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )
	local is_debug = true
	-- local is_debug = proxy.global.config.rwsplit.is_debug
	if is_debug then
        print("[read_auth_result] " .. proxy.connection.client.src.name)
        print("  using connection from: " .. proxy.connection.backend_ndx)
        print("  server address: " .. proxy.connection.server.dst.name)
    end
	if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
		-- auth was fine, disconnect from the server
        if not use_pool_conn and is_backend_conn_keepalive then
            proxy.connection.backend_ndx = 0
        else
            if is_debug then
                print("  no need to put the connection to pool ... ok")
            end
        end
        if is_debug then
            print("  (read_auth_result) ... ok")
        end
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
		-- we received either a 
		-- 
		-- * MYSQLD_PACKET_ERR and the auth failed or
		-- * MYSQLD_PACKET_EOF which means a OLD PASSWORD (4.0) was sent
		print("  (read_auth_result) ... not ok yet")
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
		-- auth failed
	end
end


--- 
-- read/write splitting
function read_query( packet )
	local is_debug = proxy.global.config.rwsplit.is_debug
	local cmd      = commands.parse(packet)
	local c        = proxy.connection.client
    local ps_cnt   = 0
    local conn_reserved = false

	local r = auto_config.handle(cmd)
	if r then return r end

	local tokens
	local norm_query

    if is_prepared then
        ps_cnt = proxy.connection.valid_prepare_stmt_cnt
        if is_debug then
            print("  valid_prepare_stmt_cnt:" .. ps_cnt)
        end
    end

	-- looks like we have to forward this statement to a backend
	if is_debug then
		print("[read_query] " .. proxy.connection.client.src.name)
		print("  current backend   = " .. proxy.connection.backend_ndx)
		print("  client default db = " .. c.default_db)
		print("  client username   = " .. c.username)
		if cmd.type == proxy.COM_QUERY then 
			print("  query             = "        .. cmd.query)
		end
	end

	if cmd.type == proxy.COM_QUIT and is_backend_conn_keepalive  then
		-- don't send COM_QUIT to the backend. We manage the connection
		-- in all aspects.
		-- proxy.response = {
		--	type = proxy.MYSQLD_PACKET_OK,
		-- }
	
		if is_debug then
            print("  valid_prepare_stmt_cnt:" .. ps_cnt)
			print("  (QUIT) current backend   = " .. proxy.connection.backend_ndx)
		end

		return proxy.PROXY_SEND_NONE
	end
	
	-- COM_BINLOG_DUMP packet can't be balanced
	--
	-- so we must send it always to the master
	if cmd.type == proxy.COM_BINLOG_DUMP then
		-- if we don't have a backend selected, let's pick the master
		--
		if proxy.connection.backend_ndx == 0 then
			proxy.connection.backend_ndx = lb.idle_failsafe_rw()
		end

		return
	end

	proxy.queries:append(1, packet, { resultset_is_needed = true })

	-- read/write splitting 
	--
	-- send all non-transactional SELECTs to a slave
    if not is_in_transaction and
        cmd.type == proxy.COM_QUERY then
        tokens     = tokens or assert(tokenizer.tokenize(cmd.query))

        local stmt = tokenizer.first_stmt_token(tokens)

        if stmt.token_name == "TK_SQL_SELECT" then
            is_in_select_calc_found_rows = false
            local is_insert_id = false

            for i = 2, #tokens do
                local token = tokens[i]
                -- SQL_CALC_FOUND_ROWS + FOUND_ROWS() have to be executed
                -- on the same connection
                -- print("token: " .. token.token_name)
                -- print("  val: " .. token.text)

                if not is_in_select_calc_found_rows and token.token_name == "TK_SQL_SQL_CALC_FOUND_ROWS" then
                    is_in_select_calc_found_rows = true
                elseif not is_insert_id and token.token_name == "TK_LITERAL" then
                    local utext = token.text:upper()

                    if utext == "LAST_INSERT_ID" or
                        utext == "@@INSERT_ID" then
                        is_insert_id = true
                    end
                end

                -- we found the two special token, we can't find more
                if is_insert_id and is_in_select_calc_found_rows then
                    break
                end
            end

            -- if we ask for the last-insert-id we have to ask it on the original 
            -- connection
            if not is_insert_id then
                local backend_ndx = lb.idle_ro()

                if is_debug then
                    print("  [pure select statement] ")
                end

                if backend_ndx > 0 then
                    proxy.connection.backend_ndx = backend_ndx
                end
            else
                conn_reserved = true
                if is_debug then
                    print("  [this select statement should use the same connection] ")
                end
            end
        else
            if is_debug then
                print("  [query but not select statement, reuse connection] ")
            end
        end
    else
        if is_in_transaction then
            conn_reserved = true
            if is_debug then
                print("  [transaction statement, should use the same connection] ")
            end
        else
            if cmd.type == proxy.COM_STMT_PREPARE then
                is_prepared = true
                conn_reserved = true
                if is_debug then
                    print("  [prepare statement], cmd:" .. cmd.query)
                end
                tokens     = tokens or assert(tokenizer.tokenize(cmd.query))
                local stmt = tokenizer.first_stmt_token(tokens)

                if stmt.token_name == "TK_SQL_SELECT" then
                    local session_read_only = 0
                    for i = 2, #tokens do
                        local token = tokens[i]
                        if (token.token_name == "TK_COMMENT") then
                            if is_debug then
                                print("  [check readonly for ps]")
                            end
                            local _, _, readonly= string.find(token.text, "sesson_read_only=%s*(%d)")
                            if readonly then
                                session_read_only = 1
                                break
                            end
                        end
                    end

                    if session_read_only == 1 then
                        local backend_ndx = lb.idle_ro()

                        if is_debug then
                            print("  [pure readonly statement] ")
                        end

                        if backend_ndx > 0 then
                            proxy.connection.backend_ndx = backend_ndx
                        end
                    end
                end
            elseif ps_cnt > 0 then
                conn_reserved = true
                if is_debug then
                    print("  [prepare keeping connection]")
                end
            end
        end
    end

    c.is_server_conn_reserved = conn_reserved

	if proxy.connection.backend_ndx == 0 then
        local backend_ndx = lb.idle_failsafe_rw()
        if backend_ndx <= 0 and proxy.global.config.rwsplit.is_slave_write_forbidden_set then
            backend_ndx = lb.idle_ro()
        end

        proxy.connection.backend_ndx = backend_ndx
	end

	-- by now we should have a backend
	--
	-- in case the master is down, we have to close the client connections
	-- otherwise we can go on
	if proxy.connection.backend_ndx == 0 then
		return proxy.PROXY_SEND_QUERY
	end

    if cmd_type == proxy.COM_STMT_EXECUTE then
        proxy.connection.selected_server_ndx = lua_proto_change_stmt_id_from_client_stmt_execute_packet(inj.query)
    end

	local s = proxy.connection.server

	-- if client and server db don't match, adjust the server-side 
	--
	-- skip it if we send a INIT_DB anyway
	if cmd.type ~= proxy.COM_INIT_DB and 
	   c.default_db and c.default_db ~= s.default_db then
		print("    server default db: " .. s.default_db)
		print("    client default db: " .. c.default_db)
		print("    syncronizing")
		proxy.queries:prepend(2, string.char(proxy.COM_INIT_DB) .. c.default_db, { resultset_is_needed = true })
	end

	-- send to master
	if is_debug then
		if proxy.connection.backend_ndx > 0 then
			local b = proxy.global.backends[proxy.connection.backend_ndx]
			print("  sending to backend : " .. b.dst.name)
			print("    is_slave         : " .. tostring(b.type == proxy.BACKEND_TYPE_RO))
			print("    server default db: " .. s.default_db)
			print("    server username  : " .. s.username)
		end
		print("    in_trans        : " .. tostring(is_in_transaction))
		print("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
		print("    COM_QUERY       : " .. tostring(cmd.type == proxy.COM_QUERY))
	end

    if cmd_type == proxy.COM_STMT_EXECUTE then
        proxy.queries:append(3, packet, { resultset_is_needed = true } )
    elseif cmd_type == proxy.COM_STMT_PREPARE then
        proxy.queries:append(1, packet, { resultset_is_needed = true } )
    end

	return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj ) 
	local is_debug = proxy.global.config.rwsplit.is_debug
	local res      = assert(inj.resultset)
  	local flags    = res.flags
    local server_index

	if inj.id ~= 1 && inj.id ~= 3 then
		-- ignore the result of the USE <default_db>
		-- the DB might not exist on the backend, what do do ?
		--
		if inj.id == 2 then
			-- the injected INIT_DB failed as the slave doesn't have this DB
			-- or doesn't have permissions to read from it
			if res.query_status == proxy.MYSQLD_PACKET_ERR then
				proxy.queries:reset()

				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't change DB ".. proxy.connection.client.default_db ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}

				return proxy.PROXY_SEND_RESULT
			end
		end
		return proxy.PROXY_IGNORE_RESULT
	end

	is_in_transaction = flags.in_trans

    server_index = proxy.connection.selected_server_ndx;

    if server_index > 0 then
        if inj.id == 3 then
            change_stmt_id_from_server_prepare_ok(inj.query, server_index)
        else
            change_stmt_id_from_server_stmt_execute_packet(inj.query, server_index)
        end
    end
end

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
	local is_debug = true
	-- local is_debug = proxy.global.config.rwsplit.is_debug
	if is_debug then
		print("[disconnect_client] " .. proxy.connection.client.src.name)
	end
    
    if not is_backend_conn_keepalive then 
        if is_debug then
            print("  set connection_close true ")
        end
        proxy.connection.connection_close = true
    else
        -- make sure we are disconnection from the connection
        -- to move the connection into the pool
        proxy.connection.backend_ndx = 0
    end
end

