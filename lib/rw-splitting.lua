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
local proto       = require("mysql.proto")
local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local auto_config = require("proxy.auto-config")

local testsuit = false

--- config
--
-- connection pool
if not proxy.global.config.rwsplit then
    if testsuit then
        proxy.global.config.rwsplit = {
            min_idle_connections = 1,
            mid_idle_connections = 4,
            max_idle_connections = 8,
            -- = down--up for each backend
            max_init_time = 1,

            is_debug = false,
            is_slave_write_forbidden_set = false,
            auto_warm_up = false,
            auto_warm_up_connect = false,
            warm_up = 0,
        }
    else
        proxy.global.config.rwsplit = {
            min_idle_connections = 10,
            mid_idle_connections = 40,
            max_idle_connections = 80,
            max_init_time = 1,
            is_debug = false,
            is_slave_write_forbidden_set = false,
            auto_warm_up = false,
            auto_warm_up_connect = false,
            warm_up = 0,
        }
    end
end

--stat
if not proxy.global.stats then
    proxy.global.stats = {
       	query_info = {
            ro = 0,
            rw = 0,
        },
        backend_info = {
            ro = 0,
            rw = 0,
        },
        backend_details = {
        },
    }
end

local stats = proxy.global.stats

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_in_transaction          = false
local is_auto_commit             = true
local is_prepared                = false
local is_backend_conn_keepalive  = true
local use_pool_conn = false
local multiple_server_mode = false

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

local function return_insert_id(utext, last_insert_id)
    local rows = { }
    local fields = {
        {
            name = utext,
            type = proxy.MYSQL_TYPE_LONGLONG
        },
    }

    rows[#rows + 1] = {
        last_insert_id
    }

    proxy.response = {
        type = proxy.MYSQLD_PACKET_OK,
        resultset = {
            fields = fields,
            rows = rows
        }
    }
    return proxy.PROXY_SEND_RESULT
end


--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server() 
    local is_debug = proxy.global.config.rwsplit.is_debug
    -- make sure that we connect to each backend at least ones to 
    -- keep the connections to the servers alive
    --
    -- on read_query we can switch the backends again to another backend

    if is_debug then
        print("[connect_server] " .. proxy.connection.client.src.name)
    end

    if not proxy.global.stat_clients then
        proxy.global.stat_clients = 0
    end
    proxy.global.stat_clients = proxy.global.stat_clients + 1

    local rw_ndx = 0
    local max_idle_conns = 0
    local min_idle_conns = 0
    local mid_idle_conns = 0
    local cur_idle = 0
    local connected_clients = 0
    local total_clients = 0
    local total_available_conns = 0

    total_clients = proxy.global.stat_clients + 1

    -- init all backends 
    for i = 1, #proxy.global.backends do
        local s        = proxy.global.backends[i]
        local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
        local init_phase = false
        cur_idle = pool.users[""].cur_idle_connections
        if testsuit then
            local root_cur_idle = pool.users["root"].cur_idle_connections
            print("  root idle:" .. root_cur_idle)
            --init_phase = pool.init_phase
        else
            init_phase = pool.init_phase
        end
        connected_clients = s.connected_clients

        if connected_clients > 0 then
            pool.serve_req_after_init = true
        else
            pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
            pool.mid_idle_connections = proxy.global.config.rwsplit.mid_idle_connections
            pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections
            pool.max_init_time = proxy.global.config.rwsplit.max_init_time
        end

        if proxy.global.config.rwsplit.warm_up > 0 and
             s.type == proxy.BACKEND_TYPE_RW and
            (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) then
            proxy.connection.backend_ndx = i
            print('current warm_up is ', proxy.global.config.rwsplit.warm_up, 'create new connection')
            proxy.global.config.rwsplit.warm_up = proxy.global.config.rwsplit.warm_up -1
            return
        end

        if init_phase then
            local init_time = pool.init_time
            if init_time > 0 and not pool.serve_req_after_init then
                pool.set_init_time = 1
                init_time = pool.init_time
            end

            local max_init_time = proxy.global.config.rwsplit.max_init_time
            if max_init_time <= 0 then
                max_init_time = 1
            end
            if init_time == 0 then
                init_time = 1
            elseif init_time > max_init_time then
                init_time = max_init_time
            end
            min_idle_conns = proxy.global.config.rwsplit.min_idle_connections
            mid_idle_conns = math.floor(proxy.global.config.rwsplit.mid_idle_connections * init_time / max_init_time)
            max_idle_conns = math.floor(proxy.global.config.rwsplit.max_idle_connections * init_time / max_init_time)

            if mid_idle_conns < min_idle_conns then
                mid_idle_conns = min_idle_conns
            end

            max_idle_conns = mid_idle_conns + 1

            total_available_conns = total_available_conns + mid_idle_conns
        else
            min_idle_conns = proxy.global.config.rwsplit.min_idle_connections
            mid_idle_conns = proxy.global.config.rwsplit.mid_idle_connections
            max_idle_conns = proxy.global.config.rwsplit.max_idle_connections

            total_available_conns = total_available_conns + max_idle_conns
        end

        if pool.stop_phase then
            if is_debug then
                print("  connection will be rejected")
            end
            proxy.response = {
                type = proxy.MYSQLD_PACKET_ERR,
                errmsg = "proxy stops serving requests now"
            }
            return proxy.PROXY_SEND_RESULT
        end

        if is_debug then
            print("  [".. i .."].connected_clients = " .. connected_clients)
            print("  [".. i .."].pool.cur_idle     = " .. cur_idle)
            print("  [".. i .."].pool.max_idle     = " .. max_idle_conns)
            print("  [".. i .."].pool.mid_idle     = " .. mid_idle_conns)
            print("  [".. i .."].pool.min_idle     = " .. min_idle_conns)
            print("  [".. i .."].type = " .. s.type)
            print("  [".. i .."].state = " .. s.state)
        end

        -- prefer connections to the master 
        if s.type == proxy.BACKEND_TYPE_RW and
            (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) and
            (cur_idle < min_idle_conns and (connected_clients + cur_idle) < max_idle_conns) then
            proxy.connection.backend_ndx = i
            rw_ndx = i
            break
        elseif s.type == proxy.BACKEND_TYPE_RO and
            (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) and
            (cur_idle > min_idle_conns or 
            (cur_idle < min_idle_conns and (connected_clients + cur_idle) < max_idle_conns)) then
            proxy.connection.backend_ndx = i
            is_backend_conn_keepalive = true
            break
        elseif s.type == proxy.BACKEND_TYPE_RW and
            (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) and
            rw_ndx == 0 then
            if cur_idle == 0 and connected_clients >= max_idle_conns then
                if init_phase then 
                    if is_debug then
                        print("  connection will be rejected because init phase")
                    end
                    proxy.response = {
                        type = proxy.MYSQLD_PACKET_ERR,
                        errmsg = "proxy stops serving requests now"
                    }
                    return proxy.PROXY_SEND_RESULT
                else
                    is_backend_conn_keepalive = false
                end
            end
            rw_ndx = i
        end
    end

    if proxy.connection.backend_ndx == 0 then
        if is_debug then
            print("  [" .. rw_ndx .. "] taking master as default")
        end

        if rw_ndx > 0 then
            proxy.connection.backend_ndx = rw_ndx
        else
            if is_debug then 
                print("connection will be rejected")
            end
            proxy.response = {
                type = proxy.MYSQLD_PACKET_ERR,
                errmsg = "(proxy) all backends are down"
            }
            return proxy.PROXY_SEND_RESULT
        end
    else
        is_backend_conn_keepalive = true
    end

    local backend = proxy.global.backends[proxy.connection.backend_ndx]
    cur_idle = backend.pool.users[""].cur_idle_connections
    connected_clients =  backend.connected_clients

    if cur_idle > min_idle_conns and proxy.connection.server then 
        --if is_debug then
        --	print("  using pooled connection from: " .. proxy.connection.backend_ndx)
        --end
        local backend_state = backend.state
        if backend_state == proxy.BACKEND_STATE_UP then
            use_pool_conn = true
            if backend.type == proxy.BACKEND_TYPE_RW and (cur_idle > mid_idle_conns) and
                (cur_idle + connected_clients) > (max_idle_conns + min_idle_conns) then
                is_backend_conn_keepalive = false
                if is_debug then 
                    print("  [" .. proxy.connection.backend_ndx .. "] set conn keepalive false"); 
                end
            end
            -- stay with it
            return proxy.PROXY_IGNORE_RESULT
        end
    end

    --if is_debug then
    --	print("  [" .. proxy.connection.backend_ndx .. "] idle-conns below min-idle")
    --end

    -- open a new connection 
end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )
    local is_debug = proxy.global.config.rwsplit.is_debug

    --if is_debug then
    --    print("[read_auth_result] " .. proxy.connection.client.src.name)
    --    print("  using connection from: " .. proxy.connection.backend_ndx)
    --    print("  server address: " .. proxy.connection.server.dst.name)
    --    print("  server charset: " .. proxy.connection.server.character_set_client)
    --end
    if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
        -- auth was fine, disconnect from the server
        if not use_pool_conn and is_backend_conn_keepalive then
            proxy.connection.backend_ndx = 0
        else
            if is_debug then
                print("  no need to put the connection to pool ... ok")
            end
        end
        --if is_debug then
        --    print("  (read_auth_result) ... ok")
        --end
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
    local ro_server = false
    local rw_op = true
    local backend_ndx = proxy.connection.backend_ndx
    local charset_str
    local is_charset_reset = false
    local is_charset_set = false
    local is_charset_client = false
    local is_charset_connection = false
    local is_charset_results = false
    local sql_mode_set = false
    local charset_client 
    local charset_connection
    local charset_results

    local r = auto_config.handle(cmd)
    if r then return r end

    local tokens
    local norm_query


    if is_prepared then
        ps_cnt = proxy.connection.valid_prepare_stmt_cnt
    end

    if backend_ndx > 0 then
        local b = proxy.global.backends[backend_ndx]
        if b.type == proxy.BACKEND_TYPE_RO then
            ro_server = true
        end

        if b.state ~= proxy.BACKEND_STATE_UP then
            if ro_server == true then
                local rw_backend_ndx = lb.idle_failsafe_rw()
                if rw_backend_ndx > 0 then
                    backend_ndx = rw_backend_ndx
                    proxy.connection.backend_ndx = backend_ndx
                else
                    local ro_backend_ndx = lb.idle_ro()
                    if ro_backend_ndx > 0 and ro_backend_ndx ~= backend_ndx then
                        backend_ndx = ro_backend_ndx
                        proxy.connection.backend_ndx = backend_ndx
                    else
                        if is_debug then
                            print('both master/slave have no idle connection');
                        end
                        proxy.response = {
                            type = proxy.MYSQLD_PACKET_ERR,
                            errmsg = "1,master/slave connections are too small"
                        }
                        return proxy.PROXY_SEND_RESULT
                    end
                end
            else
                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "proxy stops serving requests now"
                }
                return proxy.PROXY_SEND_RESULT
            end
        end

        if b.pool.stop_phase then
            if is_debug then
                print("  stop serving requests")
            end
            proxy.response = {
                type = proxy.MYSQLD_PACKET_ERR,
                errmsg = "proxy stops serving requests now"
            }
            return proxy.PROXY_SEND_RESULT
        end
    end

    -- looks like we have to forward this statement to a backend
    if is_debug then
        print("[read_query] " .. proxy.connection.client.src.name)
        print("  current backend   = " .. backend_ndx)
        print("  client default db = " .. c.default_db)
        print("  client username   = " .. c.username)
        if cmd.type == proxy.COM_QUERY then 
            print("  query             = "        .. cmd.query)
        end
    end

    if cmd.type == proxy.COM_QUIT then
        if proxy.global.warm_up_pipe_handle then
            if (proxy.global.warm_up_conn and proxy.global.warm_up_conn == proxy.connection) then
                print('close old pipe handle', proxy.global.warm_up_pipe_handle,
                      'for connection', proxy.global.warm_up_conn)
                io.close(proxy.global.warm_up_pipe_handle)
                proxy.global.warm_up_pipe_handle = nil
                proxy.global.warm_up_conn = nil
            end
        end

        if backend_ndx <= 0 or (is_backend_conn_keepalive and not is_in_transaction) then
            -- don't send COM_QUIT to the backend. We manage the connection
            -- in all aspects.
            -- proxy.response = {
            --	type = proxy.MYSQLD_PACKET_OK,
            -- }

            if is_debug then
                print("  valid_prepare_stmt_cnt:" .. ps_cnt)
                print("  (QUIT) current backend   = " .. backend_ndx)
            end

            return proxy.PROXY_SEND_NONE
        end
    end

    -- COM_BINLOG_DUMP packet can't be balanced
    --
    -- so we must send it always to the master
    if cmd.type == proxy.COM_BINLOG_DUMP then
        -- if we don't have a backend selected, let's pick the master
        --
        if backend_ndx == 0 or ro_server == true then
            local rw_backend_ndx = lb.idle_failsafe_rw()
            if rw_backend_ndx > 0 then
                backend_ndx = rw_backend_ndx
                proxy.connection.backend_ndx = backend_ndx
            else
                if is_debug then
                    print(' master have no idle connection');
                end
                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "1,master connections are too small"
                }
                return proxy.PROXY_SEND_RESULT
            end
        end

        return
    end

    -- for testsuit we use shortter wait_clt_next_sql, as 100ms, default is 1000
    if testsuit and is_backend_conn_keepalive then
        proxy.connection.wait_clt_next_sql = 100
    end

    -- read/write splitting 
    --
    -- send all non-transactional SELECTs to a slave
    if not is_in_transaction and
        cmd.type == proxy.COM_QUERY then
        if proxy.global.warm_up_pipe_handle and proxy.global.warm_up_pid then
            if proxy.global.warm_up_pid == cmd.query then
                print('setup warm_up_conn to ', proxy.connection,'for query', cmd.query)
                proxy.global.warm_up_conn = proxy.connection
                return return_insert_id('id', 1)
            end
        end

        tokens     = tokens or assert(tokenizer.tokenize(cmd.query))

        local stmt = tokenizer.first_stmt_token(tokens)

        if stmt.token_name == "TK_SQL_SELECT" then
            is_in_select_calc_found_rows = false
            local is_insert_id = false
            local last_insert_id_name = nil

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
                        last_insert_id_name = utext
                    end
                elseif not is_insert_id and token.token_name == "TK_FUNCTION" then
                    local utext = token.text:upper()
                    if utext == "LAST_INSERT_ID" then
                        is_insert_id = true
                        utext = utext ..  "()"
                        last_insert_id_name = utext
                    end
                end

                -- we found the two special token, we can't find more
                if is_insert_id and is_in_select_calc_found_rows then
                    break
                end
            end

            if not is_insert_id then
                if is_backend_conn_keepalive then
                    rw_op = false
                    local ro_backend_ndx = lb.idle_ro()
                    if backend_ndx ~= ro_backend_ndx and ro_backend_ndx > 0 then
                        backend_ndx = ro_backend_ndx
                        proxy.connection.backend_ndx = backend_ndx

                        if is_debug then
                            print("  [use ro server: " .. backend_ndx .. "]")
                        end
                    end
                end
            else
                -- only support last insert id in non transaction environment
                local last_insert_id = proxy.connection.last_insert_id
                return return_insert_id(last_insert_id_name, last_insert_id)
            end

        else 

            -- We assume charset set will happen before transaction
            if stmt.token_name == "TK_SQL_SET" then
                local token_len = #tokens
                if token_len  > 2 then
                    local token = tokens[2]
                    if token.token_name == "TK_LITERAL" then
                        if token.text == "NAMES" then
                            local charset = tokens[3]
                            is_charset_set = true
                            is_charset_client = true
                            is_charset_connection = true
                            is_charset_results = true
                            proxy.connection.client.charset = charset.text
                            charset_client = charset.text
                            charset_connection = charset.text
                            charset_results = charset.text
                        else
                            if token_len > 3 then
                                local nxt_token = tokens[3]
                                if nxt_token.token_name == "TK_EQ" then
                                    local nxt_nxt_token = tokens[4]
                                    if token.text == "character_set_client" then
                                        proxy.connection.client.character_set_client = nxt_nxt_token.text
                                        charset_client = nxt_nxt_token.text
                                        is_charset_client = true
                                    elseif token.text == "character_set_connection" then
                                        proxy.connection.client.character_set_connection = nxt_nxt_token.text
                                        charset_connection = nxt_nxt_token.text
                                        is_charset_connection = true
                                    elseif token.text == "character_set_results" then
                                        proxy.connection.client.character_set_results = nxt_nxt_token.text
                                        charset_results = nxt_nxt_token.text
                                        is_charset_results = true
                                    elseif token.text == "autocommit" then
                                        if nxt_nxt_token.text == "0" then
                                            is_auto_commit = false
                                            if is_debug then
                                                print("  [set is_auto_commit false]" )
                                            end
                                            rw_op = true
                                        else
                                            is_auto_commit = true
                                        end
                                    elseif token.text == "sql_mode" then
                                        if is_debug then
                                            print("   sql mode:" .. nxt_nxt_token.text)
                                        end
                                        proxy.connection.client.sql_mode = nxt_nxt_token.text
                                        sql_mode_set = true
                                    end
                                end
                            end
                        end
                    end
                end
            end

            if is_backend_conn_keepalive and is_auto_commit and 
                (stmt.token_name == "TK_SQL_USE" or stmt.token_name == "TK_SQL_SET" or
                stmt.token_name == "TK_SQL_SHOW" or stmt.token_name == "TK_SQL_DESC"
                or stmt.token_name == "TK_SQL_EXPLAIN") then
                rw_op = false
                local ro_backend_ndx = lb.idle_ro()
                if backend_ndx ~= ro_backend_ndx and ro_backend_ndx > 0 then
                    backend_ndx = ro_backend_ndx
                    proxy.connection.backend_ndx = backend_ndx

                    if stmt.token_name == "TK_SQL_USE" then
                        local token_len = #tokens
                        if token_len  > 1 then
                            c.default_db = tokens[2].text
                        end
                    end
                    if is_debug then
                        print("  [use ro server: " .. backend_ndx .. "]")
                    end
                end
            end
        end

        if ro_server == true and rw_op == true then
            local rw_backend_ndx = lb.idle_failsafe_rw()
            if rw_backend_ndx > 0 then
                backend_ndx = rw_backend_ndx
                proxy.connection.backend_ndx = backend_ndx
                if is_debug then
                    print("  [use rw server:" .. backend_ndx .."]")
                end
                if ps_cnt > 0 then 
                    multiple_server_mode = true
                    if is_debug then
                        print("  [set multiple_server_mode true in complex env]")
                    end
                end
            else
                if is_debug then
                    print('2, master have no idle connection');
                end
                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "2, master connections are too small"
                }
                return proxy.PROXY_SEND_RESULT
            end
        end

    else

        if is_in_transaction then
            if cmd.type == proxy.COM_QUERY then
                tokens     = tokens or assert(tokenizer.tokenize(cmd.query))
                local stmt = tokenizer.first_stmt_token(tokens)
                if stmt.token_name == "TK_SQL_SET" then
                    local token_len = #tokens
                    if token_len  > 3 then
                        local token = tokens[2]
                        local nxt_token = tokens[4]
                        if token.text == "autocommit" then
                            if nxt_token.text == "1" then
                                is_auto_commit = true
                                if is_debug then
                                    print("  [set is_auto_commit true after trans]" )
                                end
                            end
                        end
                    end
                end
            elseif cmd.type == proxy.COM_STMT_PREPARE then
                is_prepared = true
            end

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

                local session_read_only = 0

                if stmt.token_name == "TK_SQL_SELECT" then
                    session_read_only = 1
                    for i = 2, #tokens do
                        local token = tokens[i]
                        if (token.token_name == "TK_COMMENT") then
                            if is_debug then
                                print("  [check trans for ps]")
                            end
                            local _, _, in_trans = string.find(token.text, "in_trans=%s*(%d)")
                            if in_trans ~= 0 then
                                is_in_transaction = true
                                break
                            end
                        end
                    end

                    if is_backend_conn_keepalive and ps_cnt == 0 and session_read_only == 1 then
                        local ro_backend_ndx = lb.idle_ro()
                        if backend_ndx ~= ro_backend_ndx and ro_backend_ndx > 0 then
                            backend_ndx = ro_backend_ndx
                            proxy.connection.backend_ndx = backend_ndx

                            if is_debug then
                                print("  [use ro server: " .. backend_ndx .. "]")
                            end
                        end
                    end
                end

                if session_read_only == 0 or is_in_transaction == true then
                    if ro_server == true then
                        local rw_backend_ndx = lb.idle_failsafe_rw()
                        if rw_backend_ndx > 0 then
                            multiple_server_mode = true
                            backend_ndx = rw_backend_ndx
                            proxy.connection.backend_ndx = backend_ndx
                            if is_debug then
                                print("  [set multiple_server_mode true]")
                            end
                        else
                            if is_debug then
                                print('3, master have no idle connection');
                            end
                            proxy.response = {
                                type = proxy.MYSQLD_PACKET_ERR,
                                errmsg = "3, master connections are too small"
                            }
                            return proxy.PROXY_SEND_RESULT
                        end
                    end
                end

            elseif ps_cnt > 0 or not is_auto_commit then
                conn_reserved = true
            end
        end
    end

    if backend_ndx == 0 then
        local rw_backend_ndx = lb.idle_failsafe_rw()
        if is_backend_conn_keepalive and rw_backend_ndx <= 0 and proxy.global.config.rwsplit.is_slave_write_forbidden_set then
            local ro_backend_ndx = lb.idle_ro()
            if ro_backend_ndx > 0 then
                backend_ndx = ro_backend_ndx
                proxy.connection.backend_ndx = backend_ndx
            end
        elseif rw_backend_ndx > 0 then
            backend_ndx = rw_backend_ndx
            proxy.connection.backend_ndx = backend_ndx
        end
    end

    if rw_op then
        stats.query_info.rw = stats.query_info.rw + 1
    else
        stats.query_info.ro = stats.query_info.ro + 1
    end

    -- in case the master is down, we have to close the client connections
    -- otherwise we can go on
    if backend_ndx == 0 then
        if is_debug then
            print("    backend_ndx is zero")
        end
        proxy.queries:append(1, packet, { resultset_is_needed = true })
        return proxy.PROXY_SEND_QUERY
    end

    if not stats.backend_details[backend_ndx] then 
        stats.backend_details[backend_ndx] = {
            ro = 0,
            rw = 0,
        }
    end

    if rw_op then
        stats.backend_details[backend_ndx].rw = stats.backend_details[backend_ndx].rw + 1
    else
        stats.backend_details[backend_ndx].ro = stats.backend_details[backend_ndx].ro + 1
    end

    local s = proxy.global.backends[backend_ndx] 
    if s.type == proxy.BACKEND_TYPE_RW then
        stats.backend_info.rw = stats.backend_info.rw + 1
    else
        stats.backend_info.ro = stats.backend_info.ro + 1
    end

    if cmd.type == proxy.COM_STMT_EXECUTE then
        proxy.queries:append(3, packet, { resultset_is_needed = true } )
    elseif cmd.type == proxy.COM_STMT_PREPARE then
        proxy.queries:append(4, packet, { resultset_is_needed = true } )
    else
        proxy.queries:append(1, packet, { resultset_is_needed = true })
        if cmd.type == proxy.COM_STMT_CLOSE and is_in_transaction then
            proxy.connection.is_still_in_trans = true
        end
    end

    -- attension: change stmt id after append the query
    if multiple_server_mode == true then
        if cmd.type == proxy.COM_STMT_EXECUTE or cmd.type == proxy.COM_STMT_CLOSE then
            if is_debug then
                print("    stmt id before change:" .. cmd.stmt_handler_id)
            end
            proxy.connection.change_server_by_stmt_id = cmd.stmt_handler_id
            -- all related fields are invalid after this such as stmt_handler_id
        elseif cmd.type == proxy.COM_QUERY then
            if is_debug then
                print("    change server by backend index")
            end
            proxy.connection.change_server_by_rw = backend_ndx
        end
        conn_reserved = true
    end

    if is_debug then
        print("    cmd type:" .. cmd.type)
        if conn_reserved then
            print(" connection reserved")
        else
            print(" connection not reserved")
        end
    end

    c.is_server_conn_reserved = conn_reserved

    if is_debug then
        backend_ndx = proxy.connection.backend_ndx
        if is_debug then
            print("  backend_ndx:" .. backend_ndx)
        end
    end

    local s = proxy.connection.server
    local sql_mode = proxy.connection.client.sql_mode
    local srv_sql_mode = proxy.connection.server.sql_mode

    if is_debug then
        if sql_mode ~= nil then
            print("  client sql mode:" .. sql_mode)
        else
            print("  client sql mode nil")
        end
        if srv_sql_mode ~= nil then
            print("  server sql mode:" .. srv_sql_mode)
        else
            print("  server sql mode nil")
        end
    end

    if sql_mode == nil then
        sql_mode = ""
    end

    if srv_sql_mode == nil then 
        srv_sql_mode = ""
    end

    local sql_mode_check = true
    if sql_mode == "" and srv_sql_mode == "" then
        sql_mode_check = false
    end

    if sql_mode_check then
        if not sql_mode_set then

            if sql_mode ~= srv_sql_mode then
                if is_debug then
                    print("  change sql mode")
                end

                if sql_mode ~= "" then
                    local modes = tokenizer.tokenize(sql_mode)
                    local num = 9
                    if is_debug then
                        print("   sql mode:" .. sql_mode)
                        print("   sql mode num:" .. #modes)
                    end
                    proxy.queries:prepend(num,
                    string.char(proxy.COM_QUERY) .. "SET sql_mode='" .. modes[1].text .. "'",
                    { resultset_is_needed = true })

                    for i = 2, #modes do
                        num = num + 1
                        proxy.queries:prepend(num,
                        string.char(proxy.COM_QUERY) .. "SET sql_mode='" .. modes[i].text .. "'",
                        { resultset_is_needed = true })
                    end
                else
                    proxy.queries:prepend(9,
                    string.char(proxy.COM_QUERY) .. "SET sql_mode=''",
                    { resultset_is_needed = true })
                end
                proxy.connection.server.server_sql_mode = sql_mode
            end
        else
            if sql_mode ~= srv_sql_mode then
                proxy.connection.server.server_sql_mode = sql_mode
                if is_debug then
                    print("  set server sql mode:" .. sql_mode)
                end
            end
        end
    end

    if not is_charset_set then
        local clt_charset = proxy.connection.client.charset
        local srv_charset = proxy.connection.server.charset

        if is_debug then
            if clt_charset ~= nil then
                print("  client charset:" .. clt_charset)
            end
            if srv_charset ~= nil then
                print("  server charset:" .. srv_charset)
            end
        end


        if clt_charset ~= srv_charset then
            if is_debug then
                print("  change charset")
            end
            if clt_charset ~= nil then
                is_charset_reset = true
                charset_str = clt_charset
            end

            proxy.connection.server.charset = clt_charset
        end
    else
        proxy.connection.server.charset = proxy.connection.client.charset
    end

    if not is_charset_client then
        local clt_charset_client = proxy.connection.client.character_set_client
        local srv_charset_client = proxy.connection.server.character_set_client

        if is_debug then
            if clt_charset_client ~= nil then
                print("  client charset_client:" .. clt_charset_client)
            end
            if srv_charset_client ~= nil then
                print("  server charset_client:" .. srv_charset_client)
            end
        end

        if clt_charset_client ~= srv_charset_client then
            if is_debug then
                print("  change server charset_client")
            end
            if clt_charset_client ~= nil then
                proxy.queries:prepend(5,
                string.char(proxy.COM_QUERY) .. "SET character_set_client = " .. clt_charset_client,
                { resultset_is_needed = true })
            end

            proxy.connection.server.character_set_client = clt_charset_client
        end
    else
        proxy.connection.server.character_set_client = charset_client
    end

    if not is_charset_connection then
        local clt_charset_conn = proxy.connection.client.character_set_connection
        local srv_charset_conn = proxy.connection.server.character_set_connection

        if is_debug then
            if clt_charset_conn ~= nil then
                print("  client charset_connection:" .. clt_charset_conn)
            end
            if srv_charset_conn ~= nil then
                print("  server charset_connection:" .. srv_charset_conn)
            end
        end

        if clt_charset_conn ~= srv_charset_conn then
            if is_debug then
                print("  change server charset conn:")
            end
            if clt_charset_conn ~= nil then
                proxy.queries:prepend(6,
                string.char(proxy.COM_QUERY) .. "SET character_set_connection = " .. clt_charset_conn,
                { resultset_is_needed = true })
            end
            proxy.connection.server.character_set_connection = clt_charset_conn
        end
    else
        proxy.connection.server.character_set_connection = charset_connection
    end

    if not is_charset_results then
        local clt_charset_results = proxy.connection.client.character_set_results
        local srv_charset_results = proxy.connection.server.character_set_results

        if is_debug then
            if clt_charset_results ~= nil then
                print("  client charset_results:" .. clt_charset_results)
            end
            if srv_charset_results ~= nil then
                print("  server charset_results:" .. srv_charset_results)
            end
        end

        if clt_charset_results ~= srv_charset_results then
            if is_debug then
                print("  change server charset results")
            end
            if clt_charset_results == nil then
                proxy.queries:prepend(7,
                string.char(proxy.COM_QUERY) .. "SET character_set_results = NULL",
                { resultset_is_needed = true })
            else
                proxy.queries:prepend(7,
                string.char(proxy.COM_QUERY) .. "SET character_set_results = " .. clt_charset_results,
                { resultset_is_needed = true })
            end
            proxy.connection.server.character_set_results = clt_charset_results
        end
    else
        proxy.connection.server.character_set_results = charset_results
    end

    if is_charset_reset then
        proxy.queries:prepend(8,
        string.char(proxy.COM_QUERY) .. "SET NAMES " .. charset_str,
        { resultset_is_needed = true })
    end

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
        if backend_ndx > 0 then
            local b = proxy.global.backends[backend_ndx]
            print("  sending to backend : " .. b.dst.name)
            print("    is_slave         : " .. tostring(b.type == proxy.BACKEND_TYPE_RO))
            print("    server default db: " .. s.default_db)
            print("    server username  : " .. s.username)
        end
        print("    in_trans        : " .. tostring(is_in_transaction))
        print("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
        print("    COM_QUERY       : " .. tostring(cmd.type == proxy.COM_QUERY))
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

    if is_debug then
        local backend_ndx = proxy.connection.backend_ndx
        print("[read_query_result] " .. proxy.connection.client.src.name)
        print("   backend_ndx:" .. backend_ndx)
        print("   read from server:" .. proxy.connection.server.dst.name)
        print("   proxy used port:" .. proxy.connection.server.src.name)
        print("   read index from server:" .. proxy.connection.backend_ndx)
        print("   inj id:" .. inj.id)
        print("   res status:" .. res.query_status)
    end

    if not is_backend_conn_keepalive then
        proxy.connection.to_be_closed_after_serve_req = true
    end

    if inj.id ~= 1 and inj.id ~= 3 and inj.id ~= 4 then
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

    if res.query_status then
        if res.query_status ~= 0 then
            if is_debug and is_in_transaction then
                print("   query_status: " .. res.query_status .. " error, reserve origin is_in_transaction value")
            end
        elseif inj.id ~= 4 then
            is_in_transaction = flags.in_trans
            if not is_in_transaction then
                if not is_auto_commit then
                    is_in_transaction = true
                    if is_debug then
                        print("   set is_in_transaction true")
                    end
                else
                    if not is_prepared then
                        proxy.connection.client.is_server_conn_reserved = false
                    end
                end
            end
        end
    end

    if multiple_server_mode == true then
        if is_debug then
            print("   multiple_server_mode true")
        end
        if inj.id == 4 then
            local server_index = proxy.connection.selected_server_ndx
            if is_debug then
                print("   multiple_server_mode, server index:" .. server_index)
                print("    change stmt id")
            end
            res.prepared_stmt_id = server_index
        end
    end
end

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
    local is_debug = proxy.global.config.rwsplit.is_debug
    if is_debug then
        print("[disconnect_client] " .. proxy.connection.client.src.name)
    end

    proxy.global.stat_clients = proxy.global.stat_clients - 1

    if is_debug then print("[stat clients]: " .. proxy.global.stat_clients); end
    if proxy.connection.client_abnormal_close == true then
        proxy.connection.connection_close = true
    else
        if not is_backend_conn_keepalive or is_in_transaction or not is_auto_commit then 
            if is_debug then print("  set connection_close true"); end
            if is_in_transaction then
                if is_debug then print(" is_in_transaction is still true"); end 
            end
            proxy.connection.connection_close = true
        else
            -- make sure we are disconnection from the connection
            -- to move the connection into the pool
            proxy.connection.backend_ndx = 0
        end

    end

end

