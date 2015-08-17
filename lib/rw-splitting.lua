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
local utils = require("shard.utils")
local queryAnalyzer = require("shard.queryAnalyzer")

-- Load configuration.
local config = require("shard.config")
local tableKeyColumns = config.getAllTableKeyColumns()
local shardingLookup = require("shard.shardingLookup")
local proxy = proxy
local tokens

shardingLookup.init(config)

-- Statistics
local stats = require("shard.stats")

-- Admin module
local admin = require("shard.admin")
admin.init(config)


-- Local variable to hold result for multiple queries
local _combinedResultSet 
local _combinedNumberOfQueries
local _total_queries_per_req 
local _combinedLimit 
local _query


--- config
--
-- connection pool
if not proxy.global.config.rwsplit then
    proxy.global.config.rwsplit = {
        min_idle_connections = 1,
        mid_idle_connections = 4,
        max_idle_connections = 8,
        max_init_time = 10,
        default_user = "",
        default_index = 1,
        is_warn_up = false,
        is_debug = true,
        is_slave_write_forbidden_set = false
    }
end

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_passed_but_req_rejected = false
local is_in_transaction       = false
local is_auto_commit          = true
local is_prepared             = false
local is_backend_conn_keepalive = true
local use_pool_conn = false
local multiple_server_mode = false
local last_group = nil

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

function warm_up()
    --local is_debug = proxy.global.config.rwsplit.is_debug
    if is_debug then
        print("[connect_server] " .. proxy.connection.client.src.name)
    end

    if not proxy.global.stat_clients then
        proxy.global.stat_clients = 0
    end
    proxy.global.stat_clients = proxy.global.stat_clients + 1
    
    local index    = proxy.global.config.rwsplit.default_index

    -- init one backend 
    if index <= #proxy.global.backends then
        local user     = proxy.global.config.rwsplit.default_user
        local s        = proxy.global.backends[index]
        local pool     = s.pool 
        local cur_idle = pool.users[user].cur_idle_connections
        local min_idle_conns
        local max_idle_conns
        local connected_clients = s.connected_clients

        min_idle_conns = proxy.global.config.rwsplit.min_idle_connections
        max_idle_conns = proxy.global.config.rwsplit.max_idle_connections

        if is_debug then
            print("  [".. index .."].user = " .. user)
            print("  [".. index .."].connected_clients = " .. connected_clients)
            print("  [".. index .."].pool.cur_idle     = " .. cur_idle)
            print("  [".. index .."].pool.max_idle     = " .. max_idle_conns)
            print("  [".. index .."].pool.min_idle     = " .. min_idle_conns)
            print("  [".. index .."].type = " .. s.type)
            print("  [".. index .."].state = " .. s.state)
        end

        -- prefer connections to the master 
        if (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) then
            proxy.connection.backend_ndx = index
        else
            proxy.response = {
                type = proxy.MYSQLD_PACKET_ERR,
                errmsg = "01,proxy rejects create backend connections"
            }
            return proxy.PROXY_SEND_RESULT
        end
    end

end

--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server() 
    --local is_debug = proxy.global.config.rwsplit.is_debug
    -- make sure that we connect to each backend at least ones to 
    -- keep the connections to the servers alive
    --
    -- on read_query we can switch the backends again to another backend

    if is_debug then
        print("[connect_server] " .. proxy.connection.client.src.name)
    end

    if proxy.global.config.rwsplit.is_warn_up then
        return warm_up()
    end

    if not proxy.global.stat_clients then
        proxy.global.stat_clients = 0
    end
    proxy.global.stat_clients = proxy.global.stat_clients + 1

    local rw_ndx = 0
    local max_idle_conns = 0
    local mid_idle_conns = 0
    local min_idle_conns = 0
    local init_phase = false
    local cur_idle = 0
    local connected_clients = 0
    local total_clients = 0
    local total_available_conns = 0

    total_clients = proxy.global.stat_clients + 1

    -- init all backends 
    for i = 1, #proxy.global.backends do
        local s        = proxy.global.backends[i]
        local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
        cur_idle = pool.users[""].cur_idle_connections
        init_phase = pool.init_phase
        connected_clients = s.connected_clients

        if connected_clients > 0 then
            pool.serve_req_after_init = true
        else
            pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
            pool.mid_idle_connections = proxy.global.config.rwsplit.mid_idle_connections
            pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections
            pool.max_init_time = proxy.global.config.rwsplit.max_init_time
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
                errmsg = "001,proxy stops serving requests now"
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
            ((cur_idle < min_idle_conns and connected_clients < max_idle_conns)
            or cur_idle > 0) then
            proxy.connection.backend_ndx = i
            break
        elseif s.type == proxy.BACKEND_TYPE_RO and
            (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) and
            ((cur_idle < min_idle_conns and connected_clients < max_idle_conns)
            or cur_idle > 0) then
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
                        errmsg = "002,proxy stops serving requests now"
                    }
                    return proxy.PROXY_SEND_RESULT
                else
                    is_backend_conn_keepalive = false
                end
            end
            rw_ndx = i
        end
    end

    -- fuzzy check which is not accurate
    if init_phase and cur_idle <= min_idle_conns and total_available_conns < total_clients then
        is_passed_but_req_rejected = true
        if is_debug then
            print("  total available conn:" .. total_available_conns .. ",total clients:" .. total_clients)
            print("  is_passed_but_req_rejected set true")
        end
    else
        is_passed_but_req_rejected = false
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

    if cur_idle > 0 and proxy.connection.server then 
        --if is_debug then
        --	print("  using pooled connection from: " .. proxy.connection.backend_ndx)
        --end
        local backend_state = proxy.global.backends[proxy.connection.backend_ndx].state
        if backend_state == proxy.BACKEND_STATE_UP then
            use_pool_conn = true
            -- stay with it
            return proxy.PROXY_IGNORE_RESULT
        end
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
    local is_debug = proxy.global.config.rwsplit.is_debug

    if is_debug then
        print("[read_auth_result] " .. proxy.connection.client.src.name)
        print("  using connection from: " .. proxy.connection.backend_ndx)
        print("  server address: " .. proxy.connection.server.dst.name)
        print("  server charset: " .. proxy.connection.server.character_set_client)
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
        print("  (read_auth_result) ... auth failed")
    end
end

local get_sharding_group
local dispose_one_query
local _buildUpCombinedResultSet
local _getFields

function read_query( packet )
    local groups = {}
    _combinedResultSet = {}
    _combinedResultSet.fields = nil 
    _combinedResultSet.rows = nil 
    _combinedResultSet.affected_rows = 0 
    _combinedNumberOfQueries = 0 
    _total_queries_per_req = 0
    _combinedLimit = {}
    _combinedLimit.rowsProcessed = 0 
    _combinedLimit.rowsSent = 0 
    _combinedLimit.from = 0 
    _combinedLimit.rows = 0 

    get_sharding_group(packet, groups)
    local shard_num = #groups
    for _, group in pairs(groups) do
        if shard_num > 1 then
            proxy.connection.shard_num = shard_num
            _combinedNumberOfQueries = _combinedNumberOfQueries + 1
        end
        local result = dispose_one_query(packet, group)
        if result == proxy.PROXY_SEND_RESULT then
            return proxy.PROXY_SEND_RESULT
        end
    end

    return proxy.PROXY_SEND_QUERY
end


function get_sharding_group(packet, groups)
    _query = packet:sub(2)

    if packet:byte() == proxy.COM_QUERY then
        tokens = tokenizer.tokenize(_query)
        local _queryAnalyzer = queryAnalyzer.QueryAnalyzer.create(tokens, tableKeyColumns)
        local success, errorMessage = pcall(_queryAnalyzer.analyze, _queryAnalyzer)
        if (success) then
            if (_queryAnalyzer:isPartitioningNeeded()) then
                if (_queryAnalyzer:isFullPartitionScanNeeded()) then
                    utils.debug("--- full scan '" .. _query .. "'")
                    local tableName = _queryAnalyzer:getAffectedTables()[1]
                    stats.incFullPartitionScans(tableName)
                    _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                    shardingLookup.getAllShardingGroups(tableName, groups)
                else
                    utils.debug("--- not full scan '" .. _query .. "'")
                    if _queryAnalyzer:getShardType() == 0 then
                        for tableName, key in pairs(_queryAnalyzer:getTableKeyValues()) do
                            local group = shardingLookup.getShardingGroupByHash(tableName, key)
                            print("group chosen:" .. group)
                            table.insert(groups, group)
                        end
                    else
                        utils.debug("--- range sharding '" .. _query .. "'")
                        _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                        for tableName, range in pairs(_queryAnalyzer:getTableKeyRange()) do
                            shardingLookup.getShardingGroupByRange(tableName, range, groups)
                        end
                    end
                end
            end
        end
    end

    if #groups == 0 then
        table.insert(groups, "default")
    end
end


--- 
-- read/write splitting
function dispose_one_query( packet, group )
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
    local base = _total_queries_per_req

    if is_prepared then
        ps_cnt = proxy.connection.valid_parallel_stmt_cnt
    end

    if backend_ndx > 0 then
        local b = proxy.global.backends[backend_ndx]
        if b.type == proxy.BACKEND_TYPE_RO then
            ro_server = true
        end

        if b.state ~= proxy.BACKEND_STATE_UP then
            if ro_server == true then
                local rw_backend_ndx = lb.idle_failsafe_rw(group)
                if rw_backend_ndx > 0 then
                    backend_ndx = rw_backend_ndx
                    proxy.connection.backend_ndx = backend_ndx
                else
                    if is_debug then
                        print("  [no rw connections yet")
                    end
                    proxy.response = {
                        type = proxy.MYSQLD_PACKET_ERR,
                        errmsg = "003,master connections are too small"
                    }
                    return proxy.PROXY_SEND_RESULT
                end
            else
                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "004,proxy stops serving requests now"
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
                errmsg = "005,proxy stops serving requests now"
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
        print("  cmd type = " .. cmd.type)
        if cmd.type == proxy.COM_QUERY then 
            print("  query             = "        .. cmd.query)
        end
    end

    if cmd.type == proxy.COM_QUIT then
        if backend_ndx <= 0 or (is_backend_conn_keepalive and not is_in_transaction) then
            -- don't send COM_QUIT to the backend. We manage the connection
            -- in all aspects.
            -- proxy.response = {
            --	type = proxy.MYSQLD_PACKET_OK,
            -- }

            if is_debug then
                print("  valid_parallel_stmt_cnt:" .. ps_cnt)
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
        if backend_ndx == 0 then
            local rw_backend_ndx = lb.idle_failsafe_rw(group)
            if rw_backend_ndx > 0 then
                backend_ndx = rw_backend_ndx
                proxy.connection.backend_ndx = backend_ndx
            else
                if is_debug then
                    print("  [no rw connections yet")
                end
                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "006,master connections are too small"
                }
                return proxy.PROXY_SEND_RESULT
            end
        end

        return
    end

    -- read/write splitting 
    --
    -- send all non-transactional SELECTs to a slave
    if not is_in_transaction and
        cmd.type == proxy.COM_QUERY then
        tokens = tokens or assert(tokenizer.tokenize(cmd.query))

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
                rw_op = false
                local ro_backend_ndx = lb.idle_ro(group)
                if ro_backend_ndx > 0 then
                    backend_ndx = ro_backend_ndx
                    proxy.connection.backend_ndx = backend_ndx

                    if is_debug then
                        print("  [use ro server: " .. backend_ndx .. "]")
                    end
                end

            else
                conn_reserved = true
                if is_debug then
                    print("  [this select statement should use the same connection] ")
                end
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
                                            if ro_server == true then
                                                local rw_backend_ndx = lb.idle_failsafe_rw(group)
                                                if rw_backend_ndx > 0 then
                                                    backend_ndx = rw_backend_ndx
                                                    proxy.connection.backend_ndx = backend_ndx
                                                end
                                            end
                                            conn_reserved = true
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

            if stmt.token_name == "TK_SQL_USE" or stmt.token_name == "TK_SQL_SET" or
                stmt.token_name == "TK_SQL_SHOW" or stmt.token_name == "TK_SQL_DESC"
                or stmt.token_name == "TK_SQL_EXPLAIN" then
                rw_op = false
                local ro_backend_ndx = lb.idle_ro(group)
                if ro_backend_ndx > 0 then
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
            local rw_backend_ndx = lb.idle_failsafe_rw(group)
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
                    print("  [no rw connections yet")
                end
                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "007, master connections are too small"
                }
                return proxy.PROXY_SEND_RESULT
            end
        end

    else

        if is_in_transaction then
            if cmd.type == proxy.COM_QUERY then
                tokens = tokens or assert(tokenizer.tokenize(cmd.query))
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
                tokens = tokens or assert(tokenizer.tokenize(cmd.query))
                local stmt = tokenizer.first_stmt_token(tokens)

                for i = 1, #tokens do
                    local token = tokens[i]
                    print("token: " .. token.token_name)
                    print("  val: " .. token.text)
                end

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

                    if ps_cnt == 0 and session_read_only == 1 then
                        local ro_backend_ndx = lb.idle_ro(group)
                        if ro_backend_ndx > 0 then
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
                        local rw_backend_ndx = lb.idle_failsafe_rw(group)
                        if rw_backend_ndx > 0 then
                            multiple_server_mode = true
                            backend_ndx = rw_backend_ndx
                            proxy.connection.backend_ndx = backend_ndx
                            if is_debug then
                                print("  [set multiple_server_mode true]")
                            end
                        else
                            if is_debug then
                                print("  [no rw connections yet")
                            end
                            proxy.response = {
                                type = proxy.MYSQLD_PACKET_ERR,
                                errmsg = "008, master connections are too small"
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

    print("   check for server for sharding, back index:" .. backend_ndx)
    if backend_ndx > 0 then
        local b = proxy.global.backends[backend_ndx]
        if b.group ~= group then
            backend_ndx = lb.choose_rw_backend_ndx(group)
            print("    switch to backend ndx:" .. backend_ndx)
            proxy.connection.change_server = backend_ndx
            print("    now backend ndx:" .. proxy.connection.backend_ndx)
            backend_ndx = 0
            multiple_server_mode = true
            if b.group ~= nil then
                print("   origin group:" .. b.group)
            end
        end
    end

    if backend_ndx == 0 then
        local rw_backend_ndx = lb.idle_failsafe_rw(group)
        if rw_backend_ndx <= 0 and proxy.global.config.rwsplit.is_slave_write_forbidden_set then
            local ro_backend_ndx = lb.idle_ro(group)
            if ro_backend_ndx > 0 then
                backend_ndx = ro_backend_ndx
                proxy.connection.backend_ndx = backend_ndx
            end
        elseif rw_backend_ndx > 0 then
            backend_ndx = rw_backend_ndx
            proxy.connection.backend_ndx = backend_ndx
        end
    end

    -- by now we should have a backend
    --
    -- in case the master is down, we have to close the client connections
    -- otherwise we can go on
    if backend_ndx == 0 then
        proxy.response = {
            type = proxy.MYSQLD_PACKET_ERR,
            errmsg = "009,connections are not enough"
        }
        return proxy.PROXY_SEND_RESULT
    end

    proxy.queries:append_after_nth(cmd.type, base, packet, { resultset_is_needed = true } )
    _total_queries_per_req = _total_queries_per_req + 1
    if cmd.type == proxy.COM_STMT_CLOSE and is_in_transaction then
        proxy.connection.is_still_in_trans = true
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
                print("    change server by backend index:" .. backend_ndx)
            end
            proxy.connection.change_server = backend_ndx
        end
        conn_reserved = true
    elseif proxy.connection.shard_num > 1 then
        conn_reserved = true
    end

    if is_debug then
        print("    cmd type:" .. cmd.type)
        if conn_reserved then
            print("    connection reserved")
        else
            print("    connection not reserved")
        end
    end

    c.is_server_conn_reserved = conn_reserved

    if is_debug then
        backend_ndx = proxy.connection.backend_ndx
        if is_debug then
            print("  backend_ndx:" .. backend_ndx)
        end
    end

    if is_passed_but_req_rejected then
        proxy.response = {
            type = proxy.MYSQLD_PACKET_ERR,
            errmsg = "010,too many connections"
        }
        return proxy.PROXY_SEND_RESULT
    end

    local s = proxy.connection.server
    local sql_mode = proxy.connection.client.sql_mode
    local srv_sql_mode = proxy.connection.server.sql_mode

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
                    if is_debug then
                        print("   sql mode:" .. sql_mode)
                        print("   sql mode num:" .. #modes)
                    end
                    proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
                    string.char(proxy.COM_QUERY) .. "SET sql_mode='" .. modes[1].text .. "'",
                    { resultset_is_needed = true })
                    _total_queries_per_req = _total_queries_per_req + 1

                    for i = 2, #modes do
                        proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
                        string.char(proxy.COM_QUERY) .. "SET sql_mode='" .. modes[i].text .. "'",
                        { resultset_is_needed = true })
                        _total_queries_per_req = _total_queries_per_req + 1
                    end
                else
                    proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base, 
                    string.char(proxy.COM_QUERY) .. "SET sql_mode=''",
                    { resultset_is_needed = true })
                    _total_queries_per_req = _total_queries_per_req + 1
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

        if clt_charset_client ~= srv_charset_client then
            if is_debug then
                print("  change server charset_client")
            end
            if clt_charset_client ~= nil then
                proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
                string.char(proxy.COM_QUERY) .. "SET character_set_client = " .. clt_charset_client,
                { resultset_is_needed = true })
                _total_queries_per_req = _total_queries_per_req + 1
            end

            proxy.connection.server.character_set_client = clt_charset_client
        end
    else
        proxy.connection.server.character_set_client = charset_client
    end

    if not is_charset_connection then
        local clt_charset_conn = proxy.connection.client.character_set_connection
        local srv_charset_conn = proxy.connection.server.character_set_connection

        if clt_charset_conn ~= srv_charset_conn then
            if is_debug then
                print("  change server charset conn:")
            end
            if clt_charset_conn ~= nil then
                proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
                string.char(proxy.COM_QUERY) .. "SET character_set_connection = " .. clt_charset_conn,
                { resultset_is_needed = true })
                _total_queries_per_req = _total_queries_per_req + 1
            end
            proxy.connection.server.character_set_connection = clt_charset_conn
        end
    else
        proxy.connection.server.character_set_connection = charset_connection
    end

    if not is_charset_results then
        local clt_charset_results = proxy.connection.client.character_set_results
        local srv_charset_results = proxy.connection.server.character_set_results

        if clt_charset_results ~= srv_charset_results then
            if is_debug then
                print("  change server charset results")
            end
            if clt_charset_results == nil then
                proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
                string.char(proxy.COM_QUERY) .. "SET character_set_results = NULL",
                { resultset_is_needed = true })
                _total_queries_per_req = _total_queries_per_req + 1
            else
                proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
                string.char(proxy.COM_QUERY) .. "SET character_set_results = " .. clt_charset_results,
                { resultset_is_needed = true })
                _total_queries_per_req = _total_queries_per_req + 1
            end
            proxy.connection.server.character_set_results = clt_charset_results
        end
    else
        proxy.connection.server.character_set_results = charset_results
    end

    if is_charset_reset then
        proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
        string.char(proxy.COM_QUERY) .. "SET NAMES " .. charset_str,
        { resultset_is_needed = true })
        _total_queries_per_req = _total_queries_per_req + 1
    end

    -- if client and server db don't match, adjust the server-side 
    --
    -- skip it if we send a INIT_DB anyway
    if cmd.type ~= proxy.COM_INIT_DB and 
        c.default_db and c.default_db ~= s.default_db then
        print("    server default db: " .. s.default_db)
        print("    client default db: " .. c.default_db)
        print("    syncronizing")
        proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
         string.char(proxy.COM_INIT_DB) .. c.default_db, { resultset_is_needed = true })
        _total_queries_per_req = _total_queries_per_req + 1
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

    if (proxy.connection.shard_num > 1) then
        proxy.connection.shard_server = base
    end
    return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj ) 
    --local is_debug = proxy.global.config.rwsplit.is_debug
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
        print("   inj query:" .. _query)
        print("   inj query:" .. inj.query)
        print("   init db cmd:" .. proxy.COM_INIT_DB)
        print("   res status:" .. res.query_status)
    end

    
    if not is_backend_conn_keepalive then
        proxy.connection.to_be_closed_after_serve_req = true
    end

    if inj.id == proxy.PROXY_IGNORE_RESULT then
        -- ignore the result of the USE <default_db>
        -- the DB might not exist on the backend, what do do ?
        --
        -- the injected INIT_DB failed as the slave doesn't have this DB
        -- or doesn't have permissions to read from it
        if res.query_status == proxy.MYSQLD_PACKET_ERR then
            proxy.queries:reset()

            proxy.response = {
                type = proxy.MYSQLD_PACKET_ERR,
                errmsg = "011,can't switch server ".. proxy.connection.client.default_db
            }

            return proxy.PROXY_SEND_RESULT
        end
        return proxy.PROXY_IGNORE_RESULT
    end

    local success, result = pcall(_buildUpCombinedResultSet, inj)
    if (not success) then
        stats.inc("invalidResults")
        proxy.response = {
            type     = proxy.MYSQLD_PACKET_ERR,
            errmsg   = "012: Error: " .. result .. " Query: '" .. _query .. "'"
        }
        print("error result")
        _combinedNumberOfQueries = 0
        return proxy.PROXY_SEND_RESULT
    end

    if res.query_status then
        if res.query_status ~= 0 then
            if is_debug and is_in_transaction then
                print("   query_status: " .. res.query_status .. " error, reserve origin is_in_transaction value")
            end
        elseif inj.id ~= proxy.COM_STMT_PREPARE then
            is_in_transaction = flags.in_trans
            if not is_in_transaction then
                if not is_auto_commit then
                    is_in_transaction = true
                    if is_debug then
                        print("   set is_in_transaction true")
                    end
                else
                    if not is_prepared and _combinedNumberOfQueries == 0 then
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
        if inj.id == proxy.COM_STMT_PREPARE then
            local server_index = proxy.connection.selected_server_ndx
            if is_debug then
                print("   multiple_server_mode, server index:" .. server_index)
                print("    change stmt id")
            end
            res.prepared_stmt_id = server_index
        end
    end

    return result
end

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
    --local is_debug = proxy.global.config.rwsplit.is_debug
    if is_debug then
        print("[disconnect_client] " .. proxy.connection.client.src.name)
    end

    proxy.global.stat_clients = proxy.global.stat_clients - 1

    if not is_backend_conn_keepalive or is_in_transaction or not is_auto_commit then 
        if is_debug then
            print("  set connection_close true ")
            if is_in_transaction then
                print(" is_in_transaction is still true") 
            end
        end
        proxy.connection.connection_close = true
    else
        -- make sure we are disconnection from the connection
        -- to move the connection into the pool
        proxy.connection.backend_ndx = 0
    end

end

-- Extract the "fields" part out of the result set.
-- @return nil if there is no field set
function _getFields(resultSet)
    local newFields = nil
    local fieldCount = 1
    local fields = resultSet.fields
    if (fields) then
        newFields = {}
        while fields[fieldCount] do
            table.insert(
                newFields,
                {
                    type = fields[fieldCount].type,
                    name = fields[fieldCount].name
                }
            )

            fieldCount = fieldCount + 1
        end
    end
    return newFields
end

-- Aggregate the different result sets.
function _buildUpCombinedResultSet(inj)
    utils.debug("_combinedNumberOfQueries:'" .. _combinedNumberOfQueries .. "'", 1)
    if (_combinedNumberOfQueries > 0) then
        local resultSet = assert(inj.resultset, "Something went terribly wrong, got NULL result set.")
        if (resultSet.fields) then
            assert(#(resultSet.fields) > 0, "Something went terribly wrong, got zero length fields.")
            -- We have a result set
            if (not _combinedResultSet.fields) then
                -- Build up the fields part
                _combinedResultSet.rows = {}
                _combinedResultSet.fields = _getFields(resultSet)
            end
            -- Add result respecting LIMIT constraints
            if (resultSet.rows) then
                for row in resultSet.rows do
                    if (
                        (_combinedLimit.rows < 0 or _combinedLimit.rowsSent < _combinedLimit.rows)
                        and
                        (_combinedLimit.from < 0 or _combinedLimit.rowsProcessed >= _combinedLimit.from)
                    ) then
                        table.insert(_combinedResultSet.rows, row)
                        _combinedLimit.rowsSent = _combinedLimit.rowsSent + 1
                    end
                    _combinedLimit.rowsProcessed = _combinedLimit.rowsProcessed + 1
                end
                -- Shortcut - if the LIMIT clause has been fullfilled - don't send any further queries.
                if (_combinedLimit.rows > 0 and _combinedLimit.rowsSent >= _combinedLimit.rows) then
                    proxy.queries:reset()
                end
            end
        else
            utils.debug("resultSet.fields is null", 1)
        end
        if (resultSet.affected_rows) then
            _combinedResultSet.affected_rows = _combinedResultSet.affected_rows + 
                                                tonumber(resultSet.affected_rows)
        end

        if (resultSet.query_status and (resultSet.query_status < 0)) then
            proxy.queries:reset()
        end

        utils.debug("_combinedLimit.rows:" .. _combinedLimit.rows)
        utils.debug("affected_rows:" .. _combinedResultSet.affected_rows)
        utils.debug("inj id " .. inj.id)
        _combinedNumberOfQueries = _combinedNumberOfQueries - 1
        if (_combinedNumberOfQueries == 0) then
            -- This has been the last result set - send all back to client
            if (_combinedResultSet.fields) then
                proxy.response.type = proxy.MYSQLD_PACKET_OK
                proxy.response.resultset = _combinedResultSet
            else
                proxy.response.type = proxy.MYSQLD_PACKET_RAW;
                proxy.response.packets = {
                    "\000" .. -- fields
                    string.char(_combinedResultSet.affected_rows) ..
                    "\000" .. -- insert_id
                    inj.resultset.raw:sub(4)
                }
            end
            multiple_server_mode = false
            tokens = nil
            return proxy.PROXY_SEND_RESULT
        end
        -- Ignore all result sets until we are at the last one
        return proxy.PROXY_IGNORE_RESULT
    end
end

