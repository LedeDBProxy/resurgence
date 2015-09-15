local proto       = require("mysql.proto")
local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local utils       = require("shard.utils")
local auto_config = require("proxy.auto-config")
local proxy = proxy
local tokens

--- config
--
-- connection pool
if not proxy.global.config.rwsplit then
    proxy.global.config.rwsplit = {
        min_idle_connections = 10,
        mid_idle_connections = 40,
        max_idle_connections = 80,
        max_init_time = 1,
        default_user = "",
        default_index = 1,
        master_slave_ratio = 2,
        is_debug = true,
        is_warm_up = false,
        is_sharding_mode = true,
        is_conn_reset_supported = false,
        is_slave_write_forbidden_set = false
    }
end

if proxy.global.config.rwsplit.is_debug == true then
    proxy.global.config.rwsplit.max_init_time = 1
end

local queryAnalyzer
local config
local tableKeyColumns 
local shardingLookup
local stats
local admin

-- Local variable to hold result for multiple queries
local _combinedResultSet 
local _combinedNumberOfQueries
local _combinedLimit 

if proxy.global.config.rwsplit.is_sharding_mode then
    queryAnalyzer = require("shard.queryAnalyzer")
    config = require("shard.config")
    shardingLookup = require("shard.shardingLookup")
    stats = require("shard.stats")
    admin = require("shard.admin")
    tableKeyColumns = config.getAllTableKeyColumns()
    shardingLookup.init(config)
    admin.init(config)
end

local _total_queries_per_req 
local _query

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
local last_group = nil

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

function return_insert_id(utext, last_insert_id)
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



function session_err(msg, enlarge)
    if enlarge then
        value = proxy.global.config.rwsplit.min_idle_connections
        value = value + 1
        proxy.global.config.rwsplit.min_idle_connections = value
    end
    proxy.response = {
        type = proxy.MYSQLD_PACKET_ERR,
        errmsg = msg
    }
    return proxy.PROXY_SEND_RESULT
end

function warm_up()
    utils.debug("[connect_server] " .. proxy.connection.client.src.name)

    if not proxy.global.stat_clients then
        proxy.global.stat_clients = { }
    end

    local index    = proxy.global.config.rwsplit.default_index

    if not proxy.global.stat_clients[index] then
        proxy.global.stat_clients[index] = 0
    end
    
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
        if s.type ~= proxy.BACKEND_TYPE_RW then
            min_idle_conns = min_idle_conns / proxy.global.config.rwsplit.master_slave_ratio
            max_idle_conns = max_idle_conns / proxy.global.config.rwsplit.master_slave_ratio
        end

        utils.debug("[".. index .."].user = " .. user, 1)
        utils.debug("[".. index .."].connected_clients = " .. connected_clients, 1)
        utils.debug("[".. index .."].pool.cur_idle     = " .. cur_idle, 1)
        utils.debug("[".. index .."].pool.max_idle     = " .. max_idle_conns, 1)
        utils.debug("[".. index .."].pool.min_idle     = " .. min_idle_conns, 1)
        utils.debug("[".. index .."].type = " .. s.type, 1)
        utils.debug("[".. index .."].state = " .. s.state, 1)

        if (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) then
            proxy.connection.backend_ndx = index
            proxy.global.stat_clients[index] = proxy.global.stat_clients[index] + 1
        else
            return session_err("01,proxy rejects create backend connections", 0)
        end
    end

end

--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server() 
    -- make sure that we connect to each backend at least ones to 
    -- keep the connections to the servers alive
    --
    -- on read_query we can switch the backends again to another backend

    utils.debug("[connect_server] " .. proxy.connection.client.src.name)

    if proxy.global.config.rwsplit.is_warm_up then
        return warm_up()
    end

    if not proxy.global.stat_clients then
        proxy.global.stat_clients = { }
    end

    local rw_ndx = 0
    local max_idle_conns = 0
    local mid_idle_conns = 0
    local min_idle_conns = 0
    local cur_idle = 0
    local connected_clients = 0
    local total_clients = 1
    local total_available_conns = 0


    -- init all backends 
    for i = 1, #proxy.global.backends do
        if not proxy.global.stat_clients[i] then
            proxy.global.stat_clients[i] = 0
        end

        local s        = proxy.global.backends[i]
        local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
        local init_phase = false
        cur_idle = pool.users[""].cur_idle_connections
        if proxy.global.config.rwsplit.is_debug ~= true then
            init_phase = pool.init_phase
         end
        connected_clients = s.connected_clients

        total_clients = total_clients + proxy.global.stat_clients[i] 
        if connected_clients > 0 then
            pool.serve_req_after_init = true
        else
            if s.type ~= proxy.BACKEND_TYPE_RW then
                local ratio = proxy.global.config.rwsplit.master_slave_ratio
                pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections / ratio
                pool.mid_idle_connections = proxy.global.config.rwsplit.mid_idle_connections / ratio
                pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections / ratio
            else
                pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
                pool.mid_idle_connections = proxy.global.config.rwsplit.mid_idle_connections
                pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections
            end
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
            utils.debug("connection will be rejected", 1)
            return session_err("001,proxy stops serving requests now", 0)
        end

        if s.type ~= proxy.BACKEND_TYPE_RW then
            local ratio = proxy.global.config.rwsplit.master_slave_ratio
            min_idle_conns = min_idle_conns / ratio
            mid_idle_conns = mid_idle_conns / ratio
            max_idle_conns = max_idle_conns / ratio
        end

        utils.debug("[".. i .."].connected_clients = " .. connected_clients, 1)
        utils.debug("[".. i .."].pool.cur_idle     = " .. cur_idle, 1)
        utils.debug("[".. i .."].pool.max_idle     = " .. max_idle_conns, 1)
        utils.debug("[".. i .."].pool.mid_idle     = " .. mid_idle_conns, 1)
        utils.debug("[".. i .."].pool.min_idle     = " .. min_idle_conns, 1)
        utils.debug("[".. i .."].type = " .. s.type, 1)
        utils.debug("[".. i .."].state = " .. s.state, 1)

        -- prefer connections to the master 
        if s.type == proxy.BACKEND_TYPE_RW and
            (s.state == proxy.BACKEND_STATE_UP or
            s.state == proxy.BACKEND_STATE_UNKNOWN) and
            (cur_idle < min_idle_conns and (connected_clients + cur_idle) < max_idle_conns) then
            proxy.connection.backend_ndx = i
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
                    utils.debug("connection will be rejected because init phase", 1)
                    return session_err("002,proxy stops serving requests now", 0)
                else
                    is_backend_conn_keepalive = false
                end
            end
            rw_ndx = i
        end
    end

    if proxy.connection.backend_ndx == 0 then
        utils.debug("[" .. rw_ndx .. "] taking master as default", 1)
        if rw_ndx > 0 then
            proxy.connection.backend_ndx = rw_ndx
            min_idle_conns = proxy.global.config.rwsplit.min_idle_connections
            mid_idle_conns = proxy.global.config.rwsplit.mid_idle_connections
            max_idle_conns = proxy.global.config.rwsplit.max_idle_connections
        else
            utils.debug("connection will be rejected", 1)
            return session_err("rw ndx is zero", 0)
        end
    else
        is_backend_conn_keepalive = true
    end

    local backend = proxy.global.backends[proxy.connection.backend_ndx]
    cur_idle = backend.pool.users[""].cur_idle_connections
    connected_clients =  backend.connected_clients

    if cur_idle > 0 and proxy.connection.server then 
        utils.debug("using pooled connection from: " .. proxy.connection.backend_ndx, 1)
        local backend_state = backend.state
        if backend_state == proxy.BACKEND_STATE_UP then
            use_pool_conn = true
            if backend.type == proxy.BACKEND_TYPE_RW and (cur_idle > mid_idle_conns) and
                (cur_idle + connected_clients) > (max_idle_conns + min_idle_conns) then
                is_backend_conn_keepalive = false
                utils.debug("set is_backend_conn_keepalive false when retrieving from pool", 1)
            end
            -- stay with it
            return proxy.PROXY_IGNORE_RESULT
        end
    end

    local final_ndx = proxy.connection.backend_ndx
    proxy.global.stat_clients[final_ndx] = proxy.global.stat_clients[final_ndx] + 1
  	utils.debug("use backend ndx:" .. proxy.connection.backend_ndx, 1)
  	utils.debug("connection created:" .. proxy.global.stat_clients[final_ndx], 1)

    -- open a new connection 
end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )
    utils.debug("[read_auth_result] " .. proxy.connection.client.src.name)
    utils.debug("using connection from: " .. proxy.connection.backend_ndx, 1)
    utils.debug("server address: " .. proxy.connection.server.dst.name, 1)
    utils.debug("server charset: " .. proxy.connection.server.character_set_client, 1)

    if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
        -- auth was fine, disconnect from the server
        if not use_pool_conn and is_backend_conn_keepalive then
            proxy.connection.backend_ndx = 0
        else
            utils.debug("no need to put the connection to pool ... ok", 1)
        end
        utils.debug("(read_auth_result) ... ok", 1)
    elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
        -- we received either a 
        -- 
        -- * MYSQLD_PACKET_ERR and the auth failed or
        -- * MYSQLD_PACKET_EOF which means a OLD PASSWORD (4.0) was sent
        utils.debug("(read_auth_result) ... not ok yet", 1)
    elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
        -- auth failed
        utils.debug("(read_auth_result) ... auth failed", 1)
    end
end

local get_sharding_group
local dispose_one_query
local _buildUpCombinedResultSet
local _getFields

function read_query( packet )
    _total_queries_per_req = 0

    _combinedNumberOfQueries = 0 
    tokens = nil

    if proxy.global.config.rwsplit.is_sharding_mode then
        local groups = {}
        _combinedResultSet = {}
        _combinedResultSet.fields = nil 
        _combinedResultSet.rows = nil 
        _combinedResultSet.affected_rows = 0 
        _combinedLimit = {}
        _combinedLimit.rowsProcessed = 0 
        _combinedLimit.rowsSent = 0 
        _combinedLimit.from = 0 
        _combinedLimit.rows = 0 

        get_sharding_group(packet, groups)
        local shard_num = #groups
        for _, group in pairs(groups) do
            utils.debug("group name:'" .. group .. "'", 1)
            if shard_num > 1 then
                proxy.connection.shard_num = shard_num
                _combinedNumberOfQueries = _combinedNumberOfQueries + 1
            end
            local result = dispose_one_query(packet, group)
            if result ~= proxy.PROXY_SEND_QUERY then
                return result
            end
        end
    else
        dispose_one_query(packet, "default")
    end

    return proxy.PROXY_SEND_QUERY
end


local orderByColumn

function get_sharding_group(packet, groups)
    _query = packet:sub(2)

    if packet:byte() == proxy.COM_QUERY then
        tokens = tokenizer.tokenize(_query)
        local _queryAnalyzer = queryAnalyzer.QueryAnalyzer.create(tokens, tableKeyColumns)
        local success, errorMessage = pcall(_queryAnalyzer.analyze, _queryAnalyzer)
        if (success) then
            orderByColumn = _queryAnalyzer:order_by_column()
            utils.debug("--- check isPartitioningNeeded", 1)
            if (_queryAnalyzer:isPartitioningNeeded()) then
                utils.debug("--- check isFullPartitionScanNeeded", 1)
                if (_queryAnalyzer:isFullPartitionScanNeeded()) then
                    utils.debug("--- full scan '" .. _query .. "'", 1)
                    local tableName = _queryAnalyzer:getAffectedTables()[1]
                    stats.incFullPartitionScans(tableName)
                    _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                    shardingLookup.getAllShardingGroups(tableName, groups)
                else
                    utils.debug("--- not full scan '" .. _query .. "'", 1)
                    if _queryAnalyzer:getShardType() == 0 then
                        for tableName, key in pairs(_queryAnalyzer:getTableKeyValues()) do
                            local group = shardingLookup.getShardingGroupByHash(tableName, key)
                            utils.debug("group chosen:" .. group, 1)
                            table.insert(groups, group)
                        end
                    else
                        utils.debug("--- range sharding '" .. _query .. "'", 1)
                        _combinedLimit.from, _combinedLimit.rows = _queryAnalyzer:getLimit()
                        for tableName, range in pairs(_queryAnalyzer:getTableKeyRange()) do
                            shardingLookup.getShardingGroupByRange(tableName, range, groups)
                        end
                    end
                end
            end
        else
            utils.debug("error:" .. errorMessage, 1)
        end
    end

    if #groups == 0 then
        table.insert(groups, "default")
    end
end


--- 
-- read/write splitting
function dispose_one_query( packet, group )
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
    local client_quit_need_sent = false

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
                    utils.debug("no rw connections yet", 1)
                    return session_err("003,master connections are too small", 1)
                end
            else
                return session_err("004,proxy stops serving requests now", 0)
            end
        end

        if b.pool.stop_phase then
            utils.debug("  stop serving requests", 1)
            return session_err("005,proxy stops serving requests now", 0)
        end
    end

    -- looks like we have to forward this statement to a backend
    utils.debug("[read_query] " .. proxy.connection.client.src.name)
    utils.debug("current backend   = " .. backend_ndx, 1)
    utils.debug("client default db = " .. c.default_db, 1)
    utils.debug("client username   = " .. c.username, 1)
    utils.debug("cmd type = " .. cmd.type, 1)
    if cmd.type == proxy.COM_QUERY then 
        utils.debug("query = "        .. cmd.query, 1)
    end

    if cmd.type == proxy.COM_QUIT then
        if backend_ndx <= 0 or (is_backend_conn_keepalive and not is_in_transaction) then
            -- don't send COM_QUIT to the backend. We manage the connection
            -- in all aspects.
            -- proxy.response = {
            --	type = proxy.MYSQLD_PACKET_OK,
            -- }

            utils.debug("valid_parallel_stmt_cnt:" .. ps_cnt, 1)
            utils.debug("(QUIT) current backend   = " .. backend_ndx, 1)

            if proxy.global.config.rwsplit.is_conn_reset_supported then
                proxy.queries:prepend_after_nth(cmd.type, base, string.char(31), 
                             { resultset_is_needed = true } )
                _total_queries_per_req = _total_queries_per_req + 1
            else
                return proxy.PROXY_SEND_NONE
            end
        else
            client_quit_need_sent = true
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
                utils.debug("no rw connections yet", 1)
                return session_err("006,master connections are too small", 1)
            end
        end

        return
    end

    if proxy.global.config.rwsplit.is_debug == true then
        if is_backend_conn_keepalive then
            proxy.connection.wait_clt_next_sql = 100
        end
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
            local last_insert_id_name = nil

            for i = 2, #tokens do
                local token = tokens[i]
                -- SQL_CALC_FOUND_ROWS + FOUND_ROWS() have to be executed
                -- on the same connection
                utils.debug("token: " .. token.token_name, 1)
                utils.debug("  val: " .. token.text, 1)

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
                    local ro_backend_ndx = lb.idle_ro(group)
                    if backend_ndx ~= ro_backend_ndx and ro_backend_ndx > 0 then
                        backend_ndx = ro_backend_ndx
                        proxy.connection.backend_ndx = backend_ndx

                        utils.debug("[use ro server: " .. backend_ndx .. "]", 1)
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
                                            utils.debug("[set is_auto_commit false]", 1)
                                            rw_op = true
                                        else
                                            is_auto_commit = true
                                        end
                                    elseif token.text == "sql_mode" then
                                        utils.debug("   sql mode:" .. nxt_nxt_token.text, 1)
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
                stmt.token_name == "TK_SQL_SHOW" or stmt.token_name == "TK_SQL_DESC" or 
                stmt.token_name == "TK_SQL_EXPLAIN") then
                rw_op = false
                local ro_backend_ndx = lb.idle_ro(group)
                if backend_ndx ~= ro_backend_ndx and ro_backend_ndx > 0 then
                    backend_ndx = ro_backend_ndx
                    proxy.connection.backend_ndx = backend_ndx

                    if stmt.token_name == "TK_SQL_USE" then
                        local token_len = #tokens
                        if token_len  > 1 then
                            c.default_db = tokens[2].text
                        end
                    end
                    utils.debug("[use ro server: " .. backend_ndx .. "]", 1)
                end
            end
        end

        if ro_server == true and rw_op == true then
            local rw_backend_ndx = lb.idle_failsafe_rw(group)
            if rw_backend_ndx > 0 then
                backend_ndx = rw_backend_ndx
                proxy.connection.backend_ndx = backend_ndx
                utils.debug("[use rw server:" .. backend_ndx .."]", 1)
                if ps_cnt > 0 then 
                    multiple_server_mode = true
                    utils.debug("[set multiple_server_mode true in complex env]", 1)
                end
            else
                utils.debug("no rw connections yet", 1)
                return session_err("007,master connections are too small", 1)
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
                                utils.debug("[set is_auto_commit true after trans]", 1)
                            end
                        end
                    end
                end
            elseif cmd.type == proxy.COM_STMT_PREPARE then
                is_prepared = true
            end

            conn_reserved = true
            utils.debug("[transaction statement, should use the same connection]", 1)

        else
            if cmd.type == proxy.COM_STMT_PREPARE then
                is_prepared = true
                conn_reserved = true
                utils.debug("[prepare statement], cmd:" .. cmd.query, 1)
                tokens = tokens or assert(tokenizer.tokenize(cmd.query))
                local stmt = tokenizer.first_stmt_token(tokens)

                for i = 1, #tokens do
                    local token = tokens[i]
                    utils.debug("token: " .. token.token_name, 1)
                    utils.debug("  val: " .. token.text, 1)
                end

                local session_read_only = 0

                if stmt.token_name == "TK_SQL_SELECT" then
                    session_read_only = 1
                    for i = 2, #tokens do
                        local token = tokens[i]
                        if (token.token_name == "TK_COMMENT") then
                            utils.debug("[check trans for ps]", 1)
                            local _, _, in_trans = string.find(token.text, "in_trans=%s*(%d)")
                            if in_trans ~= 0 then
                                is_in_transaction = true
                                break
                            end
                        end
                    end

                    if is_backend_conn_keepalive and ps_cnt == 0 and session_read_only == 1 then
                        local ro_backend_ndx = lb.idle_ro(group)
                        if backend_ndx ~= ro_backend_ndx and ro_backend_ndx > 0 then
                            backend_ndx = ro_backend_ndx
                            proxy.connection.backend_ndx = backend_ndx

                            utils.debug("[use ro server: " .. backend_ndx .. "]", 1)
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
                            utils.debug("[set multiple_server_mode true]", 1)
                        else
                            utils.debug("no rw connections yet", 1)
                            return session_err("008,master connections are too small", 1)
                        end
                    end
                end

            elseif ps_cnt > 0 or not is_auto_commit then
                conn_reserved = true
            end
        end
    end

    if backend_ndx > 0 then
        local b = proxy.global.backends[backend_ndx]
        if b.group ~= group then
            backend_ndx = lb.choose_rw_backend_ndx(group)
            proxy.connection.change_server = backend_ndx
            backend_ndx = 0
            multiple_server_mode = true
        end
    end

    if backend_ndx == 0 then
        local rw_backend_ndx = lb.idle_failsafe_rw(group)
        if is_backend_conn_keepalive and rw_backend_ndx <= 0 and 
            proxy.global.config.rwsplit.is_slave_write_forbidden_set then
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
        utils.debug("backend index is zero:", 1)
        return session_err("009,connections are not enough", 0)
    end

    if cmd.type ~= proxy.COM_QUIT or client_quit_need_sent then
        proxy.queries:append_after_nth(cmd.type, base, packet, { resultset_is_needed = true } )
        _total_queries_per_req = _total_queries_per_req + 1
    end
    if cmd.type == proxy.COM_STMT_CLOSE and is_in_transaction then
        proxy.connection.is_still_in_trans = true
    end

    -- attension: change stmt id after append the query
    if multiple_server_mode == true then
        if cmd.type == proxy.COM_STMT_EXECUTE or cmd.type == proxy.COM_STMT_CLOSE then
            utils.debug("stmt id before change:" .. cmd.stmt_handler_id, 1)
            proxy.connection.change_server_by_stmt_id = cmd.stmt_handler_id
            -- all related fields are invalid after this such as stmt_handler_id
        elseif cmd.type == proxy.COM_QUERY then
            utils.debug("change server by backend index:" .. backend_ndx, 1)
            proxy.connection.change_server = backend_ndx
        end
        conn_reserved = true
    elseif proxy.connection.shard_num > 1 then
        conn_reserved = true
    end

    utils.debug("cmd type:" .. cmd.type, 1)
    if conn_reserved then
        utils.debug("connection reserved", 1)
    else
        utils.debug("connection not reserved", 1)
    end

    c.is_server_conn_reserved = conn_reserved

    backend_ndx = proxy.connection.backend_ndx
    utils.debug("backend_ndx:" .. backend_ndx, 1)

    local s = proxy.connection.server
    
    if not proxy.global.ro_stat then
        proxy.global.ro_stat = 0
        proxy.global.ro_stats = 0
    end

    if not proxy.global.rw_stat then
        proxy.global.rw_stat = 0
        proxy.global.rw_stats = 0
    end

    if rw_op then
        proxy.global.rw_stat = proxy.global.rw_stat + 1
    else
        proxy.global.ro_stat = proxy.global.ro_stat + 1
    end

    if s.type == proxy.BACKEND_TYPE_RW then
        proxy.global.rw_stats = proxy.global.rw_stats + 1
    else
        proxy.global.ro_stats = proxy.global.ro_stats + 1
    end

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
                utils.debug("change sql mode", 1)

                if sql_mode ~= "" then
                    local modes = tokenizer.tokenize(sql_mode)
                    utils.debug("sql mode:" .. sql_mode, 1)
                    utils.debug("sql mode num:" .. #modes, 1)
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
                utils.debug("set server sql mode:" .. sql_mode, 1)
            end
        end
    end

    if not is_charset_set then
        local clt_charset = proxy.connection.client.charset
        local srv_charset = proxy.connection.server.charset

        if clt_charset ~= nil then
            utils.debug("client charset:" .. clt_charset, 1)
        end
        if srv_charset ~= nil then
            utils.debug("server charset:" .. srv_charset, 1)
        end

        if clt_charset ~= srv_charset then
            utils.debug("  change charset", 1)
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
            utils.debug("  change server charset_client", 1)
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
            utils.debug("  change server charset conn:", 1)
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
            utils.debug("  change server charset results", 1)
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
        utils.debug("server default db: " .. s.default_db, 1)
        utils.debug("client default db: " .. c.default_db, 1)
        utils.debug("syncronizing", 1)
        proxy.queries:prepend_after_nth(proxy.PROXY_IGNORE_RESULT, base,
         string.char(proxy.COM_INIT_DB) .. c.default_db, { resultset_is_needed = true })
        _total_queries_per_req = _total_queries_per_req + 1
    end

    -- send to master
    if backend_ndx > 0 then
        local b = proxy.global.backends[backend_ndx]
        utils.debug("sending to backend : " .. b.dst.name, 1)
        utils.debug("is_slave         : " .. tostring(b.type == proxy.BACKEND_TYPE_RO), 2)
        utils.debug("server default db: " .. s.default_db, 2)
        utils.debug("server username  : " .. s.username, 2)
    end
    utils.debug("in_trans        : " .. tostring(is_in_transaction), 2)
    utils.debug("in_calc_found   : " .. tostring(is_in_select_calc_found_rows), 2)
    utils.debug("COM_QUERY       : " .. tostring(cmd.type == proxy.COM_QUERY), 2)

    if (proxy.connection.shard_num > 1) then
        proxy.connection.shard_server = base
    end
    return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj ) 
    local res      = assert(inj.resultset)
    local flags    = res.flags

    local backend_ndx = proxy.connection.backend_ndx

    utils.debug("[read_query_result] " .. proxy.connection.client.src.name)
    utils.debug("backend_ndx:" .. backend_ndx, 1)
    utils.debug("read from server:" .. proxy.connection.server.dst.name, 1)
    utils.debug("proxy used port:" .. proxy.connection.server.src.name, 1)
    utils.debug("read index from server:" .. proxy.connection.backend_ndx, 1)
    utils.debug("inj id:" .. inj.id, 1)
    if proxy.global.config.rwsplit.is_sharding_mode then
        utils.debug("inj query:" .. _query, 1)
    end
    utils.debug("inj query:" .. inj.query, 1)
    utils.debug("init db cmd:" .. proxy.COM_INIT_DB, 1)
    utils.debug("res status:" .. res.query_status, 1)

    
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
            return session_err("011,can't switch server ".. proxy.connection.client.default_db, 0)
        end
        return proxy.PROXY_IGNORE_RESULT
    end

    if proxy.global.config.rwsplit.is_sharding_mode then
        local success, result = pcall(_buildUpCombinedResultSet, inj)
        if (not success) then
            stats.inc("invalidResults")
            _combinedNumberOfQueries = 0
            return session_err("012: Error: " .. result .. " Query: '" .. _query .. "'", 0)
        end
    end

    if res.query_status then
        if res.query_status ~= 0 then
            if is_in_transaction then
                utils.debug("   query_status: " .. res.query_status .. " error, reserve origin is_in_transaction", 1)
            end
        elseif inj.id ~= proxy.COM_STMT_PREPARE then
            is_in_transaction = flags.in_trans
            if not is_in_transaction then
                if not is_auto_commit then
                    is_in_transaction = true
                    utils.debug("set is_in_transaction true", 1)
                else
                    if not is_prepared and _combinedNumberOfQueries == 0 then
                        proxy.connection.client.is_server_conn_reserved = false
                    end
                end
            end
        end
    end

    if multiple_server_mode == true then
        utils.debug("multiple_server_mode true", 1)
        if inj.id == proxy.COM_STMT_PREPARE then
            local server_index = proxy.connection.selected_server_ndx
            utils.debug("multiple_server_mode, server index:" .. server_index, 1)
            utils.debug("change stmt id", 1)
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
    utils.debug("[disconnect_client] " .. proxy.connection.client.src.name)

    local backend_ndx = proxy.connection.backend_ndx

    if proxy.connection.client_abnormal_close == true then
        proxy.connection.connection_close = true
    else
        if not is_backend_conn_keepalive or is_in_transaction or not is_auto_commit then 
            utils.debug("  set connection_close true", 1)
            if is_in_transaction then
                utils.debug(" is_in_transaction is still true", 1) 
            end
            proxy.connection.connection_close = true
        else
            -- make sure we are disconnection from the connection
            -- to move the connection into the pool
            if  backend_ndx > 0 then
                proxy.connection.backend_ndx = 0
            end
        end
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

local orderByType = "asc"
local orderByIndex = 1

function fcompare(a, b)
    local col_type = _combinedResultSet.fields[orderByIndex].type
    if orderByType == "desc" then
        if col_type ~= 3 then
            return a[orderByIndex] > b[orderByIndex]
        else
            return tonumber(a[orderByIndex]) > tonumber(b[orderByIndex])
        end
    else
        if col_type ~= 3  then
            return a[orderByIndex] < b[orderByIndex]
        else
            return tonumber(a[orderByIndex]) < tonumber(b[orderByIndex])
        end
    end
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

        utils.debug("_combinedLimit.rows:" .. _combinedLimit.rows, 1)
        utils.debug("affected_rows:" .. _combinedResultSet.affected_rows, 1)
        utils.debug("inj id " .. inj.id, 1)
        _combinedNumberOfQueries = _combinedNumberOfQueries - 1
        if (_combinedNumberOfQueries == 0) then
            -- This has been the last result set - send all back to client
            if (_combinedResultSet.fields) then
                if (orderByColumn ~= nil) then
                    local i
                    for i = 1, #_combinedResultSet.fields do
                        utils.debug("field name:" .. _combinedResultSet.fields[i].name, 1)
                        utils.debug("order by column:" .. orderByColumn, 1)
                        if (_combinedResultSet.fields[i].name == orderByColumn) then
                            orderByIndex = i
                            break
                        end
                    end
                    table.sort(_combinedResultSet.rows, fcompare)
                end
                proxy.response.type = proxy.MYSQLD_PACKET_OK
                proxy.response.resultset = _combinedResultSet
            else
                proxy.response.type = proxy.MYSQLD_PACKET_RAW
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

