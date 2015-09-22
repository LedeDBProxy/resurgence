/* $%BEGINLICENSE%$
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

 $%ENDLICENSE%$ */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_FILIO_H
/**
 * required for FIONREAD on solaris
 */
#include <sys/filio.h>
#endif

#include <sys/ioctl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define ioctlsocket ioctl

#include <errno.h>
#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#include "network-mysqld.h"
#include "network-mysqld-packet.h"
#include "chassis-event.h"
#include "network-mysqld-lua.h"

#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"

/**
 * lua wrappers around the connection pool
 */

#define C(x) x, sizeof(x) - 1

/**
 * get the info connection pool 
 *
 * @return nil or requested information
 */
static int proxy_pool_queue_get(lua_State *L) {
	GQueue *queue = *(GQueue **)luaL_checkself(L); 
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("cur_idle_connections"))) {
		lua_pushinteger(L, queue ? queue->length : 0);
	} else {
		lua_pushnil(L);
	}

	return 1;
}

int network_connection_pool_queue_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = { 
		{ "__index", proxy_pool_queue_get },

		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

/**
 * get the info connection pool 
 *
 * @return nil or requested information
 */
static int proxy_pool_users_get(lua_State *L) {
	network_connection_pool *pool = *(network_connection_pool **)luaL_checkself(L); 
	const char *key = luaL_checkstring(L, 2); /** the username */
	GString *s = g_string_new(key);
	GQueue **q_p = NULL;

    g_debug("%s: call proxy_pool_users_get", G_STRLOC);

	q_p = lua_newuserdata(L, sizeof(*q_p)); 
	*q_p = network_connection_pool_get_conns(pool, s, NULL);
	g_string_free(s, TRUE);

	network_connection_pool_queue_getmetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

int network_connection_pool_users_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_pool_users_get },
		{ NULL, NULL },
	};
	
	return proxy_getmetatable(L, methods);
}

static int proxy_pool_get(lua_State *L) {
	network_connection_pool *pool = *(network_connection_pool **)luaL_checkself(L); 
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("max_idle_connections"))) {
		lua_pushinteger(L, pool->max_idle_connections);
    } else if (strleq(key, keysize, C("mid_idle_connections"))) {
        lua_pushinteger(L, pool->mid_idle_connections);
    } else if (strleq(key, keysize, C("min_idle_connections"))) {
        lua_pushinteger(L, pool->min_idle_connections);
    } else if (strleq(key, keysize, C("serve_req_after_init"))) {
        lua_pushboolean(L, pool->serve_req_after_init == TRUE);
    } else if (strleq(key, keysize, C("stop_phase"))) {
        lua_pushboolean(L, pool->stop_phase == TRUE);
    } else if (strleq(key, keysize, C("init_phase"))) {
        int diff = time(0) - pool->init_time;
        if (diff > pool->max_init_last_time) {
            pool->init_phase = FALSE;
        } else {
            pool->init_phase = TRUE;
        }
        lua_pushboolean(L, pool->init_phase == TRUE);
    } else if (strleq(key, keysize, C("init_time"))) {
        int diff = time(0) - pool->init_time;
        lua_pushinteger(L, diff);
    } else if (strleq(key, keysize, C("users"))) {
		network_connection_pool **pool_p;

		pool_p = lua_newuserdata(L, sizeof(*pool_p)); 
		*pool_p = pool;

		network_connection_pool_users_getmetatable(L);
		lua_setmetatable(L, -2);
	} else {
		lua_pushnil(L);
	}

	return 1;
}


static int proxy_pool_set(lua_State *L) {
	network_connection_pool *pool = *(network_connection_pool **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("max_idle_connections"))) {
		pool->max_idle_connections = lua_tointeger(L, -1);
	} else if (strleq(key, keysize, C("mid_idle_connections"))) {
		pool->mid_idle_connections = lua_tointeger(L, -1);
	} else if (strleq(key, keysize, C("min_idle_connections"))) {
		pool->min_idle_connections = lua_tointeger(L, -1);
	} else if (strleq(key, keysize, C("max_init_time"))) {
		pool->max_init_last_time = lua_tointeger(L, -1);
	} else if (strleq(key, keysize, C("set_init_time"))) {
        if (lua_tointeger(L, -1) != 0) {
            pool->init_time = time(0);
            pool->serve_req_after_init = FALSE;
        }
	} else if (strleq(key, keysize, C("serve_req_after_init"))) {
        pool->serve_req_after_init = lua_toboolean(L, -1);
	} else if (strleq(key, keysize, C("init_phase"))) {
        pool->init_phase = lua_toboolean(L, -1);
        if (pool->init_phase) {
            pool->init_time = time(0);
            pool->serve_req_after_init = FALSE;
        }
	} else if (strleq(key, keysize, C("stop_phase"))) {
        pool->stop_phase = lua_toboolean(L, -1);
	} else {
		return luaL_error(L, "proxy.backend[...].%s is not writable", key);
	}

	return 0;
}

int network_connection_pool_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_pool_get },
		{ "__newindex", proxy_pool_set },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

/**
 * handle the events of a idling server connection in the pool 
 *
 * make sure we know about connection close from the server side
 * - wait_timeout
 */
static void network_mysqld_con_idle_handle(int event_fd, short events, void *user_data) {
	network_connection_pool_entry *pool_entry = user_data;
	network_connection_pool *pool             = pool_entry->pool;

	if (events == EV_READ) {
		int b = -1;

		/**
		 * @todo we have to handle the case that the server really sent use something
		 *        up to now we just ignore it
		 */
		if (ioctlsocket(event_fd, FIONREAD, &b)) {
			g_critical("ioctl(%d, FIONREAD, ...) failed: %s", event_fd, g_strerror(errno));
		} else if (b != 0) {
			g_critical("ioctl(%d, FIONREAD, ...) said there is something to read, oops: %d", event_fd, b);
		} else {
			/* the server decided the close the connection (wait_timeout, crash, ... )
			 *
			 * remove us from the connection pool and close the connection */
		
			network_connection_pool_remove(pool, pool_entry);
		}
	}
}


/**
 * move the con->server into connection pool and disconnect the 
 * proxy from its backend 
 */
int network_connection_pool_lua_add_connection(network_mysqld_con *con, int is_swap) {
    gboolean to_be_put_to_pool = TRUE;
    int i;
	network_connection_pool_entry *pool_entry = NULL;
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	if (st->backend != NULL && st->backend->type == BACKEND_TYPE_RW && st->to_be_closed_after_serve_req) {
		g_debug("%s: to_be_closed_after_serve_req true for con:%p", G_STRLOC, con);
		return 0;
	}

	/* con-server is already disconnected, got out */
	if (!con->server) return 0;

    if (!con->server->response) return 0;

    /* Only valid for non conn swap */  
    if (!is_swap && con->state != CON_STATE_CLIENT_QUIT && 
            con->state != CON_STATE_READ_QUERY && 
            con->state != CON_STATE_READ_AUTH_RESULT)
    {
        if (con->pool_conn_used) {
            if (con->state_bef_clt_close <= CON_STATE_READ_QUERY) 
            {
                g_message("%s: client exit before using pool connection, con:%p, prev state:%s", 
                        G_STRLOC, con, network_mysqld_con_state_get_name(con->state_bef_clt_close));
            } else {
                to_be_put_to_pool = FALSE;
            } 
        } else {
            if (con->state == CON_STATE_CLOSE_CLIENT) {
                if (con->state_bef_clt_close != CON_STATE_READ_QUERY && 
                        con->state_bef_clt_close != CON_STATE_READ_AUTH_RESULT) {
                    to_be_put_to_pool = FALSE;
                }
            } else {
                to_be_put_to_pool = FALSE;
            }
        }
    }
 
    if (to_be_put_to_pool == FALSE) {
	    g_critical("%s: try to add state:%s to connect pool", G_STRLOC, network_mysqld_con_state_get_name(con->state));
	    if (con->server->recv_queue->chunks->length > 0) {
		    g_critical("%s.%d: recv queue length :%d, state:%s",
				    __FILE__, __LINE__, con->server->recv_queue->chunks->length, 
				    network_mysqld_con_state_get_name(con->state));
	    } else {
		    g_debug("%s.%d: recv queue length:%d, state:%s",
				    __FILE__, __LINE__, con->server->recv_queue->chunks->length,
				    network_mysqld_con_state_get_name(con->state));
	    }

        GString *packet;
        while ((packet = g_queue_pop_head(con->server->recv_queue->chunks))) g_string_free(packet, TRUE);

        con->server_is_closed = TRUE;

        return 0;
    }

	/* the server connection is still authed */
	con->server->is_authed = 1;
        
    g_debug("%s: call network_connection_pool_lua_add_connection", G_STRLOC);

    if (con->server_list != NULL) {
        int i, checked = 0;
        network_socket *server;
	    network_backend_t *backend;
        server_list_t *server_list;

        server_list = con->server_list;

        for (i = 0; i < MAX_SERVER_NUM; i++) {

            if (st->backend_ndx_array[i] == 0) {
                continue;
            }

            int index = st->backend_ndx_array[i] - 1;
            server = server_list->server[index];
            backend = st->backend_array[i];

            int pending = event_pending(&(server->event), EV_READ|EV_WRITE|EV_TIMEOUT, NULL);
            if (pending) { 
                g_message("%s: server event pending:%p, ev flags:%d, ev:%p", G_STRLOC, con,
                        (server->event).ev_flags, &(server->event));
                event_del(&(server->event));
            }

            g_debug("%s: here add conn fd:%d to pool:%p ", G_STRLOC, server->fd, backend->pool); 
            pool_entry = network_connection_pool_add(backend->pool, server, con->client->src->key);
            event_set(&(server->event), server->fd, EV_READ, network_mysqld_con_idle_handle, pool_entry);
            chassis_event_add_local(con->srv, &(server->event)); 

            backend->connected_clients--;
            g_debug("%s, con:%p, backend ndx:%d:connected_clients--, clients:%d",
                        G_STRLOC, con, st->backend_ndx_array[i], backend->connected_clients);
            checked++;
            if (checked >= server_list->num) {
                break;
            }
        }

        con->server_list = NULL;

    } else {
        con->valid_prepare_stmt_cnt = 0;
        g_debug("%s: con:%p, set valid_prepare_stmt_cnt 0", G_STRLOC, con);

        int pending = event_pending(&(con->server->event), EV_READ|EV_WRITE|EV_TIMEOUT, NULL);
        if (pending) { 
            g_debug("%s: server event pending:%p, ev flags:%d, ev:%p", G_STRLOC, con,
                    (con->server->event).ev_flags, &(con->server->event));
            event_del(&(con->server->event));
        }

        g_debug("%s: add conn fd:%d to pool:%p", G_STRLOC, con->server->fd, st->backend->pool);
        /* insert the server socket into the connection pool */
        pool_entry = network_connection_pool_add(st->backend->pool, con->server, con->client->src->key);

        event_set(&(con->server->event), con->server->fd, EV_READ,
                network_mysqld_con_idle_handle, pool_entry);
        chassis_event_add_local(con->srv, &(con->server->event)); 

        st->backend->connected_clients--;
         g_debug("%s, con:%p, backend ndx:%d:connected_clients--, clients:%d",
                        G_STRLOC, con, st->backend_ndx, st->backend->connected_clients);
    }

    st->backend = NULL;
    st->backend_ndx = -1;

    con->server = NULL;

	return 1;
}

/**
 * swap the server connection with a connection from
 * the connection pool
 *
 * we can only switch backends if we have a authed connection in the pool.
 *
 * @return NULL if swapping failed
 *         the new backend on success
 */
network_socket *network_connection_pool_lua_swap(network_mysqld_con *con, int backend_ndx) {
	gboolean server_switch_need_add = FALSE;
	network_backend_t *backend = NULL;
	network_socket *send_sock;
	network_mysqld_con_lua_t *st = con->plugin_con_state;
	chassis_private *g = con->srv->priv;
	GString empty_username = { "", 0, 0 };
    conn_ctl_info info = {0, 0, 0};

	/*
	 * we can only change to another backend if the backend is already
	 * in the connection pool and connected
	 */

	backend = network_backends_get(g->backends, backend_ndx);
	if (!backend) return NULL;

	/**
	 * get a connection from the pool which matches our basic requirements
	 * - username has to match
	 * - default_db should match
	 */
		
    if (con->client->response == NULL) {
        g_debug("%s: (swap) check if we have a connection for this user in the pool: nil", 
                G_STRLOC);
    } else {
        g_debug("%s: (swap) check if we have a connection for this user in the pool '%s'", 
                G_STRLOC, con->client->response->username->str);
    }

    info.key = con->client->src->key;
    info.state = con->state;

    g_debug("%s: (swap) check server switch for conn:%p, valid_prepare_stmt_cnt:%d, orig back ndx:%d, now:%d",
            G_STRLOC, con, con->valid_prepare_stmt_cnt, st->backend_ndx, backend_ndx);
    /**
     * TODO only valid for successional prepare statements,not valid for data partition
     */
    if (st->backend_ndx != -1 && con->valid_prepare_stmt_cnt > 0 && st->backend_ndx != backend_ndx) {
        g_debug("%s: (swap) server switch is true", G_STRLOC);

        if (backend->type == BACKEND_TYPE_RW) {
            server_switch_need_add = TRUE;
        } 

        if (con->server_list != NULL && st->backend_ndx_array[backend_ndx] > 0) {
            send_sock = con->server_list->server[st->backend_ndx_array[backend_ndx] - 1];
            g_debug("%s: (swap) by pass, con:%p", G_STRLOC, con);
            return send_sock;
        }
    }

    if (NULL == (send_sock = network_connection_pool_get(backend->pool, 
					con->client->response ? con->client->response->username : &empty_username,
					con->client->default_db, &info))) {
		/**
		 * no connections in the pool
		 */
		st->backend_ndx = -1;
		return NULL;
	}

    if (server_switch_need_add) {
        if (st->backend_ndx_array == NULL) {
            st->backend_ndx_array = g_new0(short, MAX_SERVER_NUM);
            st->backend_ndx_array[st->backend_ndx] = 1;
        }

        if (st->backend_array == NULL) {
            st->backend_array = g_new0(network_backend_t *, MAX_SERVER_NUM);
            st->backend_array[st->backend_ndx] = st->backend;
            g_debug("%s: (swap) first server to server list, backend array index:%d, pool:%p, server fd:%d", 
                G_STRLOC, st->backend_ndx, st->backend->pool, send_sock->fd);
        }

        if (con->server_list == NULL) {
            con->server_list = g_new0(server_list_t, 1);
            con->server_list->server[0] = con->server;
            con->server_list->server[1] = send_sock;
            con->server_list->num = 2;
            g_debug("%s: (swap) first server to server list, index:0, fd:%d, index:1, fd:%d", 
                G_STRLOC, con->server->fd, send_sock->fd);
        } else {
            g_debug("%s: (swap) add server to server list, index:%d, server fd:%d", 
                G_STRLOC, con->server_list->num, send_sock->fd);
            con->server_list->server[con->server_list->num] = send_sock;
            con->server_list->num++;

        }

        st->backend_ndx_array[backend_ndx] = con->server_list->num;
        st->backend_array[backend_ndx] = backend;
        g_debug("%s: (swap) add server to server list, backend array index:%d, pool:%p, server fd:%d", 
                G_STRLOC, backend_ndx, backend->pool, send_sock->fd);
    } else {

        if (con->server) {
            g_debug("%s: (swap) take and move the current backend into the pool:%p", G_STRLOC, con);
            /* the backend is up and cool, take and move the current backend into the pool */
            /* g_debug("%s: (swap) added the previous connection to the pool", G_STRLOC); */
            if (con->state < CON_STATE_READ_AUTH_RESULT) {
                g_critical("%s, con:%p, state:%d:server connection returned to pool",
                        G_STRLOC, con, con->state);
            }
            network_connection_pool_lua_add_connection(con, 1);
        }
    }

    /* connect to the new backend */
    st->backend = backend;
    st->backend->connected_clients++;
    st->backend_ndx = backend_ndx;

    g_debug("%s, con:%p, backend ndx:%d:connected_clients++, clients:%d, sock:%p",
                        G_STRLOC, con, backend_ndx, st->backend->connected_clients, send_sock);

    return send_sock;
}



