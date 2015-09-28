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
#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#include "network-backend.h"
#include "network-mysqld.h"
#include "network-conn-pool-lua.h"
#include "network-backend-lua.h"
#include "network-address-lua.h"
#include "network-mysqld-lua.h"

/**
 * get the info about a backend
 *
 * proxy.backend[0].
 *   connected_clients => clients using this backend
 *   address           => ip:port or unix-path of to the backend
 *   state             => int(BACKEND_STATE_UP|BACKEND_STATE_DOWN) 
 *   type              => int(BACKEND_TYPE_RW|BACKEND_TYPE_RO) 
 *
 * @return nil or requested information
 * @see backend_state_t backend_type_t
 */
static int proxy_backend_get(lua_State *L) {
	network_backend_t *backend = *(network_backend_t **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("connected_clients"))) {
		lua_pushinteger(L, backend->connected_clients);
    } else if (strleq(key, keysize, C("group"))) {
        lua_pushlstring(L, S(backend->server_group));
    } else if (strleq(key, keysize, C("dst"))) {
        network_address_lua_push(L, backend->addr);
	} else if (strleq(key, keysize, C("state"))) {
		lua_pushinteger(L, backend->state);
	} else if (strleq(key, keysize, C("type"))) {
		lua_pushinteger(L, backend->type);
	} else if (strleq(key, keysize, C("uuid"))) {
		if (backend->uuid->len) {
			lua_pushlstring(L, S(backend->uuid));
		} else {
			lua_pushnil(L);
		}
	} else if (strleq(key, keysize, C("pool"))) {
		network_connection_pool *pool; 
		network_connection_pool **pool_p;

		pool_p = lua_newuserdata(L, sizeof(pool)); 
		*pool_p = backend->pool;

		network_connection_pool_getmetatable(L);
        lua_setmetatable(L, -2);
    } else if (strleq(key, keysize, C("connections"))) {
        guint total = backend->connected_clients;

        GHashTable *users = backend->pool->users;

        if (users != NULL) {

            GHashTableIter iter;
            GString *key;
            GQueue *queue;

            g_hash_table_iter_init(&iter, users);
            while (g_hash_table_iter_next(&iter, (void **)&key, (void **)&queue)) {
                total += queue->length;
            }
        }

        lua_pushinteger(L, total);

	} else {
		lua_pushnil(L);
	}

	return 1;
}

static int proxy_backend_set(lua_State *L) {
	network_backend_t *backend = *(network_backend_t **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("state"))) {
		backend->state = lua_tointeger(L, -1);
	} else if (strleq(key, keysize, C("uuid"))) {
		if (lua_isstring(L, -1)) {
			size_t s_len = 0;
			const char *s = lua_tolstring(L, -1, &s_len);

			g_string_assign_len(backend->uuid, s, s_len);
		} else if (lua_isnil(L, -1)) {
			g_string_truncate(backend->uuid, 0);
		} else {
			return luaL_error(L, "proxy.global.backends[...].%s has to be a string", key);
		}
	} else {
		return luaL_error(L, "proxy.global.backends[...].%s is not writable", key);
	}
	return 1;
}

int network_backend_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_backend_get },
		{ "__newindex", proxy_backend_set },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

/**
 * get proxy.global.backends[ndx]
 *
 * get the backend from the array of mysql backends.
 *
 * @return nil or the backend
 * @see proxy_backend_get
 */
static int proxy_backends_get(lua_State *L) {
	network_backend_t *backend; 
	network_backend_t **backend_p;

	network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
	int backend_ndx = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
	
	/* check that we are in range for a _int_ */
	if (NULL == (backend = network_backends_get(bs, backend_ndx))) {
		lua_pushnil(L);

		return 1;
	}

	backend_p = lua_newuserdata(L, sizeof(backend)); /* the table underneath proxy.global.backends[ndx] */
	*backend_p = backend;

	network_backend_lua_getmetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

static int proxy_backends_len(lua_State *L) {
	network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);

	lua_pushinteger(L, network_backends_count(bs));

	return 1;
}


static int proxy_backends_set(lua_State *L) {
    network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);
	const gchar * address = NULL;
	backend_state_t state = BACKEND_STATE_DOWN;
	backend_type_t type = BACKEND_TYPE_UNKNOWN;
	int backend_ndx = -1;
	int add_flag = 0;
	int replace_flag = 0;

	if (strleq(key, keysize, C("backend_remove"))) {
        network_backends_remove(bs, lua_tointeger(L, -1));
	} else if (strleq(key, keysize, C("backend_add"))) {
		add_flag = 1;
	} else if (strleq(key, keysize, C("backend_replace"))) {
		replace_flag = 1;
	} else {
		return luaL_error(L, "proxy.global.backends.%s is not writable", key);
	}

	if (add_flag || replace_flag) {

		if (lua_istable(L, -1)) {
			lua_pushstring(L,"address");
			lua_gettable(L,-2);
			address = lua_tostring(L, -1);
			lua_pop(L,1);

			lua_pushstring(L,"type");
			lua_gettable(L,-2);
			if (lua_isnumber(L, -1))
				type = lua_tointeger(L, -1);
			else
				type = BACKEND_TYPE_RO;
			lua_pop(L,1);

			lua_pushstring(L,"state");
			lua_gettable(L,-2);
			if (lua_isnumber(L, -1))
				state = lua_tointeger(L, -1);
			else
				state = BACKEND_STATE_MAINTAINING;
			lua_pop(L,1);

			if (replace_flag) {
				lua_pushstring(L,"backend_ndx");
				lua_gettable(L,-2);
				if (lua_isnumber(L, -1))
					backend_ndx = lua_tointeger(L, -1);
				else {
					lua_pop(L,1);
					return luaL_error(L, "replace must have backend_ndx been set.");
				}
				lua_pop(L,1);
				network_backends_modify(bs, backend_ndx, type, state);
			} else /*add_flag*/ {
				network_backends_add(bs, address, type, state);
			}
		}
		else
			return luaL_error(L, "backends must be an table have key address, type and state");
	}

	return 1;
}


int network_backends_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_backends_get },
        { "__newindex", proxy_backends_set },
		{ "__len", proxy_backends_len },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

