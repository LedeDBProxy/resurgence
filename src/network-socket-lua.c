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

#include "network-socket.h"
#include "network-mysqld-packet.h"
#include "network-address-lua.h"
#include "network-socket-lua.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

static int proxy_socket_get(lua_State *L) {
	network_socket *sock = *(network_socket **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	/**
	 * we to split it in .client and .server here
	 */

	if (strleq(key, keysize, C("default_db"))) {
		lua_pushlstring(L, sock->default_db->str, sock->default_db->len);
		return 1;
	} else if (strleq(key, keysize, C("address"))) {
		return luaL_error(L, ".address is deprecated. Use .src.name or .dst.name instead");
	} else if (strleq(key, keysize, C("src"))) {
		return network_address_lua_push(L, sock->src);
	} else if (strleq(key, keysize, C("dst"))) {
		return network_address_lua_push(L, sock->dst);
	} else if(strleq(key, keysize, C("charset"))) {

        if (sock->charset->len == 0) {
            g_string_assign_len(sock->charset, "latin1", strlen("latin1"));
        }
        lua_pushlstring(L, sock->charset->str, sock->charset->len);
        return 1;

	} else if(strleq(key, keysize, C("character_set_client"))) {
        if (sock->charset_client->len > 0) {
            lua_pushlstring(L, sock->charset_client->str, sock->charset_client->len);
        } else {
            lua_pushnil(L);
        }
        return 1;
    } else if(strleq(key, keysize, C("character_set_connection"))) {
        if (sock->charset_connection->len > 0) {
            lua_pushlstring(L, sock->charset_connection->str, sock->charset_connection->len);
        } else {
            lua_pushnil(L);
        }
        return 1;
    } else if(strleq(key, keysize, C("character_set_results"))) {
        if (sock->charset_results->len > 0) {
            lua_pushlstring(L, sock->charset_results->str, sock->charset_results->len);
        } else {
            lua_pushnil(L);
        }
        return 1;
    } else if(strleq(key, keysize, C("sql_mode"))) {
        if (sock->sql_mode->len > 0) {
            lua_pushlstring(L, sock->sql_mode->str, sock->sql_mode->len);
        } else {
            lua_pushnil(L);
        }
        return 1;
    }

      
	if (sock->response) {
		if (strleq(key, keysize, C("username"))) {
			lua_pushlstring(L, S(sock->response->username));
			return 1;
		} else if (strleq(key, keysize, C("scrambled_password"))) {
			lua_pushlstring(L, S(sock->response->auth_plugin_data));
			return 1;
		} else if (strleq(key, keysize, C("auth_plugin_name"))) {
			lua_pushlstring(L, S(sock->response->auth_plugin_name));
			return 1;
		}
	}

	if (sock->challenge) {
		if (strleq(key, keysize, C("mysqld_version"))) {
			lua_pushinteger(L, sock->challenge->server_version);
			return 1;
		} else if (strleq(key, keysize, C("thread_id"))) {
			lua_pushinteger(L, sock->challenge->thread_id);
			return 1;
		} else if (strleq(key, keysize, C("scramble_buffer"))) {
			lua_pushlstring(L, S(sock->challenge->auth_plugin_data));
			return 1;
		} else if (strleq(key, keysize, C("auth_plugin_name"))) {
			lua_pushlstring(L, S(sock->challenge->auth_plugin_name));
            return 1;
        }
    }
    g_critical("%s: sock->challenge: %p, sock->response: %p (looking for %s)", 
			G_STRLOC,
			(void *)sock->challenge,
			(void *)sock->response,
			key
			);

	lua_pushnil(L);

	return 1;
}

static int proxy_socket_set(lua_State *L) {
    network_socket *sock = *(network_socket **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("is_server_conn_reserved"))) {
        sock->is_server_conn_reserved = lua_toboolean(L, -1);
    } else if (strleq(key, keysize, C("default_db"))) {
        size_t s_len = 0;
        const char *s = lua_tolstring(L, -1, &s_len);
        if (s != NULL && s_len > 0) { 
            g_string_assign_len(sock->default_db, s, s_len);
        }
    } else if (strleq(key, keysize, C("charset"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);
            g_string_assign_len(sock->charset, s, s_len);
            g_string_assign_len(sock->charset_client, s, s_len);
            g_string_assign_len(sock->charset_connection, s, s_len);
            g_string_assign_len(sock->charset_results, s, s_len);

            if (strleq(s, s_len, C("latin1"))) {
                sock->charset_code = 8;
            } else if (strleq(s, s_len, C("utf8"))) {
                sock->charset_code = 33;
            } else if (strleq(s, s_len, C("binary"))) {
                sock->charset_code = 63;
            } else if (strleq(s, s_len, C("utf8mb4"))) {
                sock->charset_code = 45;
            } else if (strleq(s, s_len, C("gb2312"))) {
                sock->charset_code = 24;
            } else if (strleq(s, s_len, C("gbk"))) {
                sock->charset_code = 28;
            } else if (strleq(s, s_len, C("big5"))) {
                sock->charset_code = 1;
            } else {
                g_critical("charset is unknown:%s", s);
            }

            g_critical("conn:%p, charset code:%d", sock, sock->charset_code);
        }
    } else if (strleq(key, keysize, C("character_set_client"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);
            g_string_assign_len(sock->charset_client, s, s_len);
        }
    } else if (strleq(key, keysize, C("character_set_connection"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);
            g_string_assign_len(sock->charset_connection, s, s_len);
        }
    } else if (strleq(key, keysize, C("character_set_results"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);
            g_string_assign_len(sock->charset_results, s, s_len);
        }
    } else if (strleq(key, keysize, C("sql_mode"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);
            if (sock->sql_mode->len == 0 || s_len == 0) {
                g_string_assign_len(sock->sql_mode, s, s_len);
                if (s_len == 0) {
                    g_debug("%s: empty sql mode for conn:%p", G_STRLOC, sock);
                }
            } else {
                g_string_append_len(sock->sql_mode, " ", 1);
                g_string_append_len(sock->sql_mode, s, s_len);
            }
        }
    } else if (strleq(key, keysize, C("server_sql_mode"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);
            g_string_assign_len(sock->sql_mode, s, s_len);
            if (s_len == 0) {
                g_debug("%s: empty sql mode for conn:%p", G_STRLOC, sock);
            }
        }
    }

    return 0;
}

int network_socket_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_socket_get },
		{ "__newindex", proxy_socket_set },
		{ NULL, NULL },
	};
	return proxy_getmetatable(L, methods);
}


