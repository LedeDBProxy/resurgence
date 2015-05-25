/* $%BEGINLICENSE%$
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

 $%ENDLICENSE%$ */
 

#include <glib.h>

#include "network-conn-pool.h"
#include "network-mysqld-packet.h"
#include "glib-ext.h"
#include "sys-pedantic.h"

/** @file
 * connection pools
 *
 * in the pool we manage idle connections
 * - keep them up as long as possible
 * - make sure we don't run out of seconds
 * - if the client is authed, we have to pick connection with the same user
 * - ...  
 */

/**
 * create a empty connection pool entry
 *
 * @return a connection pool entry
 */
network_connection_pool_entry *network_connection_pool_entry_new(void) {
	network_connection_pool_entry *e;

	e = g_new0(network_connection_pool_entry, 1);

	return e;
}

/**
 * free a conn pool entry
 *
 * @param e the pool entry to free
 * @param free_sock if true, the attached server-socket will be freed too
 */
void network_connection_pool_entry_free(network_connection_pool_entry *e, gboolean free_sock) {
	if (!e) return;

	if (e->sock && free_sock) {
		network_socket *sock = e->sock;
			
		event_del(&(sock->event));
		network_socket_free(sock);
	}

	g_free(e);
}

/**
 * free all pool entries of the queue
 *
 * used as GDestroyFunc in the user-hash of the pool
 *
 * @param q a GQueue to free
 *
 * @see network_connection_pool_new
 * @see GDestroyFunc
 */
static void g_queue_free_all(gpointer q) {
	GQueue *queue = q;
	network_connection_pool_entry *entry;

	while ((entry = g_queue_pop_head(queue))) network_connection_pool_entry_free(entry, TRUE);

	g_queue_free(queue);
}

/**
 * init a connection pool
 */
network_connection_pool *network_connection_pool_new(void) {
	network_connection_pool *pool;

	pool = g_new0(network_connection_pool, 1);

    pool->max_idle_connections = 100;
    pool->mid_idle_connections = 50;
    pool->min_idle_connections = 10;
    pool->init_phase = TRUE;
	pool->users = g_hash_table_new_full(g_hash_table_string_hash, g_hash_table_string_equal, g_hash_table_string_free, g_queue_free_all);

	return pool;
}

/**
 * free all entries of the pool
 *
 */
void network_connection_pool_free(network_connection_pool *pool) {
	if (!pool) return;

	g_hash_table_foreach_remove(pool->users, g_hash_table_true, NULL);

	g_hash_table_destroy(pool->users);

	g_free(pool);
}

/**
 * find the entry which has more than max_idle connections idling
 * 
 * @return TRUE for the first entry having more than _user_data idling connections
 * @see network_connection_pool_get_conns 
 */
static gboolean find_idle_conns(gpointer UNUSED_PARAM(_key), gpointer _val, gpointer _user_data) {
	guint idle_conns_threshold = *(gint *)_user_data;
	GQueue *conns = _val;

    g_debug("%s: conns length:%d, idle_conns_threshold:%d", G_STRLOC, conns->length, idle_conns_threshold);
	return (conns->length > idle_conns_threshold);
}

GQueue *network_connection_pool_get_conns(network_connection_pool *pool, GString *username, GString *UNUSED_PARAM(default_db)) {
	GQueue *conns = NULL;


	if (username && username->len > 0) {
		conns = g_hash_table_lookup(pool->users, username);
		/**
		 * if we know this use, return a authed connection 
		 */
		g_debug("%s: (get_conns) get user-specific idling connection for '%s' -> %p", G_STRLOC, username->str, conns);
		if (conns) return conns;
	}

	/**
	 * we don't have a entry yet, check the others if we have more than 
	 * min_idle waiting
	 */

    if (pool->init_phase) {
        conns = g_hash_table_find(pool->users, find_idle_conns, &(pool->mid_idle_connections));
        if (conns && conns->length > pool->mid_idle_connections) {
            pool->init_phase = FALSE;
		    g_debug("%s: (get_conns) init phase complete for user '%s' -> %p", G_STRLOC, username->str, conns);
        }
    } else {
        conns = g_hash_table_find(pool->users, find_idle_conns, &(pool->min_idle_connections));
    }

	g_debug("%s: (get_conns) try to find max-idling conns for user '%s' -> %p", G_STRLOC, username ? username->str : "", conns);

	return conns;
}

/**
 * get a connection from the pool
 *
 * make sure we have at lease <min-conns> for each user
 * if we have more, reuse a connect to reauth it to another user
 *
 * @param pool connection pool to get the connection from
 * @param username (optional) name of the auth connection
 * @param default_db (unused) unused name of the default-db
 */
network_socket *network_connection_pool_get(network_connection_pool *pool,
		GString *username,
		GString *UNUSED_PARAM(default_db), conn_ctl_info *info) {

    guint32  cur;
    guint    len, i;
	network_socket *sock = NULL;
	network_connection_pool_entry *entry, *found_entry = NULL;

	GQueue *conns = network_connection_pool_get_conns(pool, username, NULL);

	/**
	 * if we know this use, return a authed connection 
	 */
	if (conns) {
        cur = time(0);
        len = g_queue_get_length(conns);
        for (i = 0; i < len; i++) {
            entry = g_queue_peek_nth(conns, i);
            if (entry->key == info->key) {
                if (!entry->shared) {
                    found_entry = entry;
                    g_queue_pop_nth (conns, i);
                    break;
                }
            }
        }

        if (!found_entry && len > 0) {
            entry = g_queue_peek_nth(conns, 0);
            found_entry = entry;
            g_queue_pop_nth (conns, 0);
            g_debug("%s: (get) entry for user '%s' -> %p, cur:%u",
                    G_STRLOC, username ? username->str : "", entry, cur);
        }

		if (conns->length == 0) {
			/**
			 * all connections are gone, remove it from the hash
			 */
			g_hash_table_remove(pool->users, username);
		}
	}

    if (!found_entry) {
		g_debug("%s: (get) no entry for user '%s' -> %p", G_STRLOC, username ? username->str : "", conns);
		return NULL;
	}

	sock = found_entry->sock;

	network_connection_pool_entry_free(found_entry, FALSE);

	/* remove the idle handler from the socket */	
	event_del(&(sock->event));
		
	g_debug("%s: (get) got socket for user '%s' -> %p", G_STRLOC, username ? username->str : "", sock);

	return sock;
}

/**
 * add a connection to the connection pool
 *
 */
network_connection_pool_entry *network_connection_pool_add(network_connection_pool *pool, 
        network_socket *sock, guint64 key) 
{
	network_connection_pool_entry *entry;
	GQueue *conns = NULL;

	entry = network_connection_pool_entry_new();
	entry->sock = sock;
	entry->pool = pool;
    entry->key = key;

	g_get_current_time(&(entry->added_ts));
	
	g_debug("%s: (add) adding socket to pool for user '%s' -> %p", G_STRLOC, sock->response->username->str, sock);

	if (NULL == (conns = g_hash_table_lookup(pool->users, sock->response->username))) {
		conns = g_queue_new();

		g_hash_table_insert(pool->users, g_string_dup(sock->response->username), conns);
	}

	g_queue_push_tail(conns, entry);

	return entry;
}

/**
 * remove the connection referenced by entry from the pool 
 */
void network_connection_pool_remove(network_connection_pool *pool, network_connection_pool_entry *entry) {
	network_socket *sock = entry->sock;
	GQueue *conns;

	if (NULL == (conns = g_hash_table_lookup(pool->users, sock->response->username))) {
		return;
	}

	network_connection_pool_entry_free(entry, TRUE);

	g_queue_remove(conns, entry);
}


