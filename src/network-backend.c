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
 
#include <string.h>

#include <glib.h>

#include "network-backend.h"
#include "chassis-plugin.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

const char * backend_state_t_str[BACKEND_STATE_MAX] = {
"unkown",
"online",
"down",
"maintaining",
"deleted"
};

const char * backend_type_t_str[BACKEND_TYPE_MAX] = {
"unkown",
"read/write",
"readonly"
};

network_backend_t *network_backend_new() {
	network_backend_t *b;

	b = g_new0(network_backend_t, 1);

	b->pool = network_connection_pool_new();
	b->uuid = g_string_new(NULL);
	b->addr = network_address_new();

	return b;
}


void network_backend_free(network_backend_t *b) {
	if (!b) return;

	network_connection_pool_free(b->pool);

	if (b->addr)     network_address_free(b->addr);
	if (b->uuid)     g_string_free(b->uuid, TRUE);

	g_free(b);
}

network_backends_t *network_backends_new() {
	network_backends_t *bs;

	bs = g_new0(network_backends_t, 1);

	bs->backends = g_ptr_array_new();

	return bs;
}

void network_backends_free(network_backends_t *bs) {
	gsize i;

	if (!bs) return;

	for (i = 0; i < bs->backends->len; i++) {
		network_backend_t *backend = bs->backends->pdata[i];
		
		network_backend_free(backend);
	}

	g_ptr_array_free(bs->backends, TRUE);

	g_free(bs);
}

/*
 * FIXME: 1) remove _set_address, make this function callable with result of same
 *        2) differentiate between reasons for "we didn't add" (now -1 in all cases)
 */
int network_backends_add(network_backends_t *bs, const  gchar *address, backend_type_t type, backend_state_t state) {
	network_backend_t *new_backend;
	guint i;

	new_backend = network_backend_new();
	new_backend->type = type;
	new_backend->state = state;

	if (0 != network_address_set_address(new_backend->addr, address)) {
		network_backend_free(new_backend);
		return -1;
	}

	/* check if this backend is already known */
	for (i = 0; i < bs->backends->len; i++) {
		network_backend_t *old_backend = bs->backends->pdata[i];

		if (strleq(S(old_backend->addr->name), S(new_backend->addr->name))) {
			network_backend_free(new_backend);

			g_critical("backend %s is already known!", address);
			return -1;
		}
	}


	g_ptr_array_add(bs->backends, new_backend);

	g_message("added %s backend: %s, state: %s", backend_type_t_str[type],
			address, backend_state_t_str[state]);

	return 0;
}

/**
 * we just change the state to deleted to avoid lua script refer to the wrong index.
 */
int network_backends_remove(network_backends_t *bs, guint index) {
    network_backend_t* b = bs->backends->pdata[index];
    if (b != NULL) {
		//network_backend_free(b);
        //g_ptr_array_remove_index(bs->backends, index);
        return network_backends_modify(bs, index, BACKEND_TYPE_UNKNOWN, BACKEND_STATE_DELETED);
    }
    return 0;
}

/**
 * updated the _DOWN state to _UNKNOWN if the backends were
 * down for at least 4 seconds
 *
 * we only check once a second to reduce the overhead on connection setup
 *
 * @returns   number of updated backends
 */
int network_backends_check(network_backends_t *bs) {
	GTimeVal now;
	guint i;
	int backends_woken_up = 0;
	gint64	t_diff;

	g_get_current_time(&now);
	ge_gtimeval_diff(&bs->backend_last_check, &now, &t_diff);

	/* check max(once a second) */
	/* this also covers the "time went backards" case */
	if (t_diff < G_USEC_PER_SEC) {
		if (t_diff < 0) {
			g_message("%s: time went backwards (%"G_GINT64_FORMAT" usec)!",
				G_STRLOC, t_diff);
			bs->backend_last_check.tv_usec = 0;
			bs->backend_last_check.tv_sec = 0;
		}
		return 0;
	}
	
	bs->backend_last_check = now;

	for (i = 0; i < bs->backends->len; i++) {
		network_backend_t *cur = bs->backends->pdata[i];

		if (cur->state != BACKEND_STATE_DOWN) continue;

		/* check if a backend is marked as down for more than 4 sec */
		if (now.tv_sec - cur->state_since.tv_sec > 4) {
			g_debug("%s.%d: backend %s was down for more than 4 sec, waking it up", 
					__FILE__, __LINE__,
					cur->addr->name->str);

			cur->state = BACKEND_STATE_UNKNOWN;
			cur->state_since = now;
			backends_woken_up++;
		}
	}

	return backends_woken_up;
}

/**
 * modify the backends to new type and new state.
 *
 * @returns   0 for success -1 for error.
 */

int network_backends_modify(network_backends_t * bs, guint ndx, backend_type_t type, backend_state_t state) {
	GTimeVal now;
	g_get_current_time(&now);
	if (ndx >= network_backends_count(bs)) return -1;
	network_backend_t * cur = bs->backends->pdata[ndx];

	g_message("change backend: %s from type: %s, state: %s to type: %s, state: %s",
		cur->addr->name->str, backend_type_t_str[cur->type], backend_state_t_str[cur->state],
		backend_type_t_str[type], backend_state_t_str[state]);

	if (cur->type != type) cur->type = type;
	if (cur->state != state) {
		cur->state = state;
		cur->state_since = now;
	}
	return 0;
}

network_backend_t *network_backends_get(network_backends_t *bs, guint ndx) {
	if (ndx >= network_backends_count(bs)) return NULL;

	/* FIXME: shouldn't we copy the backend or add ref-counting ? */	
	return bs->backends->pdata[ndx];
}

guint network_backends_count(network_backends_t *bs) {
	guint len;

	len = bs->backends->len;

	return len;
}

