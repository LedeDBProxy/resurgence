/* $%BEGINLICENSE%$
 Copyright (c) 2009, 2012, Oracle and/or its affiliates. All rights reserved.

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
#include <errno.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h> /* for write() */
#endif

#include <sys/socket.h>	/* for SOCK_STREAM and AF_UNIX/AF_INET */

#include <event.h>

#include "chassis-event.h"

#define C(x) x, sizeof(x) - 1
#define closesocket(x) close(x)

#define E_NET_CONNRESET ECONNRESET
#define E_NET_CONNABORTED ECONNABORTED
#define E_NET_INPROGRESS EINPROGRESS
#if EWOULDBLOCK == EAGAIN
/**
 * some system make EAGAIN == EWOULDBLOCK which would lead to a 
 * error in the case handling
 *
 * set it to -1 as this error should never happen
 */
#define E_NET_WOULDBLOCK -1
#else
#define E_NET_WOULDBLOCK EWOULDBLOCK
#endif

/**
 * create a new event-op
 *
 * event-ops are async requests around event_add()
 */
chassis_event_op_t *chassis_event_op_new() {
	chassis_event_op_t *e;

	e = g_slice_new0(chassis_event_op_t);

	return e;
}

/**
 * free a event-op
 */
void chassis_event_op_free(chassis_event_op_t *e) {
	if (!e) return;

	g_slice_free(chassis_event_op_t, e);
}

/**
 * execute a event-op on a event-base
 *
 * @see: chassis_event_add_local(), chassis_event_op()
 */
void chassis_event_op_apply(chassis_event_op_t *op, struct event_base *event_base) {
	switch (op->type) {
	case CHASSIS_EVENT_OP_ADD:
		event_base_set(event_base, op->ev);
		event_add(op->ev, op->tv);
		break;
	case CHASSIS_EVENT_OP_UNSET:
		g_assert_not_reached();
		break;
	}
}

/**
 * set the timeout 
 *
 * takes a deep-copy of the timeout as we have our own lifecycle independent of the caller 
 */
void
chassis_event_op_set_timeout(chassis_event_op_t *op, struct timeval *tv) {
	if (NULL != tv) {
		op->_tv_storage = *tv;
		op->tv = &(op->_tv_storage);
	} else {
		op->tv = NULL;
	}
}

void chassis_event_add_with_timeout(chassis *chas, struct event *ev, struct timeval *tv) {
	chassis_event_op_t *op = chassis_event_op_new();
	gssize ret;

	op->type = CHASSIS_EVENT_OP_ADD;
	op->ev   = ev;
	chassis_event_op_set_timeout(op, tv);

    g_async_queue_push_unlocked(chas->event_queue, op);

	/* ping the event handler */
	if (1 != (ret = send(chas->event_notify_fds[1], C("."), 0))) {
		int last_errno; 

		last_errno = errno;

		switch (last_errno) {
		case EAGAIN:
		case E_NET_WOULDBLOCK:
			/* that's fine ... */
			g_debug("%s: send() to event-notify-pipe failed: %s (len = %d)",
					G_STRLOC,
					g_strerror(errno),
					g_async_queue_length_unlocked(chas->event_queue));
			break;
		default:
			g_critical("%s: send() to event-notify-pipe failed: %s (len = %d)",
					G_STRLOC,
					g_strerror(errno),
					g_async_queue_length_unlocked(chas->event_queue));
			break;
		}
	}
}

/**
 * add a event asynchronously
 *
 * @see network_mysqld_con_handle()
 */
void chassis_event_add(chassis *chas, struct event *ev) {
	chassis_event_add_with_timeout(chas, ev, NULL);
}


/**
 * add a event
 *
 * @see network_connection_pool_lua_add_connection()
 */
void chassis_event_add_local_with_timeout(chassis G_GNUC_UNUSED *chas, struct event *ev, struct timeval *tv) {
	struct event_base *event_base = chas->event_base;
	chassis_event_op_t *op;

	g_assert(event_base); 

	op = chassis_event_op_new();

	op->type = CHASSIS_EVENT_OP_ADD;
	op->ev   = ev;
	chassis_event_op_set_timeout(op, tv);

	chassis_event_op_apply(op, event_base);
	
	chassis_event_op_free(op);
}

void chassis_event_add_local(chassis *chas, struct event *ev) {
	chassis_event_add_local_with_timeout(chas, ev, NULL);
}
/**
 * handled events sent through the global event-queue 
 *
 * @see chassis_event_add()
 */
void chassis_event_handle(int G_GNUC_UNUSED event_fd, short G_GNUC_UNUSED events, void *user_data) {
	chassis_event_t *event = user_data;
	struct event_base *event_base = event->event_base;
	chassis *chas = event->chas;
	chassis_event_op_t *op;

	do {
        char ping[1];

        gsize ret;

        if (op = g_async_queue_try_pop_unlocked(chas->event_queue)) {

            chassis_event_op_apply(op, event_base);

            chassis_event_op_free(op);

            if (1 != (ret = recv(event->notify_fd, ping, 1, 0))) {
                /* we failed to pull .'s from the notify-queue */
                int last_errno; 

                last_errno = errno;

                switch (last_errno) {
                    case EAGAIN:
                    case E_NET_WOULDBLOCK:
                        /* that's fine ... */
                        g_debug("%s: recv() from event-notify-fd failed: %s",
                                G_STRLOC,
                                g_strerror(last_errno));
                        break;
                    default:
                        g_critical("%s: recv() from event-notify-fd failed: %s",
                                G_STRLOC,
                                g_strerror(last_errno));
                        break;
                }
            }
        }
    } while (op); /* even if op is 'free()d' it still is != NULL */
}

chassis_event_t *chassis_event_new() {
	chassis_event_t *event;

	event = g_new0(chassis_event_t, 1);

	return event;
}

/**
 * free the data-structures 
 *
 * closes notification-pipe and free's the event-base
 */
void chassis_event_free(chassis_event_t *event) {

	if (!event) return;

	if (event->notify_fd != -1) {
		event_del(&(event->notify_fd_event));
		closesocket(event->notify_fd);
	}

	if (event->event_base) event_base_free(event->event_base);

	g_free(event);
}


/**
 * setup the notification-fd 
 *
 * @see chassis_event_handle()
 */ 
int chassis_event_init(chassis_event_t *loop, chassis *chas) {
	loop->event_base = event_base_new();
	loop->chas = chas;
	loop->notify_fd = dup(chas->event_notify_fds[0]);
	if (-1 == loop->notify_fd) {
		g_critical("%s: Could not create duplicated socket: %s (%d)", G_STRLOC, g_strerror(errno), errno);
		return -1;
	}

	event_set(&(loop->notify_fd_event), loop->notify_fd, EV_READ | EV_PERSIST, chassis_event_handle, loop);
	event_base_set(loop->event_base, &(loop->notify_fd_event));
	event_add(&(loop->notify_fd_event), NULL);

	return 0;
}

/**
 * event-handler 
 *
 */
void *chassis_event_loop(chassis_event_t *loop) {

	/**
	 * check once a second if we shall shutdown the proxy
	 */
	while (!chassis_is_shutdown()) {
		struct timeval timeout;
		int r;

		timeout.tv_sec = 1;
		timeout.tv_usec = 0;

		r = event_base_loopexit(loop->event_base, &timeout);
		if (r == -1) {
			g_critical("%s: leaving chassis_event_loop early. event_base_loopexit() failed", G_STRLOC);
			break;
		}

		r = event_base_dispatch(loop->event_base);

		if (r == -1) {
			if (errno == EINTR) continue;
			g_critical("%s: leaving chassis_event_loop early, errno != EINTR was: %s (%d)", G_STRLOC, g_strerror(errno), errno);
			break;
		}
	}

	return NULL;
}

