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
 

#ifndef _NETWORK_SOCKET_H_
#define _NETWORK_SOCKET_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "network-exports.h"
#include "network-queue.h"

#ifdef HAVE_SYS_TIME_H
/**
 * event.h needs struct timeval and doesn't include sys/time.h itself
 */
#include <sys/time.h>
#endif

#include <sys/types.h>      /** u_char */
#include <sys/socket.h>     /** struct sockaddr */

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>     /** struct sockaddr_in */
#endif
#include <netinet/tcp.h>

#ifdef HAVE_SYS_UN_H
#include <sys/un.h>         /** struct sockaddr_un */
#endif
#define closesocket(x) close(x)
#include <glib.h>
#include <event.h>

#include "network-address.h"

typedef enum {
	NETWORK_SOCKET_SUCCESS,
	NETWORK_SOCKET_WAIT_FOR_EVENT,
	NETWORK_SOCKET_ERROR,
	NETWORK_SOCKET_ERROR_RETRY
} network_socket_retval_t;

typedef struct network_mysqld_auth_challenge network_mysqld_auth_challenge;
typedef struct network_mysqld_auth_response network_mysqld_auth_response;

typedef struct {
    guint64  key;
    guint32  special_type;
    int      state;
} conn_ctl_info;

typedef struct {
	int fd;             /**< socket-fd */
    guint32 last_visit_time;
	struct event event; /**< events for this fd */

	network_address *src; /**< getsockname() */
	network_address *dst; /**< getpeername() */

    guint64 sql_burst_executed;

	int socket_type; /**< SOCK_STREAM or SOCK_DGRAM for now */


	guint8   last_packet_id; /**< internal tracking of the packet_id's the automaticly set the next good packet-id */
	gboolean packet_id_is_reset; /**< internal tracking of the packet_id sequencing */

	network_queue *recv_queue;
	network_queue *recv_queue_raw;
	network_queue *send_queue;

	off_t header_read;
	off_t to_read;
	
	/**
	 * data extracted from the handshake  
	 */
	network_mysqld_auth_challenge *challenge;
	network_mysqld_auth_response  *response;

	gboolean is_authed;           /** did a client already authed this connection */
	gboolean is_server_conn_reserved;
	gboolean is_need_quick_peek_executed;

    guint8    charset_code;

	/**
	 * store the default-db of the socket
	 *
	 * the client might have a different default-db than the server-side due to
	 * statement balancing
	 */	
	GString *default_db;     /** default-db of this side of the connection */

    GString *charset;
    GString *charset_client;
    GString *charset_connection;
    GString *charset_results;
    GString *sql_mode;

} network_socket;

#define MAX_SERVER_NUM 64

typedef struct {
    int num;
    network_socket *server[MAX_SERVER_NUM];
} server_list_t;

NETWORK_API network_socket *network_socket_init(void) G_GNUC_DEPRECATED;
NETWORK_API network_socket *network_socket_new(void);
NETWORK_API void network_socket_free(network_socket *s);
NETWORK_API network_socket_retval_t network_socket_write(network_socket *con, int send_chunks);
NETWORK_API network_socket_retval_t network_socket_read(network_socket *con);
NETWORK_API network_socket_retval_t network_socket_to_read(network_socket *sock);
NETWORK_API network_socket_retval_t network_socket_set_non_blocking(network_socket *sock);
NETWORK_API network_socket_retval_t network_socket_connect(network_socket *con);
NETWORK_API network_socket_retval_t network_socket_connect_finish(network_socket *sock);
NETWORK_API network_socket_retval_t network_socket_bind(network_socket *con);
NETWORK_API network_socket *network_socket_accept(network_socket *srv);

#endif

