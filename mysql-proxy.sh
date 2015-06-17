#!/bin/sh
#
#  $%BEGINLICENSE%$
#  Copyright (c) 2009, 2012, Oracle and/or its affiliates. All rights reserved.
# 
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License as
#  published by the Free Software Foundation; version 2 of the
#  License.
# 
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
#  02110-1301  USA
# 
#  $%ENDLICENSE%$
#
# wrapper script for self-contained packaging
#
# we assume that this script is located in
#
#   {basedir}/bin/mysql-proxy
#
# and wants to call
#
#   {basedir}/libexec/mysql-proxy
#
# You can inject a wrapper like
#
#   $ MYSQL_PROXY_WRAPPER=valgrind ./bin/mysql-proxy
#

scriptdir=`dirname $0`
scriptname=`basename $0`
scriptdir=`(cd "$scriptdir/"; pwd)`
scriptdir=`dirname "$scriptdir"`
# having LD_LIBRARY_PATH_64 set will override LD_LIBRARY_PATH 
# for 64bit libraries on Solaris. since we don't anticipate needing
# anything from "outside", we just unset it
if [ `uname -s` = SunOS ]; then
	unset LD_LIBRARY_PATH_64
fi
LUA_PATH="$scriptdir/lib//lua/?.lua;$LUA_PATH"
LUA_CPATH="$scriptdir/lib//lua/?.;$LUA_CPATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$scriptdir/lib/"
export LD_LIBRARY_PATH LUA_PATH LUA_CPATH
exec $MYSQL_PROXY_WRAPPER "$scriptdir/libexec/$scriptname" "$@"
