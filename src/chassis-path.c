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
#ifdef WIN32
/* need something compatible, taken from MSDN docs */
#define PATH_MAX MAX_PATH
#include <windows.h>
#else
#include <stdlib.h> /* for realpath */
#endif
#include <string.h>
#include "chassis-path.h"

gchar *chassis_get_basedir(const gchar *prgname) {
	gchar *absolute_path;
	gchar *bin_dir;
	gchar r_path[PATH_MAX];
	gchar *base_dir;
	
	if (g_path_is_absolute(prgname)) {
		absolute_path = g_strdup(prgname); /* No need to dup, just to get free right */
	} else {
		/**
		 * the path wasn't absolute
		 *
		 * Either it is
		 * - in the $PATH 
		 * - relative like ./bin/... or
		 */

		absolute_path = g_find_program_in_path(prgname);
		if (absolute_path == NULL) {
			g_critical("can't find myself (%s) in PATH", prgname);

			return NULL;
		}

		if (!g_path_is_absolute(absolute_path)) {
			gchar *cwd = g_get_current_dir();

			g_free(absolute_path);

			absolute_path = g_build_filename(cwd, prgname, NULL);

			g_free(cwd);
		}
	}

	/* assume that the binary is in ./s?bin/ and that the the basedir is right above it
	 *
	 * to get this working we need a "clean" basedir, no .../foo/./bin/ 
	 */
#ifdef WIN32
	if (0 == GetFullPathNameA(absolute_path, PATH_MAX, r_path, NULL)) {
		g_critical("%s: GetFullPathNameA(%s) failed: %s",
				G_STRLOC,
				absolute_path,
				g_strerror(errno));

		return NULL;
	}
#else
	if (NULL == realpath(absolute_path, r_path)) {
		g_critical("%s: realpath(%s) failed: %s",
				G_STRLOC,
				absolute_path,
				g_strerror(errno));

		return NULL;
	}
#endif
	bin_dir = g_path_get_dirname(r_path);
	base_dir = g_path_get_dirname(bin_dir);
	
	/* don't free base_dir, because we need it later */
	g_free(absolute_path);
	g_free(bin_dir);

	return base_dir;
}

/**
 * Helper function to correctly take into account the users base-dir setting for
 * paths that might be relative.
 * Note: Because this function potentially frees the pointer to gchar* that's passed in and cannot lock
 *       on that, it is _not_ threadsafe. You have to ensure threadsafety yourself!
 * @returns TRUE if it modified the filename, FALSE if it didn't
 */
gboolean chassis_resolve_path(const char *base_dir, gchar **filename) {
	gchar *new_path = NULL;

	if (!base_dir ||
		!filename ||
		!*filename)
		return FALSE;
	
	/* don't even look at absolute paths */
	if (g_path_is_absolute(*filename)) return FALSE;
	
	new_path = g_build_filename(base_dir, G_DIR_SEPARATOR_S, *filename, NULL);
	
	g_debug("%s.%d: adjusting relative path (%s) to base_dir (%s). New path: %s", __FILE__, __LINE__, *filename, base_dir, new_path);

	/**
	 * We're going to rewrite the value of *filename, so we need to free the memory held by the pointer or it will lead to a memory leak.
	 *
	 * g_option_context_parse() is called after this call, so if it fails it will try to free the pointer. Since the pointer
	 * was free'd and changed here, the storage(GOptionContext) which helds the pointer will not know about it and result in an access violation.
	 * 
	 * To fix this bug(#14665885) we need to change the chassis API. The temporary fix is to remove the free() and avoid the possible crash.
	 * It's a non repeating leak so a minor drawback.
	 */
#if 0
	g_free(*filename);
#endif

	*filename = new_path;
	return TRUE;
}

/**
 * Check if a path-string is parent of another
 * @returns TRUE if the path-string is parent, FALSE if don't
 */
gboolean chassis_path_string_is_parent_of(const char *parent, gsize parent_len, const char *child, gsize child_len) {

	if (child_len < parent_len)
		return FALSE;

	if(parent_len == 0)
		return FALSE;

	/* On Unix systems '/' is parent of all directories' */
	if(0 == strcmp(parent, "/"))
		return TRUE;

	/* The path-string child can start with the same prefix string however the directory is not correct,
	 * e.g. parent: /foo/bar , child: /foo/bar-foo.
	 * To make the correct comparison we need to check if the parent path-string ends with the directory separator '/',
	 * and if not, add it before doing the comparison
	 */
	if (parent[parent_len - 1] != G_DIR_SEPARATOR) {
		/* g_strndup adds the trailing '\0' at the end of the new string */
		char *temp_parent = g_strndup(parent, parent_len + 1);
		temp_parent[parent_len] = G_DIR_SEPARATOR;

		if (0 == strncmp(temp_parent, child, parent_len + 1)) {
			g_free(temp_parent);
			return TRUE;
		}

		g_free(temp_parent);
		return FALSE;
	}

	if (0 == strncmp(parent, child, parent_len))
		return TRUE;

	return FALSE;
}

