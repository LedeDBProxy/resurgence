/* $%BEGINLICENSE%$
 Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "sql-tokenizer.h"

gboolean trav(gpointer _a, gpointer _b, gpointer _udata) {
	gboolean *is_first = _udata;
	const char *key = _a;
	gint value = GPOINTER_TO_INT(_b);

	if (!*is_first) {
		printf(",");
	}
	printf("\n\t%d /* %s */", value, key);

	*is_first = FALSE;
	return FALSE;
}

int main() {
	GTree *tokens;
	gboolean is_first = TRUE;
	gint i;

	tokens = g_tree_new((GCompareFunc)g_ascii_strcasecmp);

	for (i = 0; i < sql_token_get_last_id(); i++) {
		/** only tokens with TK_SQL_* are keyworks */
		if (0 != strncmp(sql_token_get_name(i, NULL), "TK_SQL_", sizeof("TK_SQL_") - 1)) continue;

		g_tree_insert(tokens, (sql_token_get_name(i, NULL) + sizeof("TK_SQL_") - 1), GINT_TO_POINTER(i));
	}

	/* traverse the tree and output all keywords in a sorted way */
	printf("static int sql_keywords[] = {");
	g_tree_foreach(tokens, trav, &is_first);
	printf("\n};\n");

	printf("int *sql_keywords_get() { return sql_keywords; }\n");
	printf("int sql_keywords_get_count() { return sizeof(sql_keywords) / sizeof(sql_keywords[0]); }\n");

	g_tree_destroy(tokens);

	return 0;
}
