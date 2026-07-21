/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.postgres;

/**
 * Translates PostgreSQL double-quoted identifiers into ArcadeDB SQL back-tick quoted identifiers.
 * <p>
 * In PostgreSQL (and in the SQL standard) a double-quoted token is always a delimited <b>identifier</b>, while a string
 * literal is single-quoted. ArcadeDB SQL instead inherits the OrientDB dialect where the double quote opens a string
 * literal and the back-tick quotes an identifier. Without this translation a client speaking the Postgres wire protocol
 * (psycopg, the official JDBC driver, Spark, most BI tools) that emits {@code SELECT "name" FROM "Character"} gets back
 * the constant string {@code name} on every row instead of the property value.
 * <p>
 * The rewrite is purely lexical and only touches what a Postgres lexer would recognise as a delimited identifier:
 * single-quoted literals, dollar-quoted literals, line comments and block comments are copied verbatim. The same holds
 * for anything nested in a JSON object or collection literal ({@code INSERT INTO T CONTENT {"k": "v"}},
 * {@code SET tags = ["a", "b"]}): those constructs only exist in the ArcadeDB dialect, have no Postgres counterpart, and
 * there the double quote keeps its ArcadeDB meaning of string literal. Identifiers starting with {@code @} (such as
 * {@code "@rid"}) are left untouched because ArcadeDB SQL already understands that form as a record attribute.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresQuotedIdentifierRewriter {

  private PostgresQuotedIdentifierRewriter() {
  }

  /**
   * @param sql the statement as received on the wire
   *
   * @return the same instance when nothing has to be translated, otherwise the rewritten statement
   */
  public static String rewrite(final String sql) {
    if (sql == null || sql.indexOf('"') < 0)
      // FAST PATH: no double quote at all, nothing to translate
      return sql;

    final int len = sql.length();
    final StringBuilder out = new StringBuilder(len + 8);

    int i = 0;
    int literalDepth = 0;
    while (i < len) {
      final char c = sql.charAt(i);
      switch (c) {
      case '\'' -> i = copyStringLiteral(sql, i, len, out);
      case '$' -> i = copyDollarQuotedLiteral(sql, i, len, out);
      case '-' -> i = copyLineComment(sql, i, len, out);
      case '/' -> i = copyBlockComment(sql, i, len, out);
      case '`' -> i = copyBackTickIdentifier(sql, i, len, out);
      case '"' -> i = literalDepth > 0 ?
          // INSIDE A JSON OBJECT OR A COLLECTION LITERAL THE DOUBLE QUOTE KEEPS ITS ARCADEDB MEANING
          copyJsonString(sql, i, len, out) :
          translateQuotedIdentifier(sql, i, len, out);
      case '{', '[' -> {
        literalDepth++;
        out.append(c);
        i++;
      }
      case '}', ']' -> {
        if (literalDepth > 0)
          literalDepth--;
        out.append(c);
        i++;
      }
      default -> {
        out.append(c);
        i++;
      }
      }
    }

    return out.toString();
  }

  /**
   * Copies a single-quoted literal verbatim. Both the SQL standard doubling ({@code ''}) and the back-slash escape
   * accepted by the ArcadeDB lexer are honoured while scanning for the closing quote.
   */
  private static int copyStringLiteral(final String sql, final int start, final int len, final StringBuilder out) {
    out.append('\'');
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        out.append(c).append(sql.charAt(i + 1));
        i += 2;
      } else if (c == '\'') {
        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
          out.append("''");
          i += 2;
        } else {
          out.append('\'');
          return i + 1;
        }
      } else {
        out.append(c);
        i++;
      }
    }
    // UNTERMINATED: let the parser report the error
    return i;
  }

  /**
   * Copies a dollar-quoted literal ({@code $$...$$} or {@code $tag$...$tag$}) verbatim. When no matching closing tag is
   * found the dollar sign is treated as an ordinary character, so ArcadeDB context variables such as {@code $current}
   * are not affected.
   */
  private static int copyDollarQuotedLiteral(final String sql, final int start, final int len, final StringBuilder out) {
    int i = start + 1;
    while (i < len && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_'))
      i++;

    if (i >= len || sql.charAt(i) != '$') {
      out.append('$');
      return start + 1;
    }

    final String tag = sql.substring(start, i + 1);
    final int end = sql.indexOf(tag, i + 1);
    if (end < 0) {
      out.append('$');
      return start + 1;
    }

    out.append(sql, start, end + tag.length());
    return end + tag.length();
  }

  private static int copyLineComment(final String sql, final int start, final int len, final StringBuilder out) {
    if (start + 1 >= len || sql.charAt(start + 1) != '-') {
      out.append('-');
      return start + 1;
    }

    int i = start;
    while (i < len && sql.charAt(i) != '\n')
      i++;
    out.append(sql, start, i);
    return i;
  }

  private static int copyBlockComment(final String sql, final int start, final int len, final StringBuilder out) {
    if (start + 1 >= len || sql.charAt(start + 1) != '*') {
      out.append('/');
      return start + 1;
    }

    int i = start + 2;
    int depth = 1;
    while (i < len && depth > 0) {
      if (i + 1 < len && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        depth++;
        i += 2;
      } else if (i + 1 < len && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        depth--;
        i += 2;
      } else
        i++;
    }
    out.append(sql, start, i);
    return i;
  }

  private static int copyBackTickIdentifier(final String sql, final int start, final int len, final StringBuilder out) {
    out.append('`');
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        out.append(c).append(sql.charAt(i + 1));
        i += 2;
      } else if (c == '`') {
        out.append('`');
        return i + 1;
      } else {
        out.append(c);
        i++;
      }
    }
    return i;
  }

  /**
   * Copies a JSON string verbatim, honouring the back-slash escapes of the JSON grammar.
   */
  private static int copyJsonString(final String sql, final int start, final int len, final StringBuilder out) {
    out.append('"');
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        out.append(c).append(sql.charAt(i + 1));
        i += 2;
      } else if (c == '"') {
        out.append('"');
        return i + 1;
      } else {
        out.append(c);
        i++;
      }
    }
    // UNTERMINATED: let the parser report the error
    return i;
  }

  private static int translateQuotedIdentifier(final String sql, final int start, final int len, final StringBuilder out) {
    final StringBuilder identifier = new StringBuilder();
    int i = start + 1;
    boolean closed = false;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '"') {
        if (i + 1 < len && sql.charAt(i + 1) == '"') {
          // ESCAPED DOUBLE QUOTE INSIDE THE IDENTIFIER
          identifier.append('"');
          i += 2;
        } else {
          i++;
          closed = true;
          break;
        }
      } else {
        identifier.append(c);
        i++;
      }
    }

    if (!closed || identifier.isEmpty() || identifier.charAt(0) == '@') {
      // UNTERMINATED, EMPTY OR RECORD ATTRIBUTE (E.G. "@rid"): LEAVE THE ORIGINAL TEXT UNCHANGED
      out.append(sql, start, i);
      return i;
    }

    out.append('`');
    for (int k = 0; k < identifier.length(); k++) {
      final char c = identifier.charAt(k);
      if (c == '`')
        out.append('\\');
      out.append(c);
    }
    out.append('`');
    return i;
  }
}
