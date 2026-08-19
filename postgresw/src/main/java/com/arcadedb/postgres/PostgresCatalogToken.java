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

import java.util.ArrayList;
import java.util.List;

/**
 * One lexical token of a PostgreSQL catalog query (issue #6412).
 * <p>
 * Tokenising first is what lets the rest of the catalog emulation work on the <i>structure</i> of a client's
 * query rather than on its exact text: a comma inside {@code CASE ... END} or inside a quoted string is not a
 * projection separator, and no amount of splitting the raw string can tell the difference reliably. The
 * string-equality matching this replaces got that wrong by construction - it could only recognise the one
 * spelling of the one tool it was written for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogToken {
  enum Type {
    /** A bare word: a keyword, a relation, a column, a function name. */
    IDENTIFIER,
    /** A {@code "double quoted"} identifier, which keeps its case and is never a keyword. */
    QUOTED_IDENTIFIER,
    /** A {@code 'single quoted'} string literal, already unescaped. */
    STRING,
    NUMBER,
    /** An operator or punctuation character. */
    SYMBOL
  }

  final Type   type;
  final String text;

  private PostgresCatalogToken(final Type type, final String text) {
    this.type = type;
    this.text = text;
  }

  boolean isKeyword(final String keyword) {
    return type == Type.IDENTIFIER && text.equalsIgnoreCase(keyword);
  }

  boolean isSymbol(final String symbol) {
    return type == Type.SYMBOL && text.equals(symbol);
  }

  @Override
  public String toString() {
    return type + "(" + text + ")";
  }

  /**
   * Splits a statement into tokens, or returns null when it contains something this lexer will not read -
   * an unterminated string, a dollar-quoted body, a comment that never closes. Returning null makes the
   * caller decline the query, which is the same thing it does for a shape it cannot answer.
   */
  static List<PostgresCatalogToken> tokenize(final String query) {
    final List<PostgresCatalogToken> tokens = new ArrayList<>();
    final int length = query.length();
    int i = 0;

    while (i < length) {
      final char c = query.charAt(i);

      if (Character.isWhitespace(c)) {
        ++i;
        continue;
      }

      // -- line comment
      if (c == '-' && i + 1 < length && query.charAt(i + 1) == '-') {
        while (i < length && query.charAt(i) != '\n')
          ++i;
        continue;
      }

      // /* block comment */, which PostgreSQL allows to nest
      if (c == '/' && i + 1 < length && query.charAt(i + 1) == '*') {
        int depth = 0;
        while (i < length) {
          if (query.charAt(i) == '/' && i + 1 < length && query.charAt(i + 1) == '*') {
            ++depth;
            i += 2;
          } else if (query.charAt(i) == '*' && i + 1 < length && query.charAt(i + 1) == '/') {
            --depth;
            i += 2;
            if (depth == 0)
              break;
          } else
            ++i;
        }
        if (depth != 0)
          return null;
        continue;
      }

      // 'string literal', with '' as the embedded quote
      if (c == '\'') {
        final StringBuilder value = new StringBuilder();
        ++i;
        boolean closed = false;
        while (i < length) {
          final char ch = query.charAt(i);
          if (ch == '\'') {
            if (i + 1 < length && query.charAt(i + 1) == '\'') {
              value.append('\'');
              i += 2;
              continue;
            }
            ++i;
            closed = true;
            break;
          }
          value.append(ch);
          ++i;
        }
        if (!closed)
          return null;
        tokens.add(new PostgresCatalogToken(Type.STRING, value.toString()));
        continue;
      }

      // E'escape string', whose backslash escapes matter: pgjdbc writes E'%' and E'\\_' in LIKE patterns
      if ((c == 'E' || c == 'e') && i + 1 < length && query.charAt(i + 1) == '\'') {
        final StringBuilder value = new StringBuilder();
        i += 2;
        boolean closed = false;
        while (i < length) {
          final char ch = query.charAt(i);
          if (ch == '\\' && i + 1 < length) {
            value.append(unescape(query.charAt(i + 1)));
            i += 2;
            continue;
          }
          if (ch == '\'') {
            if (i + 1 < length && query.charAt(i + 1) == '\'') {
              value.append('\'');
              i += 2;
              continue;
            }
            ++i;
            closed = true;
            break;
          }
          value.append(ch);
          ++i;
        }
        if (!closed)
          return null;
        tokens.add(new PostgresCatalogToken(Type.STRING, value.toString()));
        continue;
      }

      // `backtick quoted identifier`. PostgreSQL does not have these, but ArcadeDB does, and the executor
      // rewrites a client's "double quoted" identifiers into them before dispatching (see
      // PostgresQuotedIdentifierRewriter), so this is the form a catalog query actually arrives in.
      if (c == '`') {
        final int end = query.indexOf('`', i + 1);
        if (end < 0)
          return null;
        tokens.add(new PostgresCatalogToken(Type.QUOTED_IDENTIFIER, query.substring(i + 1, end)));
        i = end + 1;
        continue;
      }

      // "quoted identifier", with "" as the embedded quote
      if (c == '"') {
        final StringBuilder value = new StringBuilder();
        ++i;
        boolean closed = false;
        while (i < length) {
          final char ch = query.charAt(i);
          if (ch == '"') {
            if (i + 1 < length && query.charAt(i + 1) == '"') {
              value.append('"');
              i += 2;
              continue;
            }
            ++i;
            closed = true;
            break;
          }
          value.append(ch);
          ++i;
        }
        if (!closed)
          return null;
        tokens.add(new PostgresCatalogToken(Type.QUOTED_IDENTIFIER, value.toString()));
        continue;
      }

      // $1 and friends: a parameter placeholder, which no emulated catalog query can be answered without
      // the parameter itself, so it is left as a symbol the parser will refuse.
      if (c == '$') {
        int j = i + 1;
        while (j < length && Character.isDigit(query.charAt(j)))
          ++j;
        tokens.add(new PostgresCatalogToken(Type.SYMBOL, query.substring(i, j)));
        i = j;
        continue;
      }

      if (Character.isDigit(c)) {
        int j = i;
        while (j < length && (Character.isDigit(query.charAt(j)) || query.charAt(j) == '.'))
          ++j;
        if (j < length && (query.charAt(j) == 'e' || query.charAt(j) == 'E')) {
          int k = j + 1;
          if (k < length && (query.charAt(k) == '+' || query.charAt(k) == '-'))
            ++k;
          if (k < length && Character.isDigit(query.charAt(k))) {
            j = k;
            while (j < length && Character.isDigit(query.charAt(j)))
              ++j;
          }
        }
        tokens.add(new PostgresCatalogToken(Type.NUMBER, query.substring(i, j)));
        i = j;
        continue;
      }

      if (Character.isLetter(c) || c == '_') {
        int j = i;
        while (j < length && (Character.isLetterOrDigit(query.charAt(j)) || query.charAt(j) == '_' || query.charAt(j) == '$'))
          ++j;
        tokens.add(new PostgresCatalogToken(Type.IDENTIFIER, query.substring(i, j)));
        i = j;
        continue;
      }

      // Multi-character operators first, so that "<=" is one token and not two.
      final String twoOrThree = threeCharOperator(query, i);
      if (twoOrThree != null) {
        tokens.add(new PostgresCatalogToken(Type.SYMBOL, twoOrThree));
        i += twoOrThree.length();
        continue;
      }

      tokens.add(new PostgresCatalogToken(Type.SYMBOL, String.valueOf(c)));
      ++i;
    }

    return tokens;
  }

  private static String threeCharOperator(final String query, final int i) {
    final int length = query.length();
    if (i + 2 < length) {
      final String three = query.substring(i, i + 3);
      if ("!~*".equals(three))
        return three;
    }
    if (i + 1 < length) {
      final String two = query.substring(i, i + 2);
      switch (two) {
      case "<=", ">=", "<>", "!=", "||", "::", "~*", "!~" -> {
        return two;
      }
      default -> {
        return null;
      }
      }
    }
    return null;
  }

  private static char unescape(final char c) {
    return switch (c) {
      case 'n' -> '\n';
      case 't' -> '\t';
      case 'r' -> '\r';
      case 'b' -> '\b';
      case 'f' -> '\f';
      default -> c;
    };
  }

  /**
   * The token a bound parameter value stands for, so that a query carrying {@code $1} can be answered as if
   * the client had written the value inline. Returns null for a value with no literal form - a NULL, or an
   * array - which leaves the placeholder in place and the predicate around it unread.
   */
  static PostgresCatalogToken literal(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Number)
      return new PostgresCatalogToken(Type.NUMBER, value.toString());
    if (value instanceof CharSequence || value instanceof Character)
      return new PostgresCatalogToken(Type.STRING, value.toString());
    if (value instanceof Boolean b)
      return new PostgresCatalogToken(Type.IDENTIFIER, b ? "TRUE" : "FALSE");
    return null;
  }
}
