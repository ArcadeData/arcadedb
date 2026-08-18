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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Recognises a {@code SELECT} over one of the niladic PostgreSQL system-information functions this wire
 * protocol emulates, and reports which function it is and what the resulting column must be called.
 * <p>
 * The protocol used to match these queries by string equality against one exact spelling per function, in
 * two places (the simple-query and the extended-query paths, which had drifted into slightly different
 * comparisons). Anything a client added around the call - an alias, a trailing semicolon after a space,
 * a {@code pg_catalog.} prefix on a spelling that was only listed bare - missed the match and fell through
 * to ArcadeDB's own SQL engine, which answers a different question or none at all (issue #5290):
 * <ul>
 * <li>{@code SELECT CURRENT_SCHEMA() AS schema} failed to parse outright, because {@code schema} is a
 * reserved word in ArcadeDB SQL but only an ordinary alias in PostgreSQL. That is a connect-time query for
 * some clients, so the tool never got past opening the connection.</li>
 * <li>{@code SELECT version() AS v} answered with ArcadeDB's own {@code version()} function - the build
 * string - where the un-aliased spelling answers {@code PostgreSQL 12.0 (ArcadeDB ...)}. A client parsing
 * the server version out of that reads a version PostgreSQL never had.</li>
 * </ul>
 * Recognition therefore happens once, here, over a normalised statement, and both protocol paths share it.
 * <p>
 * This class only decides <i>what was asked</i>: the values live in the executor, which is what holds the
 * database, the authenticated user and the server version.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresSystemQuery {
  /**
   * The emulated system-information functions. {@code defaultColumnName} is the column name PostgreSQL
   * itself gives the expression when the client supplies no alias.
   */
  public enum Function {
    VERSION("version"),
    CURRENT_SCHEMA("current_schema"),
    CURRENT_DATABASE("current_database"),
    CURRENT_CATALOG("current_catalog"),
    CURRENT_USER("current_user"),
    SESSION_USER("session_user"),
    CURRENT_ROLE("current_role"),
    USER("user");

    public final String defaultColumnName;

    Function(final String defaultColumnName) {
      this.defaultColumnName = defaultColumnName;
    }
  }

  /**
   * {@code SELECT [pg_catalog.]<function>[()] [[AS] <alias>]}, with the whole statement anchored so that
   * nothing else can hide in it - a projection list, a FROM, a WHERE - and only the exact shape this class
   * can answer is claimed. The word boundary after the function name is what keeps {@code SELECT user_id}
   * from being read as {@code user} aliased to {@code _id}: the alternation would otherwise match the
   * {@code user} prefix and leave the rest to the alias group.
   * <p>
   * The parentheses are optional for every function rather than per-function: PostgreSQL spells some of
   * these as keywords ({@code current_user}) and some as calls ({@code version()}), and being stricter here
   * would only reject a query this protocol can still answer correctly.
   */
  private static final Pattern SYSTEM_QUERY = Pattern.compile(
      "^SELECT\\s+(?:PG_CATALOG\\s*\\.\\s*)?"
          + "(VERSION|CURRENT_SCHEMA|CURRENT_DATABASE|CURRENT_CATALOG|CURRENT_USER|SESSION_USER|CURRENT_ROLE|USER)\\b"
          + "\\s*(?:\\(\\s*\\))?"
          + "(?:\\s+(?:AS\\s+)?(?:\"([^\"]*)\"|([A-Za-z_][A-Za-z0-9_$]*)))?\\s*$",
      Pattern.CASE_INSENSITIVE);

  public final Function function;
  /** The column to announce in RowDescription: the client's alias when it gave one, else PostgreSQL's own name. */
  public final String   columnName;

  private PostgresSystemQuery(final Function function, final String columnName) {
    this.function = function;
    this.columnName = columnName;
  }

  /**
   * Returns the recognised system query, or null when {@code query} is not one - in which case the caller
   * must go on to the rest of its dispatch, exactly as before.
   */
  public static PostgresSystemQuery parse(final String query) {
    if (query == null)
      return null;

    final Matcher matcher = SYSTEM_QUERY.matcher(normalize(query));
    if (!matcher.matches())
      return null;

    final Function function = Function.valueOf(matcher.group(1).toUpperCase(Locale.ENGLISH));

    // A quoted alias keeps the case - and the emptiness - the client wrote; an unquoted one is folded to
    // lower case, as PostgreSQL folds every unquoted identifier. An explicit AS "" is a column named "",
    // which PostgreSQL allows, and is not the same as having supplied no alias at all.
    final String quotedAlias = matcher.group(2);
    final String bareAlias = matcher.group(3);
    final String columnName;
    if (quotedAlias != null)
      columnName = quotedAlias;
    else if (bareAlias != null)
      columnName = bareAlias.toLowerCase(Locale.ENGLISH);
    else
      columnName = function.defaultColumnName;

    return new PostgresSystemQuery(function, columnName);
  }

  /**
   * Strips the statement terminator and the whitespace around it. A client is free to send
   * {@code "SELECT current_schema() ;"} - the simple-query path only removes a semicolon that is the very
   * last character, which leaves a trailing space behind and used to break the match.
   */
  private static String normalize(final String query) {
    int end = query.length();
    while (end > 0) {
      final char c = query.charAt(end - 1);
      if (c == ';' || Character.isWhitespace(c))
        --end;
      else
        break;
    }
    return query.substring(0, end).trim();
  }
}
