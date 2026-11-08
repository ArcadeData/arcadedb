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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.QueryEngineManager;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * The query language of a statement received through the Postgres wire protocol, and the languages this protocol
 * permits ({@link GlobalConfiguration#POSTGRES_LANGUAGES}).
 * <p>
 * A statement is SQL unless its FIRST token is a {@code {language}} marker, as in {@code {cypher}MATCH (n) RETURN n}.
 * Leading whitespace and comments are not tokens, as for PostgreSQL's own lexer: some BI tools prepend a comment to
 * every native query. A marker anywhere else is part of the statement - typically a string literal, which is what a
 * driver that interpolates parameters on the client side writes for a bound value - and must never select an engine:
 * the rest of the statement would then run as a statement of that engine, written by whoever chose the value.
 * <p>
 * The marker may only name a language of the configured list, and only one an engine of this server answers. Anything
 * else is refused with {@code 0A000} feature_not_supported before any engine is looked up, so an unknown name never
 * falls through to the resolution of the GraalVM languages either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresQueryLanguages {
  static final String SQL                            = "sql";
  static final String SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000";

  private final Set<String> permitted;

  /**
   * @param configured the comma-separated languages a marker may select, besides SQL, which is always permitted
   */
  PostgresQueryLanguages(final String configured) {
    final Set<String> languages = new LinkedHashSet<>();
    languages.add(SQL);
    if (configured != null)
      for (final String language : configured.split(",")) {
        final String name = language.trim().toLowerCase(Locale.ENGLISH);
        if (!name.isEmpty())
          languages.add(name);
      }
    this.permitted = Collections.unmodifiableSet(languages);
  }

  /**
   * The languages a marker may select, SQL first.
   */
  Set<String> permitted() {
    return permitted;
  }

  /**
   * The language of {@code statement} and the text its engine is handed: the statement as it is when it carries no
   * leading marker, otherwise what follows the marker.
   *
   * @throws Refusal when the leading marker names a language this protocol does not permit, or no engine answers
   */
  Selection select(final String statement) {
    final int start = firstTokenOffset(statement);
    if (start >= statement.length() || statement.charAt(start) != '{')
      return new Selection(SQL, statement);

    int end = start + 1;
    while (end < statement.length() && isMarkerChar(statement.charAt(end)))
      ++end;
    if (end == start + 1 || end >= statement.length() || statement.charAt(end) != '}')
      // Not a marker: the statement is handed to SQL as it is, whose parser answers for it
      return new Selection(SQL, statement);

    final String name = statement.substring(start + 1, end);
    final String language = name.toLowerCase(Locale.ENGLISH);
    if (!permitted.contains(language))
      throw new Refusal("Query language '" + name + "' is not permitted on the PostgreSQL protocol, which accepts " + permitted
          + " (setting '" + GlobalConfiguration.POSTGRES_LANGUAGES.getKey() + "')");
    // Asked only for a permitted name: an unknown one would make the registry resolve the GraalVM languages
    if (!SQL.equals(language) && !QueryEngineManager.getInstance().isLanguageRegistered(language))
      throw new Refusal("Query language '" + name + "' is permitted on the PostgreSQL protocol (setting '"
          + GlobalConfiguration.POSTGRES_LANGUAGES.getKey() + "') but no query engine of this server answers it");

    return new Selection(language, statement.substring(end + 1).trim());
  }

  /**
   * Offset of the first character of {@code text} that is neither whitespace nor part of a comment: a {@code --} comment
   * runs to the end of the line, a C-style comment ends at its matching terminator and nests, as in PostgreSQL. An unterminated comment
   * leaves no token, and so no marker.
   */
  static int firstTokenOffset(final String text) {
    final int length = text.length();
    int i = 0;
    while (i < length) {
      final char c = text.charAt(i);
      if (Character.isWhitespace(c))
        ++i;
      else if (c == '-' && i + 1 < length && text.charAt(i + 1) == '-') {
        i += 2;
        while (i < length && text.charAt(i) != '\n' && text.charAt(i) != '\r')
          ++i;
      } else if (c == '/' && i + 1 < length && text.charAt(i + 1) == '*') {
        i += 2;
        int depth = 1;
        while (i < length && depth > 0) {
          if (text.charAt(i) == '*' && i + 1 < length && text.charAt(i + 1) == '/') {
            --depth;
            i += 2;
          } else if (text.charAt(i) == '/' && i + 1 < length && text.charAt(i + 1) == '*') {
            ++depth;
            i += 2;
          } else
            ++i;
        }
        if (depth > 0)
          return length;
      } else
        return i;
    }
    return length;
  }

  /**
   * The characters of a language name: those of {@code \w} without {@code UNICODE_CHARACTER_CLASS}, which is what a
   * marker accepted before.
   */
  private static boolean isMarkerChar(final char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
  }

  /**
   * The language a statement runs in and the text handed to its engine.
   */
  record Selection(String language, String text) {
  }

  /**
   * A leading marker this protocol does not honour. Text this server wrote, with at most the language name the client
   * sent in it, so it is shown to the client in production mode too.
   */
  static final class Refusal extends RuntimeException {
    final String sqlState = SQLSTATE_FEATURE_NOT_SUPPORTED;

    Refusal(final String message) {
      super(message);
    }
  }
}
