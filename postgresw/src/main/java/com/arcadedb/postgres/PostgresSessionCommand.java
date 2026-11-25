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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A session-reset statement the protocol layer answers itself (issue #9328): {@code DISCARD ALL | PLANS | SEQUENCES |
 * TEMP[ORARY]}, {@code DEALLOCATE [PREPARE] { name | ALL }} and {@code CLOSE { name | ALL }}. None is a production of the
 * SQL grammar, and {@code DISCARD ALL} is what a connection pooler runs between two client sessions (it is pgbouncer's
 * default {@code server_reset_query}).
 * <p>
 * Parsed at Parse and applied at Execute, like {@code SET}. The command tag is the canonical text of the statement, which
 * is PostgreSQL's own tag: it repeats the sub-command for {@code DISCARD}, and {@code CLOSE} is tagged
 * {@code CLOSE CURSOR}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
record PostgresSessionCommand(Kind kind, String name) {

  enum Kind {
    DISCARD_ALL("DISCARD ALL"),
    DISCARD_PLANS("DISCARD PLANS"),
    DISCARD_SEQUENCES("DISCARD SEQUENCES"),
    DISCARD_TEMP("DISCARD TEMP"),
    DEALLOCATE("DEALLOCATE"),
    DEALLOCATE_ALL("DEALLOCATE ALL"),
    CLOSE("CLOSE CURSOR"),
    CLOSE_ALL("CLOSE CURSOR ALL");

    final String tag;

    Kind(final String tag) {
      this.tag = tag;
    }
  }

  private static final Set<String> TAGS = Arrays.stream(Kind.values()).map(kind -> kind.tag).collect(Collectors.toUnmodifiableSet());

  /**
   * True when {@code upperCaseText} starts a statement {@link #parse} answers.
   */
  static boolean isSessionCommand(final String upperCaseText) {
    return upperCaseText.startsWith("DISCARD ") || upperCaseText.startsWith("DEALLOCATE ") || upperCaseText.startsWith("CLOSE ");
  }

  /**
   * True when {@code upperCaseText} is one of the command tags, which is what the executor leaves as the text of a
   * portal that carries a session command.
   */
  static boolean isTag(final String upperCaseText) {
    return TAGS.contains(upperCaseText);
  }

  String tag() {
    return kind.tag;
  }

  /**
   * @throws PostgresSessionSettings.SettingException {@code 42601} for a statement that is not one of the forms above
   */
  static PostgresSessionCommand parse(final String query) {
    final String[] tokens = tokensOf(query);
    final PostgresSessionCommand command = tokens.length == 0 ? null : fromTokens(tokens);
    if (command == null) {
      final String shown = query.length() > 80 ? query.substring(0, 80) + "..." : query;
      throw new PostgresSessionSettings.SettingException("syntax error in \"" + shown + "\"",
          PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR);
    }
    return command;
  }

  /**
   * The words of {@code query}, in order: a double-quoted name, which may hold blanks and doubled quotes, a back-quoted one, or
   * else a run of non-blanks. A double-quoted name that is never closed by a single quote ends at the first quote of the last
   * doubled one, and when it holds none either, what starts at its quote is a run of non-blanks. Read with a loop: the same words
   * read by a regular expression recurse once per character of a quoted name, as far as the stack allows.
   */
  private static String[] tokensOf(final String query) {
    final List<String> found = new ArrayList<>(4);
    final int length = query.length();
    int start = 0;
    while (start < length) {
      final char first = query.charAt(start);
      if (PostgresStatementText.isBlank(first)) {
        start++;
        continue;
      }

      int end = first == '"' ? endOfDoubleQuoted(query, start) : first == '`' ? endOfBackQuoted(query, start) : -1;
      if (end < 0) {
        end = start + 1;
        while (end < length && !PostgresStatementText.isBlank(query.charAt(end)))
          end++;
      }
      found.add(query.substring(start, end));
      start = end;
    }
    return found.toArray(new String[0]);
  }

  /** The end of the name that opens with the double quote at {@code open}, or -1 when it does not close. */
  private static int endOfDoubleQuoted(final String query, final int open) {
    int lastDoubled = -1;
    int from = open + 1;
    while (true) {
      final int quote = query.indexOf('"', from);
      if (quote < 0)
        return lastDoubled < 0 ? -1 : lastDoubled + 1;
      if (quote + 1 < query.length() && query.charAt(quote + 1) == '"') {
        lastDoubled = quote;
        from = quote + 2;
      } else
        return quote + 1;
    }
  }

  /** The end of the name that opens with the back quote at {@code open}, or -1 when it does not close. */
  private static int endOfBackQuoted(final String query, final int open) {
    final int close = query.indexOf('`', open + 1);
    return close < 0 ? -1 : close + 1;
  }

  /**
   * The command the tokens spell, or null when they are not one of the supported forms.
   */
  private static PostgresSessionCommand fromTokens(final String[] tokens) {
    return switch (tokens[0].toUpperCase(Locale.ENGLISH)) {
      case "DISCARD" -> tokens.length != 2 ? null : switch (tokens[1].toUpperCase(Locale.ENGLISH)) {
        case "ALL" -> new PostgresSessionCommand(Kind.DISCARD_ALL, null);
        case "PLANS" -> new PostgresSessionCommand(Kind.DISCARD_PLANS, null);
        case "SEQUENCES" -> new PostgresSessionCommand(Kind.DISCARD_SEQUENCES, null);
        case "TEMP", "TEMPORARY" -> new PostgresSessionCommand(Kind.DISCARD_TEMP, null);
        default -> null;
      };
      case "DEALLOCATE" -> {
        // DEALLOCATE [ PREPARE ] { name | ALL }
        final int first = tokens.length > 2 && "PREPARE".equalsIgnoreCase(tokens[1]) ? 2 : 1;
        if (tokens.length != first + 1)
          yield null;
        yield "ALL".equalsIgnoreCase(tokens[first]) ?
            new PostgresSessionCommand(Kind.DEALLOCATE_ALL, null) :
            new PostgresSessionCommand(Kind.DEALLOCATE, identifier(tokens[first]));
      }
      case "CLOSE" -> tokens.length != 2 ? null : "ALL".equalsIgnoreCase(tokens[1]) ?
          new PostgresSessionCommand(Kind.CLOSE_ALL, null) :
          new PostgresSessionCommand(Kind.CLOSE, identifier(tokens[1]));
      default -> null;
    };
  }

  /**
   * An identifier as PostgreSQL reads it: a double-quoted one keeps its case, an unquoted one folds to lower case.
   */
  private static String identifier(final String token) {
    // The simple-query path hands a double-quoted identifier over already rewritten into back-ticks
    if (token.length() >= 2 && token.charAt(0) == '`' && token.charAt(token.length() - 1) == '`')
      return token.substring(1, token.length() - 1);
    if (token.length() >= 2 && token.charAt(0) == '"' && token.charAt(token.length() - 1) == '"')
      return token.substring(1, token.length() - 1).replace("\"\"", "\"");
    return token.toLowerCase(Locale.ENGLISH);
  }
}
