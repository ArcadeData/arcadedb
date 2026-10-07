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
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  // A double-quoted identifier may hold spaces and doubled quotes; anything else ends at whitespace
  private static final Pattern TOKEN = Pattern.compile("\"(?:[^\"]|\"\")*\"|`[^`]*`|\\S+");
  private static final Set<String> TAGS = Set.of("DISCARD ALL", "DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP", "DEALLOCATE",
      "DEALLOCATE ALL", "CLOSE CURSOR", "CLOSE CURSOR ALL");

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
    final List<String> found = new ArrayList<>(4);
    final Matcher matcher = TOKEN.matcher(query);
    while (matcher.find())
      found.add(matcher.group());
    final String[] tokens = found.toArray(new String[0]);
    final String keyword = tokens.length > 0 ? tokens[0].toUpperCase(Locale.ENGLISH) : "";
    switch (keyword) {
    case "DISCARD" -> {
      if (tokens.length == 2)
        switch (tokens[1].toUpperCase(Locale.ENGLISH)) {
        case "ALL" -> {
          return new PostgresSessionCommand(Kind.DISCARD_ALL, null);
        }
        case "PLANS" -> {
          return new PostgresSessionCommand(Kind.DISCARD_PLANS, null);
        }
        case "SEQUENCES" -> {
          return new PostgresSessionCommand(Kind.DISCARD_SEQUENCES, null);
        }
        case "TEMP", "TEMPORARY" -> {
          return new PostgresSessionCommand(Kind.DISCARD_TEMP, null);
        }
        default -> {
        }
        }
    }
    case "DEALLOCATE" -> {
      // DEALLOCATE [ PREPARE ] { name | ALL }
      final int first = tokens.length > 2 && "PREPARE".equalsIgnoreCase(tokens[1]) ? 2 : 1;
      if (tokens.length == first + 1)
        return "ALL".equalsIgnoreCase(tokens[first]) ?
            new PostgresSessionCommand(Kind.DEALLOCATE_ALL, null) :
            new PostgresSessionCommand(Kind.DEALLOCATE, identifier(tokens[first]));
    }
    case "CLOSE" -> {
      if (tokens.length == 2)
        return "ALL".equalsIgnoreCase(tokens[1]) ?
            new PostgresSessionCommand(Kind.CLOSE_ALL, null) :
            new PostgresSessionCommand(Kind.CLOSE, identifier(tokens[1]));
    }
    default -> {
    }
    }
    throw new PostgresSessionSettings.SettingException("syntax error in \"" + (query.length() > 80 ? query.substring(0, 80) + "..." : query) + "\"", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR);
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
