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

import com.arcadedb.query.sql.parser.CreateEdgeStatement;
import com.arcadedb.query.sql.parser.CreateVertexStatement;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.query.sql.parser.InsertStatement;
import com.arcadedb.query.sql.parser.MatchStatement;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.TraverseStatement;
import com.arcadedb.query.sql.parser.UpdateStatement;

import java.util.Locale;

/**
 * The tag of the CommandComplete message that ends a statement (issues #11312, #11313).
 * <p>
 * A statement the server parsed is tagged from its CLASS: the text a client sent can open with a comment, a line break or two
 * spaces between {@code INSERT} and {@code INTO}, which the grammar accepts and a test on the text's first words does not, and
 * the tag of an INSERT, UPDATE or DELETE is where a driver reads the affected-row count from ({@code executeUpdate()}, the
 * counts of {@code executeBatch()}): a commented INSERT that wrote its row was tagged {@code ""} and reported 0 rows. The
 * statements of the family PostgreSQL counts carry the count ({@code INSERT 0 n}, {@code UPDATE n}, {@code DELETE n},
 * {@code SELECT n}); every other statement, a schema change among them, is tagged with its own keywords and no count, as
 * PostgreSQL tags {@code CREATE TABLE}: a driver reads a tag that does not end with a number as 0 rows changed. Derived from the
 * class name, so a statement class the grammar gains later is tagged without an entry here.
 * <p>
 * The text is only read for what the server answers without parsing (transaction control, SET/RESET, SHOW, SAVEPOINT,
 * DISCARD, COPY) and for a language this server does not parse, past any leading comment and with its whitespace collapsed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresCommandTag {
  /** The text tags never test more than the first words, nor compare against a longer canonical keyword than this. */
  private static final int TEXT_PREFIX_LENGTH = 64;

  /** {@code CreateDocumentTypeStatement} -> {@code CREATE DOCUMENT TYPE}, computed once per class. */
  private static final ClassValue<String> KEYWORDS = new ClassValue<>() {
    @Override
    protected String computeValue(final Class<?> type) {
      return keywordsOf(type.getSimpleName());
    }
  };

  private PostgresCommandTag() {
  }

  /**
   * @param statement the statement the server parsed, or {@code null} when it parsed none
   * @param text      the text of the statement, or the canonical keyword the caller already resolved it to
   * @param count     the rows the statement returned or changed
   */
  static String of(final Statement statement, final String text, final long count) {
    if (statement == null)
      return ofText(text, count);
    final String tag = ofStatement(statement, count);
    // a class whose name spells no keyword (an anonymous subclass) falls back to the text rather than answer ""
    return tag.isEmpty() ? ofText(text, count) : tag;
  }

  static String ofStatement(final Statement statement, final long count) {
    return switch (statement) {
      case InsertStatement ignored -> "INSERT 0 " + count;
      case CreateVertexStatement ignored -> "INSERT 0 " + count;
      case CreateEdgeStatement ignored -> "INSERT 0 " + count;
      case UpdateStatement ignored -> "UPDATE " + count;
      case DeleteStatement ignored -> "DELETE " + count;
      case SelectStatement ignored -> "SELECT " + count;
      case MatchStatement ignored -> "SELECT " + count;
      case TraverseStatement ignored -> "SELECT " + count;
      // a schema change among them, which returns no rows and changes none: no count, as CREATE TABLE has none on PostgreSQL
      default -> KEYWORDS.get(statement.getClass());
    };
  }

  static String ofText(final String text, final long count) {
    final String upperCaseText = leadingWords(text);
    if (upperCaseText.startsWith("CREATE VERTEX ") && !upperCaseText.startsWith("CREATE VERTEX TYPE") || upperCaseText.startsWith(
        "INSERT INTO")) {
      return "INSERT 0 " + count;
    } else if (upperCaseText.startsWith("COPY")) {
      return "COPY " + count;
    } else if (upperCaseText.startsWith("SELECT") || upperCaseText.startsWith("MATCH")) {
      return "SELECT " + count;
    } else if (upperCaseText.startsWith("UPDATE")) {
      return "UPDATE " + count;
    } else if (upperCaseText.startsWith("DELETE")) {
      return "DELETE " + count;
    } else if (PostgresTransactionStatement.TAG_START_TRANSACTION.equals(upperCaseText)) {
      // The one transaction-control tag that is not the statement's first word (issue #9631)
      return PostgresTransactionStatement.TAG_START_TRANSACTION;
    } else if ("BEGIN".equals(upperCaseText) || "COMMIT".equals(upperCaseText) || "ROLLBACK".equals(upperCaseText)) {
      // Never the client's own text: once recognized, every transaction-control statement - PostgresTransactionStatement
      // or the #8273 grammar fallback - leaves its canonical tag as portal.query (extended) or commandTag (simple), which
      // is the text that reaches here. A new recognition path must do the same, or its statement is tagged ""
      return upperCaseText;
    } else if (upperCaseText.startsWith("SAVEPOINT ")) {
      return "SAVEPOINT";
    } else if (upperCaseText.startsWith("RELEASE ")) {
      return "RELEASE";
    } else if (upperCaseText.startsWith("SET ")) {
      return "SET";
    } else if (upperCaseText.startsWith("RESET ")) {
      return "RESET";
    } else if (upperCaseText.startsWith("SHOW ")) {
      // PostgreSQL tags SHOW ALL the same way (issue #9326)
      return "SHOW";
    } else if (PostgresSessionCommand.isTag(upperCaseText)) {
      // A DISCARD/DEALLOCATE/CLOSE leaves its canonical tag as the text of the statement (issue #9328)
      return upperCaseText;
    } else {
      return "";
    }
  }

  /**
   * The first {@link #TEXT_PREFIX_LENGTH} characters of {@code text} past its leading whitespace and comments ({@code --} and
   * {@code //} to the end of the line, {@code /* ... *&#47;} nested as PostgreSQL nests them), upper-cased, every run of
   * whitespace collapsed into one space and no trailing one. Bounded, so a statement of megabytes costs no more than a short one.
   */
  static String leadingWords(final String text) {
    final int length = text.length();
    int i = 0;
    while (i < length) {
      final char c = text.charAt(i);
      if (Character.isWhitespace(c))
        i++;
      else if ((c == '-' || c == '/') && i + 1 < length && text.charAt(i + 1) == c) {
        while (i < length && text.charAt(i) != '\n')
          i++;
      } else if (c == '/' && i + 1 < length && text.charAt(i + 1) == '*') {
        int depth = 1;
        i += 2;
        while (i < length && depth > 0) {
          if (text.charAt(i) == '/' && i + 1 < length && text.charAt(i + 1) == '*') {
            depth++;
            i += 2;
          } else if (text.charAt(i) == '*' && i + 1 < length && text.charAt(i + 1) == '/') {
            depth--;
            i += 2;
          } else
            i++;
        }
      } else
        break;
    }

    final StringBuilder words = new StringBuilder(Math.min(TEXT_PREFIX_LENGTH, length - i));
    boolean pendingSpace = false;
    for (; i < length && words.length() < TEXT_PREFIX_LENGTH; i++) {
      final char c = text.charAt(i);
      if (Character.isWhitespace(c))
        pendingSpace = true;
      else {
        if (pendingSpace) {
          words.append(' ');
          pendingSpace = false;
        }
        words.append(c);
      }
    }
    return words.toString().toUpperCase(Locale.ENGLISH);
  }

  /** {@code CreateDocumentTypeStatement} -> {@code CREATE DOCUMENT TYPE}: the statement's keywords, as its class spells them. */
  static String keywordsOf(final String simpleName) {
    final String name = simpleName.endsWith("Statement") ? simpleName.substring(0, simpleName.length() - "Statement".length()) : simpleName;
    final StringBuilder keywords = new StringBuilder(name.length() + 4);
    for (int i = 0; i < name.length(); i++) {
      final char c = name.charAt(i);
      if (Character.isUpperCase(c) && i > 0)
        keywords.append(' ');
      keywords.append(Character.toUpperCase(c));
    }
    return keywords.toString();
  }
}
