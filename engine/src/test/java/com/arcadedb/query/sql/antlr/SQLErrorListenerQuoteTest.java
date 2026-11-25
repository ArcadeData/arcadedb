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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SQL syntax error quotes the offending line of the statement, and a line can be longer than the field the message
 * travels in can carry (issue #12567).
 * <p>
 * The message reaches the client over gRPC as the {@code grpc-message} metadata entry, and gRPC caps the metadata a peer
 * receives at 16 KB. A statement that is one long line - a generated {@code INSERT}, a wide {@code CREATE} - therefore
 * carried every character of it into the message, and the client was answered with a transport failure instead of the
 * syntax error that named the offending character.
 */
class SQLErrorListenerQuoteTest {

  /**
   * The most characters of the offending line the message may quote. The listener has a
   * constant of its own for it; the test states the limit itself so that it fails on a
   * listener that quotes the whole line.
   */
  private static final int QUOTE_BUDGET = 4 * 1024;

  @Test
  @DisplayName("A short line is quoted whole")
  void aShortLineIsQuotedWhole() {
    final String statement = "SELECT * FROM " + "x".repeat(100);

    final String message = messageFor(statement, 10);

    assertThat(message).contains(statement);
    assertThat(message).contains("^");
  }

  @Test
  @DisplayName("A long line is quoted within a budget, so the message fits what a transport can carry")
  void aLongLineIsQuotedWithinABudget() {
    final String statement = "SELECT * FROM " + "x".repeat(40_000);

    final String message = messageFor(statement, 10);

    // The message is bounded: the quote is the part that can be long.
    assertThat(message.length()).isLessThan(QUOTE_BUDGET + 1_000);
    // What was wrong is still said, and the caret still points into the quoted text.
    assertThat(message).contains("SQL syntax error at line 1");
    assertThat(message).contains("^");
    assertThat(message).contains("...");
  }

  @Test
  @DisplayName("An error late in a long line is quoted around the failing character, and the caret points at it")
  void anErrorLateInALongLineIsQuotedAroundIt() {
    final String statement = "SELECT " + "a".repeat(40_000) + " FROM";

    final String message = messageFor(statement, 40_005);

    assertThat(message.length()).isLessThan(QUOTE_BUDGET + 1_000);
    // The caret is under the quoted text: the line it points at is the quote, not the whole
    // statement, so it is not padded out to the original column.
    final String[] lines = message.split("\n");
    assertThat(lines).hasSizeGreaterThanOrEqualTo(3);
    final String quoted = lines[1];
    final String caret = lines[2];
    assertThat(caret.strip()).isEqualTo("^");
    assertThat(caret.length()).isLessThanOrEqualTo(quoted.length() + 1);
    // The quote starts at the window around the failing character, not at the start of the
    // statement, so the failing character is in it.
    assertThat(quoted).startsWith("...");
    assertThat(quoted).contains("FROM");
  }

  @Test
  @DisplayName("A short line keeps the caret at the parser's column, also one past its end")
  void aShortLineKeepsTheCaretAtTheColumn() {
    final String statement = "SELECT * FROM";

    // An error at the end of the input is reported one column past the last character
    final String[] lines = messageFor(statement, statement.length()).split("\n");

    assertThat(lines[1]).isEqualTo(statement);
    assertThat(lines[2]).isEqualTo(" ".repeat(statement.length()) + "^");
  }

  @Test
  @DisplayName("A short line keeps the caret under the failing character")
  void aShortLineKeepsTheCaretUnderTheFailingCharacter() {
    final String statement = "SELECT * FORM Foo";

    final String[] lines = messageFor(statement, 9).split("\n");

    assertThat(lines[1]).isEqualTo(statement);
    assertThat(lines[2]).isEqualTo(" ".repeat(9) + "^");
  }

  /**
   * The message the parser raises for this statement.
   */
  private String messageFor(final String statement, final int column) {
    try {
      new SQLErrorListener(statement).syntaxError(null, null, 1, column, "mismatched input", null);
      throw new AssertionError("the listener did not raise");
    } catch (final CommandSQLParsingException e) {
      return e.getMessage();
    }
  }
}
