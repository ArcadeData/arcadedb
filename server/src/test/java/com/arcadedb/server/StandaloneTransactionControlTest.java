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
package com.arcadedb.server;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link StandaloneTransactionControl#isTransactionControl} must recognize every text the openCypher engine reads as a transaction
 * control statement: the cheap prefix test in front of the parse skips what the grammar skips, and anything it lets through is
 * decided by the engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StandaloneTransactionControlTest extends TestHelper {
  @ParameterizedTest
  @ValueSource(strings = { "START TRANSACTION", "start transaction", "COMMIT", "rollback", "  COMMIT", "\tROLLBACK", "/* note */ COMMIT",
      "// note\nCOMMIT", "PROFILE START TRANSACTION", "EXPLAIN COMMIT",
      // whitespace of the openCypher lexer that java.lang.Character.isWhitespace() does not know
      "\u00A0COMMIT", "\u0085COMMIT", "\u2007ROLLBACK", "\u202FSTART TRANSACTION", "\u3000COMMIT" })
  void aTransactionControlIsRecognized(final String command) {
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "opencypher", command)).as(command).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "RETURN 1", "MATCH (n) RETURN n", "BEGINNING", "COMMITTED",
      "START", "\u00A0RETURN 1", "" })
  void anyOtherTextIsNot(final String command) {
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "opencypher", command)).as(command).isFalse();
  }

  /** The USE clause the grammar accepts in front of a statement names the database of the connection. */
  @Test
  void aTransactionControlBehindAUseClauseIsRecognized() {
    final String name = database.getName();
    for (final String command : new String[] { "USE " + name + " COMMIT", "use " + name + " rollback",
        "USE GRAPH " + name + " START TRANSACTION", "USE\u00A0" + name + "\u00A0COMMIT", "/* note */ USE " + name + " COMMIT" })
      assertThat(StandaloneTransactionControl.isTransactionControl(database, "opencypher", command)).as(command).isTrue();

    for (final String command : new String[] { "USE " + name + " RETURN 1", "USE " + name + " MATCH (n) RETURN n" })
      assertThat(StandaloneTransactionControl.isTransactionControl(database, "opencypher", command)).as(command).isFalse();
  }

  @Test
  void aMissingDatabaseCommandOrLanguageIsNotATransactionControl() {
    assertThat(StandaloneTransactionControl.isTransactionControl(null, "opencypher", "COMMIT")).isFalse();
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "opencypher", null)).isFalse();
    assertThat(StandaloneTransactionControl.isTransactionControl(database, null, "COMMIT")).isFalse();
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "gremlin", "COMMIT")).isFalse();
  }

  @Test
  void theSqlTransactionControlsAreStillRecognized() {
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "sql", "BEGIN")).isTrue();
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "sql", "commit")).isTrue();
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "sql", "PROFILE ROLLBACK")).isTrue();
    assertThat(StandaloneTransactionControl.isTransactionControl(database, "sql", "SELECT 1")).isFalse();
  }
}
