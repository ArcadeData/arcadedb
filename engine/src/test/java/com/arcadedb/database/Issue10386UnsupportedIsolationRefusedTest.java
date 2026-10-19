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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10386: an isolation level the engine cannot provide is refused on every surface, because silently giving a weaker
 * guarantee than the one asked for is the one failure a client cannot detect. {@link IsolationLevels} is the one parser the
 * surfaces share, and the SQL and openCypher entry points of this module go through it: asking for {@code SERIALIZABLE}
 * is refused with the same words, naming the levels the engine provides, instead of the JVM's own "No enum constant"
 * (SQL) or a message of that surface's own (openCypher).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10386UnsupportedIsolationRefusedTest extends TestHelper {

  @Test
  void theLevelsTheEngineProvidesAreParsedByTheirExactName() {
    assertThat(IsolationLevels.parse("READ_COMMITTED")).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    assertThat(IsolationLevels.parse("REPEATABLE_READ")).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  @Test
  void aLevelTheEngineDoesNotProvideIsRefusedNamingTheOnesItDoes() {
    assertThatThrownBy(() -> IsolationLevels.parse("SERIALIZABLE")).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid isolationLevel 'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]");
    assertThatThrownBy(() -> IsolationLevels.parse("READ_UNCOMMITTED")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IsolationLevels.parse("repeatable_read")).as("the spelling is exact: a caller upper-cases first")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IsolationLevels.parse("")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IsolationLevels.parse(null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("accepted values are");
  }

  @Test
  void aSqlBeginOfAnUnsupportedLevelIsRefusedWithTheSharedMessage() {
    assertThatThrownBy(() -> database.command("sql", "BEGIN ISOLATION SERIALIZABLE")).hasStackTraceContaining(
        "Invalid isolationLevel 'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]");
    assertThat(database.isTransactionActive()).as("the refused BEGIN opened nothing").isFalse();
  }

  @Test
  void aSqlBeginOfASupportedLevelStillBegins() {
    database.command("sql", "BEGIN ISOLATION REPEATABLE_READ").close();
    try {
      assertThat(database.isTransactionActive()).isTrue();
      assertThat(((DatabaseInternal) database).getTransaction().getIsolationLevel())
          .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aCypherStartTransactionOfAnUnsupportedLevelIsRefusedWithTheSharedMessage() {
    assertThatThrownBy(() -> database.command("opencypher", "START TRANSACTION ISOLATION SERIALIZABLE"))
        .hasStackTraceContaining("Invalid isolationLevel 'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]");
    assertThat(database.isTransactionActive()).as("the refused START TRANSACTION opened nothing").isFalse();
  }

  @Test
  void aCypherStartTransactionOfASupportedLevelStillBegins() {
    database.command("opencypher", "START TRANSACTION ISOLATION REPEATABLE_READ").close();
    try {
      assertThat(database.isTransactionActive()).isTrue();
      assertThat(((DatabaseInternal) database).getTransaction().getIsolationLevel())
          .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    } finally {
      database.rollback();
    }
  }

  @Test
  void theCypherRefusalIsAParsingError() {
    assertThatThrownBy(() -> database.command("opencypher", "START TRANSACTION ISOLATION SERIALIZABLE"))
        .isInstanceOf(CommandParsingException.class);
  }
}
