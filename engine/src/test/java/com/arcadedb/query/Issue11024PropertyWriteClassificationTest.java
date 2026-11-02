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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11024: a statement that takes its provisional records back gives back the writes it counted beside them only when they
 * assigned no property, and decides that from {@code MutableDocument.arePropertiesAssigned()}. The asynchronous (write-now) arm of
 * the update wrote the page first, which clears the flag on the caller's instance, and asked it after: a property write was
 * classified property-less, given back, and the commit the property should have refused was allowed. Both arms are driven here
 * with the savepoint budget at one entry, so the statement cannot be taken back and the counter is the only guard, as in
 * {@code Issue10482StatementGuardGapsTest.aStatementThatWritesPastTheLimitOfTheSavepointKeepsTheCommitRefused}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11024PropertyWriteClassificationTest extends TestHelper {
  private AfterRecordCreateListener listener;

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE PROPERTY V.p STRING");
    database.command("sql", "ALTER PROPERTY V.p MANDATORY true");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX T SET k = 1").close();
      database.command("sql", "CREATE VERTEX T SET k = 2").close();
    });
    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 1);
  }

  @AfterEach
  void restore() {
    if (listener != null)
      database.getEvents().unregisterListener(listener);
    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
        GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
  }

  /** The deferred arm: the property is set on an existing record while the provisional one is pending. */
  @Test
  void aPropertyWrittenWhileTheProvisionalRecordIsPendingStillRefusesTheCommit() {
    database.begin();
    assertStatementFails(() -> database.command("opencypher",
        "CREATE (v:V {k: 15}) WITH v MATCH (m:T {k: 1}) SET m.note = 'x' CREATE (m)-[:R {w: 1}]->(v)").close());

    assertCommitRefusedAndNothingStands();
  }

  /**
   * The write-now arm, the one {@code DatabaseAsyncUpdateRecord} takes: a property written to another record while the provisional
   * one is pending. With {@code discardRecordAfter} false the page write hands the instance its new buffer, which clears the flag;
   * with it true the flag survives. Both must refuse the commit.
   */
  @ParameterizedTest(name = "discardRecordAfter={0}")
  @ValueSource(booleans = { false, true })
  void aPropertyWrittenNowWhileTheProvisionalRecordIsPendingStillRefusesTheCommit(final boolean discardRecordAfter) {
    final boolean[] fired = new boolean[1];
    listener = record -> {
      if (fired[0] || !record.asDocument().getTypeName().equals("V"))
        return;
      fired[0] = true;
      try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k = 1")) {
        final MutableDocument other = rs.next().getElement().orElseThrow().modify();
        other.set("note", "x");
        assertThat(((DatabaseInternal) database).updateRecordWithEventsNoLock(other, discardRecordAfter)).isTrue();
      }
    };
    database.getEvents().registerListener(listener);

    database.begin();
    // V.p is mandatory and never given: the scope deletes the provisional vertex and the statement fails
    assertStatementFails(() -> database.command("opencypher", "CREATE (v:V {k: 15}) RETURN v").close());
    assertThat(fired[0]).as("the listener wrote the property while the vertex was provisional").isTrue();

    assertCommitRefusedAndNothingStands();
  }

  /** The control of the write-now arm: an update that assigns no property is still given back with the statement. */
  @Test
  void aStatementWhoseOnlyWritesWereTakenBackStillCommits() {
    database.begin();
    assertStatementFails(() -> database.command("opencypher", "CREATE (v:V {k: 15}) RETURN v").close());
    database.commit();

    assertThat(count("V")).isZero();
  }

  private void assertCommitRefusedAndNothingStands() {
    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    if (database.isTransactionActive())
      database.rollback();

    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    assertThat(count("V")).isZero();
    assertThat(count("R")).isZero();
  }

  private static void assertStatementFails(final Runnable statement) {
    assertThat(catchThrowable(statement::run)).as("the statement failed").isNotNull();
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type)) {
      return rs.next().<Long>getProperty("c");
    }
  }
}
