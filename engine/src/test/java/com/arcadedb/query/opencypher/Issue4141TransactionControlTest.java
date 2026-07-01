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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4141 (ISO/IEC 39075 GQL, section 2 - Infrastructure & Environment): transaction control must be
 * available <em>within the query language</em>, not only through the driver/embedded API. This covers the
 * three standard GQL statements:
 * <ul>
 *   <li>{@code START TRANSACTION}</li>
 *   <li>{@code COMMIT}</li>
 *   <li>{@code ROLLBACK}</li>
 * </ul>
 * A transaction opened with {@code START TRANSACTION} stays open across separate {@code command()} calls
 * (write steps reuse the active transaction instead of opening/committing their own), and is finalized only
 * by an explicit {@code COMMIT}/{@code ROLLBACK} - mirroring the SQL BEGIN/COMMIT/ROLLBACK semantics.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4141TransactionControlTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue4141TransactionControl");
    if (factory.exists())
      factory.open().drop(); // defend against a leftover db from a previously interrupted run
    database = factory.create();
    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      // A test may intentionally leave an explicit transaction open (START TRANSACTION without COMMIT);
      // roll it back so the database can be dropped.
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  /** Runs a fire-and-forget Cypher command, closing the ResultSet to release it. */
  private void exec(final String cypher) {
    try (final ResultSet ignored = database.command("opencypher", cypher)) {
      // no rows to consume
    }
  }

  // ---- COMMIT persists the work done since START TRANSACTION -----------------------------------

  @Test
  void startTransactionThenCommitPersistsAcrossCommands() {
    exec("START TRANSACTION");
    exec("CREATE (p:Person {name: 'Alice'})");
    exec("CREATE (p:Person {name: 'Bob'})");
    exec("COMMIT");

    // Both vertices survive because the explicit transaction was committed.
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS cnt")) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    }
  }

  // ---- ROLLBACK discards the work done since START TRANSACTION ---------------------------------

  @Test
  void startTransactionThenRollbackDiscardsAcrossCommands() {
    exec("START TRANSACTION");
    exec("CREATE (p:Person {name: 'Charlie'})");
    exec("ROLLBACK");

    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS cnt")) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
    }
  }

  // ---- Statements report the operation performed ----------------------------------------------

  @Test
  void transactionStatementsReportOperation() {
    try (final ResultSet rs = database.command("opencypher", "START TRANSACTION")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("begin");
    }
    try (final ResultSet rs = database.command("opencypher", "COMMIT")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("commit");
    }
    exec("START TRANSACTION");
    try (final ResultSet rs = database.command("opencypher", "ROLLBACK")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("rollback");
    }
  }

  // ---- Uncommitted work is visible within the same explicit transaction ------------------------

  @Test
  void uncommittedWorkIsVisibleWithinTheSameTransaction() {
    exec("START TRANSACTION");
    exec("CREATE (p:Person {name: 'Dave'})");

    // Visible to a read issued on the same thread/transaction before COMMIT.
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name: 'Dave'}) RETURN count(p) AS cnt")) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(1L);
    }
    exec("ROLLBACK");
  }

  // ---- Nested transactions: a second START TRANSACTION opens a nested tx -----------------------

  @Test
  void doubleStartTransactionOpensNestedTransaction() {
    // Same behavior as SQL BEGIN: a second START TRANSACTION does not error, it nests. The following
    // COMMIT finalizes only the inner transaction, leaving the outer one still active.
    exec("START TRANSACTION");
    exec("START TRANSACTION");
    exec("CREATE (p:Person {name: 'Nina'})");
    exec("COMMIT");

    // The outer transaction is still open after committing the inner one.
    assertThat(database.isTransactionActive()).isTrue();

    // Finalize the outer transaction; the work is persisted only now.
    exec("COMMIT");
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name: 'Nina'}) RETURN count(p) AS cnt")) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(1L);
    }
  }

  // ---- COMMIT/ROLLBACK without an active transaction -------------------------------------------

  @Test
  void commitWithoutActiveTransactionFailsWithActionableMessage() {
    assertThatThrownBy(() -> database.command("opencypher", "COMMIT"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("No active transaction");
  }

  @Test
  void rollbackWithoutActiveTransactionIsANoOp() {
    // ROLLBACK is lenient: with no active transaction it succeeds as a no-op.
    try (final ResultSet rs = database.command("opencypher", "ROLLBACK")) {
      assertThat(rs.next().<String>getProperty("operation")).isEqualTo("rollback");
    }
  }

  // ---- ArcadeDB extension: optional ISOLATION level on START TRANSACTION -----------------------

  @Test
  void startTransactionWithIsolationLevelCommits() {
    exec("START TRANSACTION ISOLATION REPEATABLE_READ");
    // The level is applied to the active transaction (not the database default), matching SQL 'BEGIN ISOLATION'.
    assertThat(((DatabaseInternal) database).getTransaction().getIsolationLevel())
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    exec("CREATE (p:Person {name: 'Frank'})");
    exec("COMMIT");

    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name: 'Frank'}) RETURN count(p) AS cnt")) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(1L);
    }
  }

  @Test
  void startTransactionWithReadCommittedIsolation() {
    exec("START TRANSACTION ISOLATION READ_COMMITTED");
    assertThat(((DatabaseInternal) database).getTransaction().getIsolationLevel())
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    exec("ROLLBACK");
  }

  @Test
  void invalidIsolationLevelIsRejectedWithActionableMessage() {
    assertThatThrownBy(() -> database.command("opencypher", "START TRANSACTION ISOLATION BOGUS"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("READ_COMMITTED")
        .hasMessageContaining("REPEATABLE_READ");
  }

  // ---- GQL access mode (READ ONLY / READ WRITE) is intentionally unimplemented -----------------

  @Test
  void gqlAccessModeIsNotSupportedAndReportsAParseError() {
    // ArcadeDB's begin() has no read-only transaction mode, so the GQL access-mode clause is not parsed;
    // it must surface as an actionable parse error rather than being silently accepted/ignored. The failure
    // is raised while parsing, before begin() runs, so no transaction is opened.
    assertThatThrownBy(() -> database.command("opencypher", "START TRANSACTION READ ONLY"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("READ");
  }

  // ---- Backward compatibility: commit/rollback/transaction remain valid identifiers ------------

  @Test
  void keywordsRemainUsableAsIdentifiers() {
    database.transaction(() ->
        exec("CREATE (p:Person {name: 'Eve', commit: 1, rollback: 2, transaction: 3, isolation: 4})"));

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name: 'Eve'}) RETURN p.commit AS commit, p.rollback AS rollback, p.transaction AS transaction, p.isolation AS isolation")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("commit").intValue()).isEqualTo(1);
      assertThat(r.<Number>getProperty("rollback").intValue()).isEqualTo(2);
      assertThat(r.<Number>getProperty("transaction").intValue()).isEqualTo(3);
      assertThat(r.<Number>getProperty("isolation").intValue()).isEqualTo(4);
    }
  }
}
