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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6073: a write {@code CypherProcedure} (e.g. merge.node, merge.relationship,
 * apoc.refactor.mergeNodes) invoked via a top-level {@code CALL} with no transaction already open used to fail
 * with {@code TransactionException: Transaction not active}, because {@code CallStep.executeProcedure} was the
 * only write path in the openCypher engine that did not auto-commit the way {@code SetStep}/{@code DeleteStep}/
 * {@code MergeStep}/{@code RemoveStep}/{@code ForeachStep} do.
 */
class CallStepWriteProcedureAutoCommitTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-call-step-autocommit");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void mergeNodeAutoCommitsWithNoExplicitTransaction() {
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "CALL merge.node(['Person'], {name: 'John'}, {age: 30}) YIELD node RETURN node")) {
      final Result result = rs.next();
      assertThat(result.getVertex().get().get("age")).isEqualTo(30L);
    }

    assertThat(database.isTransactionActive()).isFalse();

    // Committed durably: a fresh, unrelated read outside any transaction sees it.
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = 'John'")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("age")).isEqualTo(30L);
    }
  }

  @Test
  void mergeRelationshipAutoCommitsWithNoExplicitTransaction() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    database.commit();

    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL merge.relationship(a, 'KNOWS', {}, {since: 2020}, b) YIELD rel RETURN rel")) {
      final Result result = rs.next();
      assertThat(result.getEdge().get().get("since")).isEqualTo(2020L);
    }

    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT expand(outE('KNOWS')) FROM Person WHERE name = 'A'")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /**
   * Exact reproduction from the issue body: a write procedure called via a single top-level statement with no
   * caller-managed transaction used to throw {@code TransactionException: Transaction not active} on the first
   * {@code ResultSet.next()} pull.
   */
  @Test
  void refactorMergeNodesAutoCommitsWithNoExplicitTransaction() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    database.newVertex("Person").set("name", "B").save();
    database.commit();
    final String aId = a.getIdentity().toString();

    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CALL apoc.refactor.mergeNodes([a,b], {}) YIELD node RETURN node")) {
      final String survivorId = rs.next().getVertex().get().getIdentity().toString();
      assertThat(survivorId).isEqualTo(aId);
    }

    assertThat(database.isTransactionActive()).isFalse();
  }

  /**
   * Explicit-transaction usage (the documented workaround for #6073, exercised by
   * {@code RefactorMergeNodesTest}) must keep working unchanged: auto-commit only fires when no
   * transaction is already open.
   */
  @Test
  void writeProcedureInsideExplicitTransactionIsUnaffected() {
    database.begin();
    try (final ResultSet rs = database.command("opencypher",
        "CALL merge.node(['Person'], {name: 'Explicit'}, {}) YIELD node RETURN node")) {
      rs.next();
    }
    assertThat(database.isTransactionActive()).isTrue();
    database.commit();

    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = 'Explicit'")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /**
   * A chained CALL (procedure invoked once per upstream row, e.g. via UNWIND) with no explicit transaction
   * auto-commits each row independently, the same way SetStep does for a per-row SET with no explicit transaction.
   */
  @Test
  void chainedCallAutoCommitsEachRowWithNoExplicitTransaction() {
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "UNWIND ['X', 'Y', 'Z'] AS n CALL merge.node(['Person'], {name: n}, {}) YIELD node RETURN node.name AS name")) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    }

    assertThat(database.isTransactionActive()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Person")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(3L);
    }
  }

  /**
   * A write procedure call that fails validation with no explicit transaction open must not leave a dangling
   * transaction behind - the auto-commit's rollback path has to fire on the error path too.
   */
  @Test
  void failedWriteProcedureCallDoesNotLeaveTransactionOpen() {
    assertThat(database.isTransactionActive()).isFalse();

    assertThatThrownBy(() -> database.command("opencypher",
        "CALL merge.node([], {}, {}) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandExecutionException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);

    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void refactorCloneNodesWithRelationshipsAutoCommitsWithNoExplicitTransaction() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.commit();

    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) YIELD output RETURN output")) {
      final Result result = rs.next();
      final Vertex clone = result.getProperty("output");
      assertThat(clone.get("name")).isEqualTo("A");
    }

    assertThat(database.isTransactionActive()).isFalse();
    try (final ResultSet check = database.query("sql", "SELECT count(*) AS c FROM Person")) {
      assertThat(check.next().<Long>getProperty("c")).isEqualTo(2L);
    }
  }

  /**
   * {@code do.when} takes a materially different path from the other write procedures: instead of mutating
   * directly, it dispatches its write sub-query to a nested {@code database.command(...)} call. Confirms that
   * indirection still works correctly with no explicit transaction wrapping the outer {@code CALL}.
   */
  @Test
  void doWhenAutoCommitsWriteSubQueryWithNoExplicitTransaction() {
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.do.when(true, \"CREATE (n:Person {name: 'Bob'}) RETURN n\", '', {}) YIELD value RETURN value")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    }

    assertThat(database.isTransactionActive()).isFalse();
    try (final ResultSet check = database.query("sql", "SELECT FROM Person WHERE name = 'Bob'")) {
      assertThat(check.hasNext()).isTrue();
    }
  }

  /**
   * The one branch combination in {@code executeProcedure} not otherwise exercised:
   * {@code OPTIONAL CALL} to a write procedure that fails validation, with no explicit transaction open.
   * The {@code catch (Exception e)} branch both rolls back (autoCommit's own guard) and returns
   * {@code null} instead of rethrowing (OPTIONAL's suppression) - must do both, not just one. Per OPTIONAL
   * semantics (matching {@code OPTIONAL MATCH}), the failed call still yields exactly one row with every
   * YIELD field {@code null}, rather than zero rows.
   */
  @Test
  void optionalCallToFailingWriteProcedureSuppressesErrorAndLeavesNoDanglingTransaction() {
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("opencypher",
        "OPTIONAL CALL merge.node([], {}, {}) YIELD node RETURN node")) {
      assertThat(rs.hasNext()).isTrue();
      final Object node = rs.next().getProperty("node");
      assertThat(node).isNull();
      assertThat(rs.hasNext()).isFalse();
    }

    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void unknownPropertiesPolicyStillThrowsWithNoExplicitTransaction() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.newVertex("Person").set("name", "B").save();
    database.commit();

    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'bogus'}) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandSemanticException.class);

    assertThat(database.isTransactionActive()).isFalse();
  }
}
