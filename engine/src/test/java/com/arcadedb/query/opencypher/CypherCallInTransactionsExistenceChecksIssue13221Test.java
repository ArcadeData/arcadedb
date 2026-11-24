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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13221: {@code CALL { ... } IN TRANSACTIONS} commits work partway through the statement - what the statement wrote before
 * the CALL (#13172) and each batch - while the deferred existence checks of #7945 ran only once, at the end of the statement. A
 * record left without its MANDATORY property was committed at one of those points; the check at the end then deleted it in a
 * transaction the statement's failure rolled back, so the record stayed, and the error still said "The record has been removed".
 * <p>
 * The checks now also run at every commit point the CALL introduces, as Neo4j validates existence constraints when an inner
 * transaction commits: before the commit of the statement's writes ahead of a batch (a violation fails the statement before
 * anything is committed), and before each batch's own commit (a violation is a failure of that batch, governed by ON ERROR).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherCallInTransactionsExistenceChecksIssue13221Test extends TestHelper {

  private static final String BATCHED = "UNWIND range(10, 19) AS i "//
      + "CALL (i) { CREATE (x:T {id: i}) SET x.m = CASE WHEN i = 15 THEN null ELSE 'y' END } IN TRANSACTIONS OF 3 ROWS";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.m STRING (mandatory true)");
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.name STRING (mandatory true)");
  }

  /** The repro of the issue: a batch-body record left incomplete is not committed, and ON ERROR FAIL ends the statement there. */
  @Test
  void anIncompleteRecordOfABatchIsNotCommittedUnderOnErrorFail() {
    assertThatThrownBy(() -> drain(BATCHED))
        .satisfies(e -> assertValidationFailure(e, "'T.m' is mandatory"));

    assertThat(incompleteTs()).isEmpty();
    // the batch (10, 11, 12) committed; the batch holding 15 failed and no later batch ran
    assertThat(ids()).containsExactly(10, 11, 12);
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void anIncompleteRecordOfABatchFailsOnlyThatBatchUnderOnErrorContinue() {
    final List<Result> rows = rows("UNWIND range(10, 19) AS i "//
        + "CALL (i) { CREATE (x:T {id: i}) SET x.m = CASE WHEN i = 15 THEN null ELSE 'y' END } "//
        + "IN TRANSACTIONS OF 3 ROWS ON ERROR CONTINUE REPORT STATUS AS s RETURN i, s");

    assertThat(incompleteTs()).isEmpty();
    assertThat(ids()).containsExactly(10, 11, 12, 16, 17, 18, 19);
    assertThat(rows).hasSize(10);
    for (final Result row : rows) {
      final int i = row.<Number>getProperty("i").intValue();
      final Map<String, Object> status = row.getProperty("s");
      final boolean failedBatch = i >= 13 && i <= 15;
      assertThat(status.get("committed")).isEqualTo(!failedBatch);
      if (failedBatch)
        assertThat((String) status.get("errorMessage")).contains("'T.m' is mandatory");
    }
  }

  @Test
  void anIncompleteRecordOfABatchStopsTheLaterBatchesUnderOnErrorBreak() {
    drain("UNWIND range(10, 19) AS i "//
        + "CALL (i) { CREATE (x:T {id: i}) SET x.m = CASE WHEN i = 15 THEN null ELSE 'y' END } IN TRANSACTIONS OF 3 ROWS ON ERROR BREAK");

    assertThat(incompleteTs()).isEmpty();
    assertThat(ids()).containsExactly(10, 11, 12);
  }

  /** A record the statement created before the CALL and never completed is refused before the commit ahead of the first batch. */
  @Test
  void anIncompleteRecordBeforeTheCallIsNotCommitted() {
    assertThatThrownBy(() -> drain("CREATE (p:Person {id: 1}) WITH p CALL (p) { CREATE (:T {id: 1, m: 'y'}) } IN TRANSACTIONS RETURN p"))
        .satisfies(e -> assertValidationFailure(e, "'Person.name' is mandatory"));

    assertThat(nodes("Person")).isEmpty();
    // no batch ran
    assertThat(nodes("T")).isEmpty();
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** The same inside the implicit transaction a server opens around a request (HTTP, gRPC, MCP). */
  @Test
  void anIncompleteRecordBeforeTheCallIsNotCommittedInsideAServerOpenedTransaction() {
    assertThatThrownBy(() -> database.transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      drain("CREATE (p:Person {id: 1}) WITH p CALL (p) { CREATE (:T {id: 1, m: 'y'}) } IN TRANSACTIONS RETURN p");
    })).satisfies(e -> assertValidationFailure(e, "'Person.name' is mandatory"));

    assertThat(nodes("Person")).isEmpty();
    assertThat(nodes("T")).isEmpty();
  }

  /** ON ERROR governs the batches, not the statement's own writes before the CALL. */
  @Test
  void anIncompleteRecordBeforeTheCallFailsTheStatementWhateverOnErrorSays() {
    assertThatThrownBy(() -> drain("CREATE (p:Person {id: 1}) WITH p UNWIND range(1, 3) AS i "//
        + "CALL (i) { CREATE (:T {id: i, m: 'y'}) } IN TRANSACTIONS OF 1 ROW ON ERROR CONTINUE"))
        .satisfies(e -> assertValidationFailure(e, "'Person.name' is mandatory"));

    assertThat(nodes("Person")).isEmpty();
    assertThat(nodes("T")).isEmpty();
  }

  /** What the deferral exists for still works on both sides of the CALL: the record completed before its commit point. */
  @Test
  void recordsCompletedBeforeTheirCommitPointAreKept() {
    drain("CREATE (p:Person {id: 1}) SET p.name = 'a' WITH p UNWIND range(1, 4) AS i "//
        + "CALL (i) { MERGE (x:T {id: i}) SET x.m = 'y' } IN TRANSACTIONS OF 2 ROWS");

    assertThat(count("MATCH (n:Person {name: 'a'}) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(ids()).containsExactly(1, 2, 3, 4);
  }

  /** A record created after the CALL is still checked at the end of the statement, and taken back for real. */
  @Test
  void anIncompleteRecordAfterTheCallIsStillTakenBack() {
    assertThatThrownBy(() -> drain("UNWIND range(1, 2) AS i CALL (i) { CREATE (:T {id: i, m: 'y'}) } IN TRANSACTIONS OF 1 ROW "//
        + "WITH count(*) AS c CREATE (:Person {id: c})"))
        .satisfies(e -> assertValidationFailure(e, "'Person.name' is mandatory"));

    assertThat(nodes("Person")).isEmpty();
    assertThat(ids()).containsExactly(1, 2);
  }

  /**
   * A record a batch created is validated when the batch commits, as in Neo4j: a clause after the CALL cannot complete it any
   * more, the batch has committed by then.
   */
  @Test
  void aBatchRecordIsCheckedWhenTheBatchCommitsNotAfterTheCall() {
    assertThatThrownBy(() -> drain("UNWIND [1] AS i CALL (i) { CREATE (x:T {id: i}) RETURN x } IN TRANSACTIONS SET x.m = 'y'"))
        .satisfies(e -> assertValidationFailure(e, "'T.m' is mandatory"));

    assertThat(nodes("T")).isEmpty();
  }

  /**
   * A nested {@code CALL { ... } IN TRANSACTIONS} commits its batches in a transaction nested in the enclosing batch's, which does
   * not see what the enclosing batch wrote: the inner commit point checks only the records of the inner batch, and the record the
   * enclosing batch left incomplete is still refused when that batch commits. It used to be forgotten at the inner commit point (the
   * inner transaction could not read it back), and the enclosing batch committed it incomplete.
   */
  @Test
  void anIncompleteRecordOfAnEnclosingBatchSurvivesTheCommitOfANestedBatch() {
    assertThatThrownBy(() -> drain("UNWIND [1] AS i CALL (i) { CREATE (p:Person {id: i}) WITH p, i "//
        + "CALL (i) { CREATE (:T {id: i, m: 'y'}) } IN TRANSACTIONS } IN TRANSACTIONS"))
        .satisfies(e -> assertValidationFailure(e, "'Person.name' is mandatory"));

    assertThat(nodes("Person")).isEmpty();
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** The same when the nested batch fails and ON ERROR CONTINUE undoes it: only the nested batch's records go with it. */
  @Test
  void anIncompleteRecordOfAnEnclosingBatchSurvivesTheRollbackOfANestedBatch() {
    assertThatThrownBy(() -> drain("UNWIND [1] AS i CALL (i) { CREATE (p:Person {id: i}) WITH p, i "//
        + "CALL (i) { CREATE (:T {id: i}) } IN TRANSACTIONS ON ERROR CONTINUE } IN TRANSACTIONS"))
        .satisfies(e -> assertValidationFailure(e, "'Person.name' is mandatory"));

    assertThat(nodes("Person")).isEmpty();
    assertThat(incompleteTs()).isEmpty();
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** A record of the enclosing batch completed after the nested CALL is kept: the nested commit point does not judge it. */
  @Test
  void aRecordOfAnEnclosingBatchCompletedAfterANestedBatchIsKept() {
    drain("UNWIND [1, 2] AS i CALL (i) { CREATE (p:Person {id: i}) WITH p, i "//
        + "CALL (i) { CREATE (:T {id: i, m: 'y'}) } IN TRANSACTIONS SET p.name = 'a' } IN TRANSACTIONS");

    assertThat(count("MATCH (n:Person {name: 'a'}) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(ids()).containsExactly(1, 2);
  }

  /**
   * The statement failed on the existence constraint, and the message says the record is gone, which the caller then checks: the
   * message used to claim it although the record had been committed.
   */
  private static void assertValidationFailure(final Throwable thrown, final String violation) {
    Throwable cause = thrown;
    while (cause != null && !(cause instanceof ValidationException))
      cause = cause.getCause();
    assertThat(cause).as("a ValidationException in the chain of %s", thrown).isNotNull();
    assertThat(cause.getMessage()).contains(violation).contains("has been removed");
  }

  private void drain(final String cypher) {
    try (final ResultSet resultSet = database.command("opencypher", cypher)) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  private List<Result> rows(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", cypher)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  private List<Integer> ids() {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (n:T) RETURN n.id AS id ORDER BY id")) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }

  private List<Integer> incompleteTs() {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (n:T) WHERE n.m IS NULL RETURN n.id AS id")) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }

  /**
   * Every node of the label, read by a scan: an unfiltered {@code count(n)} can be answered from the buckets' cached record counters
   * rather than from the records, which is not the ground truth a removal assertion needs.
   */
  private List<Object> nodes(final String label) {
    final List<Object> nodes = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (n:" + label + ") RETURN n")) {
      while (resultSet.hasNext())
        nodes.add(resultSet.next().getProperty("n"));
    }
    return nodes;
  }

  private long count(final String cypher) {
    try (final ResultSet resultSet = database.query("opencypher", cypher)) {
      return resultSet.next().<Number>getProperty("c").longValue();
    }
  }
}
