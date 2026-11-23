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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13172: the batches of a {@code CALL { ... } IN TRANSACTIONS} run in transactions of their own, which do not see what the
 * statement wrote before the CALL and has not committed. A label write before the CALL replaces the node by a new record, so the
 * body, pointed at that record, failed with "Record #1:2 not found". The statement's own (implicit) transaction is now committed
 * before each batch begins: IN TRANSACTIONS gives up the atomicity of the statement anyway, each batch committing on its own.
 */
class CypherCallInTransactionsOuterWritesIssue13172Test extends TestHelper {

  private static final String LEFT = "MATCH (a:Probe)-[:R]->(b:Probe) "//
      + "CALL (b) { RETURN 1 AS k } IN TRANSACTIONS OF 10 ROWS "//
      + "RETURN a.id AS a, b.id AS b, k";

  private static final String RIGHT = "MATCH (a:Probe)-[:R]->(b:Probe) "//
      + "FOREACH (n IN [b] | SET n:TmpReview) "//
      + "FOREACH (n IN [b] | REMOVE n:TmpReview) "//
      + "CALL (b) { "//
      + "  FOREACH (n IN [b] | SET n:TmpReview) "//
      + "  FOREACH (n IN [b] | REMOVE n:TmpReview) "//
      + "  RETURN 1 AS k "//
      + "} IN TRANSACTIONS OF 10 ROWS "//
      + "RETURN a.id AS a, b.id AS b, k";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:Probe {id: 1})-[:R]->(b:Probe {id: 2})"));
  }

  @Test
  void leftQueryReturnsTheRow() {
    assertSingleProbeRow(rows(LEFT));
  }

  @Test
  void rightQueryWithTemporaryLabelsReturnsTheSameRow() {
    assertSingleProbeRow(rows(RIGHT));
    assertGraphUnchanged();
  }

  @Test
  void rightQueryInsideAServerOpenedTransaction() {
    final List<Result> result = new ArrayList<>();
    // What the HTTP auto-commit wrapper does around every command it runs
    database.transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      result.addAll(rows(RIGHT));
    });
    assertSingleProbeRow(result);
    assertGraphUnchanged();
  }

  @Test
  void labelRoundTripBeforeTheCallThenLabelWriteInsideIt() {
    final List<Result> result = rows("MATCH (b) SET b:T REMOVE b:T WITH b CALL (b) { SET b:U } IN TRANSACTIONS RETURN b");
    assertThat(result).hasSize(2);
    for (final Result row : result) {
      final Vertex b = row.<Vertex>getProperty("b");
      assertThat(b.getType().getName()).isNotNull();
      assertThat(b.getType().instanceOf("U")).isTrue();
    }
    assertThat(count("MATCH (n:U) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (:U)-[r:R]->(:U) RETURN count(r) AS c")).isEqualTo(1L);
  }

  /**
   * The outer rows of a later batch are produced after the first batch committed, and what the clauses before the CALL write for
   * them sits in the statement's transaction again: it is committed before that batch too, not only before the first one.
   */
  @Test
  void outerWritesForALaterBatchAreCommittedBeforeIt() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(3, 6) AS i CREATE (:Probe {id: i})"));
    final List<Result> result = rows(
        "MATCH (b:Probe) SET b:T REMOVE b:T WITH b CALL (b) { SET b:U, b.done = true } IN TRANSACTIONS OF 1 ROW RETURN b.id AS id");
    assertThat(result).hasSize(6);
    assertThat(count("MATCH (n:U) WHERE n.done = true RETURN count(n) AS c")).isEqualTo(6L);
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isZero();
  }

  /**
   * The documented outcome of committing the statement's writes before the first batch: a batch failing under ON ERROR FAIL
   * fails the statement, the batches committed before it stand, and so does what the statement wrote before the CALL.
   */
  @Test
  void writesBeforeTheCallStandWhenALaterBatchFailsUnderOnErrorFail() {
    database.command("sql", "CREATE VERTEX TYPE Y");
    database.command("sql", "CREATE PROPERTY Y.i INTEGER");
    database.command("sql", "CREATE INDEX ON Y (i) UNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:Y {i: 3})"));

    assertThatThrownBy(() -> database.command("opencypher",
        "CREATE (:Pre {v: 1}) WITH 1 AS one UNWIND range(1, 5) AS i "//
            + "CALL (i) { CREATE (:Y {i: i}) } IN TRANSACTIONS OF 2 ROWS ON ERROR FAIL"));

    assertThat(count("MATCH (n:Pre) RETURN count(n) AS c")).isEqualTo(1L);
    // the prefilled i = 3 and the first batch, i = 1 and 2: the second batch (3, 4) failed, the third never ran
    assertThat(count("MATCH (n:Y) RETURN count(n) AS c")).isEqualTo(3L);
  }

  @Test
  void writesBeforeTheCallStandUnderOnErrorContinue() {
    database.command("sql", "CREATE VERTEX TYPE Y");
    database.command("sql", "CREATE PROPERTY Y.i INTEGER");
    database.command("sql", "CREATE INDEX ON Y (i) UNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:Y {i: 3})"));

    database.command("opencypher", "CREATE (:Pre {v: 1}) WITH 1 AS one UNWIND range(1, 5) AS i "//
        + "CALL (i) { CREATE (:Y {i: i}) } IN TRANSACTIONS OF 2 ROWS ON ERROR CONTINUE").close();

    assertThat(count("MATCH (n:Pre) RETURN count(n) AS c")).isEqualTo(1L);
    // batches (1, 2) and (5) committed, (3, 4) failed on the prefilled key
    assertThat(count("MATCH (n:Y) RETURN count(n) AS c")).isEqualTo(4L);
  }

  /** A write after the CALL still runs in a transaction, which the statement commits at its end. */
  @Test
  void writesAfterTheCallAreCommittedWithTheStatement() {
    database.command("opencypher",
        "MATCH (b:Probe {id: 2}) SET b:T REMOVE b:T WITH b CALL (b) { SET b.inner = 1 } IN TRANSACTIONS SET b.after = 2").close();
    assertThat(count("MATCH (n:Probe {id: 2}) WHERE n.inner = 1 AND n.after = 2 RETURN count(n) AS c")).isEqualTo(1L);
  }

  private void assertSingleProbeRow(final List<Result> result) {
    assertThat(result).hasSize(1);
    final Result row = result.getFirst();
    assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(1);
    assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(2);
    assertThat(row.<Number>getProperty("k").intValue()).isEqualTo(1);
  }

  private void assertGraphUnchanged() {
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH (:Probe {id: 1})-[r:R]->(:Probe {id: 2}) RETURN count(r) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (n:TmpReview) RETURN count(n) AS c")).isZero();
  }

  private List<Result> rows(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", cypher)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  private long count(final String cypher) {
    try (final ResultSet resultSet = database.query("opencypher", cypher)) {
      return resultSet.next().<Number>getProperty("c").longValue();
    }
  }
}
