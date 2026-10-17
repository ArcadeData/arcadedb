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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10190: a Cypher write statement run through the embedded API with no transaction open is ONE transaction, as it
 * is over HTTP and as Neo4j's implicit transaction is. The write steps used to begin and commit their own, row by row, so
 * a failing statement kept the writes made before the error, a MERGE over several pairs conflicted with its own earlier
 * rows, and a label round trip never returned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10190AutoCommitStatementAtomicityTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createEdgeType("R");
      database.getSchema().createEdgeType("S");
    });
  }

  @Test
  void aFailingCreateLeavesNothingBehind() {
    assertThatThrownBy(() -> drain("CREATE (:A {k: 1}) WITH 1 AS z RETURN 1 / (z - 1) AS e")).hasMessageContaining("/ by zero");
    assertThat(graph()).isEmpty();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void aSetFailingOnALaterRowUndoesTheEarlierRows() {
    setup("CREATE (:A {k: 0}), (:A {k: 1})");
    assertThatThrownBy(() -> drain("MATCH (n:A) WITH n ORDER BY n.k SET n.i = 10 / (n.k - 1)")).hasMessageContaining("/ by zero");
    assertThat(graph()).containsExactly("[A]{k=0}", "[A]{k=1}");
  }

  @Test
  void aDeleteRefusedOnALaterRowUndoesTheEarlierRows() {
    setup("CREATE (:A {k: 0}), (:A {k: 1})-[:R]->(:A {k: 2})");
    assertThatThrownBy(() -> drain("MATCH (n:A) DELETE n")).hasMessageContaining("still has relationships");
    assertThat(graph()).containsExactly("[A]{k=0}", "[A]{k=1}", "[A]{k=2}");
    assertThat(relationships()).isEqualTo(1);
  }

  @Test
  void aMergeOverPairsCreatesEveryRelationship() {
    setup("CREATE (:A {k: 0}), (:A {k: 1}), (:A {k: 2})");
    drain("MATCH (a:A), (b:A) WHERE a.k < b.k MERGE (a)-[:S]->(b)");
    assertThat(relationships()).isEqualTo(3);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS) // A HANG DETECTOR: THE STATEMENT USED TO NEVER RETURN
  void aLabelRoundTripReturnsAndKeepsTheLabel() {
    setup("CREATE (:A {k: 0})");
    final List<Result> rows = drain("MATCH (n:A) REMOVE n:A SET n:A RETURN labels(n) AS l");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<List<String>>getProperty("l")).containsExactly("A");
    assertThat(graph()).containsExactly("[A]{k=0}");
  }

  @Test
  void aSucceedingStatementIsCommittedAsOneTransaction() {
    setup("CREATE (:A {k: 0}), (:A {k: 1}), (:A {k: 2})");
    // THE SAME STATEMENT IN AN EXPLICIT TRANSACTION IS THE YARDSTICK: ONE PER ROW USED TO BE COMMITTED WITHOUT ONE
    final long beforeExplicit = writeTransactions();
    database.transaction(() -> drain("MATCH (n:A) SET n.j = n.k"));
    final long explicitCommits = writeTransactions() - beforeExplicit;

    final long before = writeTransactions();
    drain("MATCH (n:A) SET n.i = n.k * 10");
    assertThat(writeTransactions() - before).isEqualTo(explicitCommits);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(graph()).containsExactly("[A]{i=0, j=0, k=0}", "[A]{i=10, j=1, k=1}", "[A]{i=20, j=2, k=2}");
  }

  @Test
  void aStatementInsideAnOpenTransactionIsLeftToItsOwner() {
    setup("CREATE (:A {k: 0})");
    database.begin();
    try {
      drain("MATCH (n:A) SET n.i = 1");
      // THE CALLER'S TRANSACTION IS STILL OPEN AND STILL OWNS THE WRITE
      assertThat(database.isTransactionActive()).isTrue();
    } finally {
      database.rollback();
    }
    assertThat(graph()).containsExactly("[A]{k=0}");
  }

  @Test
  void callInTransactionsStillCommitsItsBatches() {
    setup("UNWIND range(1, 10) AS i CREATE (:A {k: i})");
    drain("MATCH (n:A) CALL { WITH n SET n.done = true } IN TRANSACTIONS OF 3 ROWS");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) WHERE n.done RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(10L);
    }
    assertThat(database.isTransactionActive()).isFalse();
  }

  private void setup(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }

  private List<Result> drain(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<String> graph() {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN labels(n) AS l, properties(n) AS p ORDER BY n.k")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("l") + "" + new TreeMap<>(r.<Map<String, Object>>getProperty("p")));
      }
    }
    return out;
  }

  private long relationships() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long writeTransactions() {
    return ((Number) database.getStats().get("writeTx")).longValue();
  }
}
