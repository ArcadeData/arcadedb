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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10251 (the Halloween problem): an openCypher write that moves the key of the index its MATCH walks must update
 * every matched record exactly once. The range scan used to meet the keys the statement had just written ahead of its
 * cursor, update the same records again and again, and end in an {@code OutOfMemoryError}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10251SetIndexedRangeHalloweenTest extends TestHelper {
  private static final int N = 300;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A").createProperty("k", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "A", "k");
    });
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:A {k: i})").close());
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD) // A HANG DETECTOR: THE STATEMENT USED TO NEVER RETURN
  void autoCommitSetOfTheRangeKeyRaisesEveryKeyOnce() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void setOfTheRangeKeyInsideATransactionRaisesEveryKeyOnce() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000").close());
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void lowerBoundedDescendingMoveRaisesEveryKeyOnce() {
    // THE KEYS MOVE DOWN, BEHIND AN ASCENDING CURSOR BUT AHEAD OF A DESCENDING ONE
    database.command("opencypher", "MATCH (n:A) WHERE n.k <= " + N + " WITH n ORDER BY n.k DESC SET n.k = n.k - 1000").close();
    assertRaisedOnce(-1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void boundedRangeReturnsEachRowOnce() {
    final List<Result> rows = drain("MATCH (n:A) WHERE n.k >= 1 AND n.k <= " + N + " SET n.k = n.k + 1 RETURN n.k AS k");
    assertThat(rows).hasSize(N);
    // EVERY KEY EXACTLY ONCE: A NODE SKIPPED AND ANOTHER RAISED TWICE WOULD KEEP THE COUNT AND THE BOUNDS
    final List<Integer> returned = new ArrayList<>();
    for (final Result row : rows)
      returned.add(row.<Number>getProperty("k").intValue());
    assertThat(returned).containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(2, N + 1).boxed().toList());
    final List<Integer> stored = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN n.k AS k")) {
      while (rs.hasNext())
        stored.add(rs.next().<Number>getProperty("k").intValue());
    }
    assertThat(stored).containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(2, N + 1).boxed().toList());
    assertRaisedOnce(1);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void mapMergeOfTheRangeKeyRaisesEveryKeyOnce() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n += {k: n.k + 1000}").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void setThroughAWithRaisesEveryKeyOnce() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 WITH n, n.k AS old SET n.k = old + 1000").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void mergeOnCreateSetOfTheRangeKeyRaisesEveryKeyOnce() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 MERGE (n)-[:R]->(m:B {id: n.k}) ON CREATE SET n.k = n.k + 1000").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void mergeOnMatchSetOfTheRangeKeyRaisesEveryKeyOnce() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 MERGE (m:A {k: n.k}) ON MATCH SET m.k = m.k + 1000").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void legacyPipelineRaisesEveryKeyOnce() {
    // AN UNWIND BEFORE THE MATCH TAKES THE ORDINARY PIPELINE, WHICH READS NO INDEX RANGE
    database.command("opencypher", "UNWIND [1] AS x MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000 * x").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void indexOrderedDescendingReturnRaisesEveryKeyOnceInOrder() {
    final List<Result> rows = drain("MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000 RETURN n.k AS k ORDER BY k DESC");
    assertThat(rows).hasSize(N);
    assertThat(rows.getFirst().<Number>getProperty("k").intValue()).isEqualTo(N + 1000);
    assertThat(rows.getLast().<Number>getProperty("k").intValue()).isEqualTo(1001);
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void indexOrderedScanWithNullKeysRaisesEveryKeyOnce() {
    // AN INDEX-ORDERED SCAN OF THE WHOLE LABEL ALSO READS THE VERTICES THAT HAVE NO KEY, AFTER THE INDEX
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 10) AS i CREATE (:A {other: i})").close());
    database.command("opencypher", "MATCH (n:A) WITH n ORDER BY n.k SET n.k = coalesce(n.k, 0) + 1000").close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N + 10);
      assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1000);
      assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 1000);
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void joinOnACompositePrefixRaisesEveryKeyOnce() {
    // THE INNER SIDE OF AN INDEX NESTED-LOOP JOIN SEEKS A PREFIX PER LEFT ROW
    database.transaction(() -> {
      database.getSchema().createVertexType("C").createProperty("g", Integer.class);
      database.getSchema().getType("C").createProperty("k", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "g", "k");
      database.getSchema().createVertexType("G").createProperty("g", Integer.class);
    });
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:C {g: i % 2, k: i})").close();
      database.command("opencypher", "CREATE (:G {g: 0}), (:G {g: 1})").close();
    });

    final String join = "MATCH (x:G), (n:C) WHERE n.g = x.g SET n.k = n.k + 1000";
    assertThat(explain(join)).contains("IndexNestedLoopJoin");
    database.command("opencypher", join).close();

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N);
      assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1001);
      assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 1000);
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void theAddressesReadFirstAreChargedToTheHeapBudgetAndReleased() {
    final long baseline = QueryHeapBudget.getReservedBytes();
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000").close();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    // 150,000 addresses take about 2MB, past a budget of 1MB: the statement fails, rolls back and gives the budget back
    database.transaction(() -> database.command("opencypher", "UNWIND range(" + (N + 1) + ", 150000) AS i CREATE (:A {k: i})").close());
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    try {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:A) WHERE n.k > 1000 SET n.k = n.k + 1000000").close())
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) WHERE n.k > 1000000 RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void setInsideACallSubqueryRaisesEveryKeyOnce() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 CALL { WITH n SET n.k = n.k + 1000 }").close();
    assertRaisedOnce(1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void thePrefixSeekGivesBackTheHeapItCharged() {
    database.transaction(() -> {
      database.getSchema().createVertexType("C").createProperty("g", Integer.class);
      database.getSchema().getType("C").createProperty("k", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "g", "k");
    });
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:C {g: 1, k: i})").close());
    final long baseline = QueryHeapBudget.getReservedBytes();
    database.command("opencypher", "MATCH (n:C {g: 1}) SET n.k = n.k + 1000").close();
    database.command("opencypher", "MATCH (n:C) WHERE n.g IN [1, 2] SET n.k = n.k + 1000").close();
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN min(n.k) AS lo, max(n.k) AS hi")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(2001);
      assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 2000);
    }
  }

  @Test
  void aWriteOfAnotherKeyKeepsStreamingAndWritesEveryNode() {
    database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.other = n.k * 2").close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) WHERE n.other = n.k * 2 RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(N);
    }
    assertRaisedOnce(0);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void createInTheScannedTypeCreatesOnePerMatchedRow() {
    // THE SAME PROBLEM WITHOUT AN INDEX: THE NEW NODES LAND IN THE TYPE THE MATCH SCANS
    database.command("opencypher", "MATCH (n:A) CREATE (:A {k: n.k + 1000})").close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L * N);
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void setOfTheTrailingKeyOfACompositePrefixSeekRaisesEveryKeyOnce() {
    // THE SEEK OF A PREFIX WALKS THE ENTRIES SHARING IT: RAISING THE TRAILING KEY MOVES EACH ONE AHEAD OF THE CURSOR
    database.transaction(() -> {
      database.getSchema().createVertexType("C").createProperty("g", Integer.class);
      database.getSchema().getType("C").createProperty("k", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "g", "k");
    });
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:C {g: 1, k: i})").close());

    database.command("opencypher", "MATCH (n:C {g: 1}) SET n.k = n.k + 1000").close();

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N);
      assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1001);
      assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 1000);
    }
  }

  @Test
  void theRangeIsReadFirstOnlyWhenTheStatementWritesItsKey() {
    assertThat(explain("MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000")).contains("range read first");
    assertThat(explain("MATCH (n:A) WHERE n.k > 0 SET n += {k: 1}")).contains("range read first");
    // A WRITE OF ANOTHER KEY KEEPS THE STREAMING SCAN
    assertThat(explain("MATCH (n:A) WHERE n.k > 0 SET n.other = 1")).doesNotContain("range read first");
    assertThat(explain("MATCH (n:A) WHERE n.k > 0 RETURN n")).doesNotContain("range read first");
  }

  private String explain(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + cypher)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private void assertRaisedOnce(final int delta) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N);
      assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1 + delta);
      assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + delta);
    }
    // THE INDEX AGREES WITH THE RECORDS: EVERY KEY IS FOUND, ONCE
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM A WHERE k >= " + (1 + delta) + " AND k <= " + (N + delta))) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(N);
    }
  }

  private List<Result> drain(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
