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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10192: {@code INSERT INTO T FROM SELECT ... FROM T} read the rows it was inserting. An OR filter (planned as one scan per
 * branch) copied a row twice, because the second branch met the copy the first one had just inserted, and an index range
 * scan on the filtered property never ended, meeting its own new keys ahead of the cursor. The SELECT must be evaluated on the
 * state the statement started from.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10192InsertFromSelectSelfReadTest extends TestHelper {

  private void createType(final String name, final String extra) {
    database.command("sql", "CREATE DOCUMENT TYPE " + name + extra).close();
    database.command("sql", "CREATE PROPERTY " + name + ".id LONG").close();
    database.command("sql", "CREATE PROPERTY " + name + ".a INTEGER").close();
    database.command("sql", "CREATE PROPERTY " + name + ".b INTEGER").close();
  }

  private List<Long> ids(final String type) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").longValue());
    }
    return ids;
  }

  private long insert(final String statement) {
    final long[] count = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", statement)) {
        while (rs.hasNext()) {
          rs.next();
          count[0]++;
        }
      }
    });
    return count[0];
  }

  @Test
  void orFilterCopiesEachRowOnce() {
    createType("T", "");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1, a = 3, b = 4").close());

    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, a, b FROM T WHERE a = 3 OR b = 4")).isEqualTo(1);
    assertThat(ids("T")).containsExactly(1L, 101L);
  }

  @Test
  void plainFilterCopiesEachRowOnce() {
    createType("T", "");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1, a = 3, b = 4").close());

    // A WHERE WITH NO INDEX IS PUSHED INTO THE SCAN OF EACH BUCKET: THAT SCAN READS THE TARGET TOO
    try (final ResultSet rs = database.command("sql", "EXPLAIN INSERT INTO T FROM SELECT id + 100 AS id, a, b FROM T WHERE a = 3")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("MATERIALIZE");
    }
    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, a, b FROM T WHERE a = 3")).isEqualTo(1);
    assertThat(ids("T")).containsExactly(1L, 101L);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void indexRangeScanEnds() {
    createType("T", "");
    database.command("sql", "CREATE INDEX ON T (id) NOTUNIQUE").close();
    database.transaction(() -> {
      for (int i = 4; i <= 6; i++)
        database.command("sql", "INSERT INTO T SET id = ?, a = 1", i).close();
    });

    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, a FROM T WHERE id >= 4")).isEqualTo(3);
    assertThat(ids("T")).containsExactly(4L, 5L, 6L, 104L, 105L, 106L);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void indexRangeScanEndsOnManyRows() {
    createType("T", "");
    database.command("sql", "CREATE INDEX ON T (id) NOTUNIQUE").close();
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.command("sql", "INSERT INTO T SET id = ?, a = 1", i).close();
    });

    assertThat(insert("INSERT INTO T FROM SELECT id + 10 AS id, a FROM T WHERE id >= 0")).isEqualTo(2_000);
    assertThat(ids("T")).hasSize(4_000);
  }

  @Test
  void selectFromSuperTypeDoesNotSeeTheInsertedSubTypeRows() {
    createType("P", "");
    database.command("sql", "CREATE DOCUMENT TYPE T EXTENDS P").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO P SET id = 1, a = 3, b = 4").close();
      database.command("sql", "INSERT INTO T SET id = 2, a = 3, b = 4").close();
    });

    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, a, b FROM P WHERE a = 3 OR b = 4")).isEqualTo(2);
    assertThat(ids("P")).containsExactly(1L, 2L, 101L, 102L);
  }

  @Test
  void subQueryOnTheTargetSeesTheStartingState() {
    createType("T", "");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, a = 3, b = 4").close();
      database.command("sql", "INSERT INTO T SET id = 2, a = 5, b = 6").close();
    });

    // EVERY ROW WHOSE id IS ONE OF T's ids AT THE START: THE COPIES (id 101, 102) MUST NOT QUALIFY THEMSELVES
    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, a, b FROM T WHERE id IN (SELECT id FROM T) OR b = 4")).isEqualTo(2);
    assertThat(ids("T")).containsExactly(1L, 2L, 101L, 102L);
  }

  @Test
  void targetReadOnlyThroughASubQueryIsGuarded() {
    createType("A", "");
    createType("T", "");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO A SET id = 1, a = 3, b = 4").close();
      database.command("sql", "INSERT INTO T SET id = 7, a = 3, b = 4").close();
    });

    // T IS READ ONLY BY THE SUBQUERY OF THE PROJECTION: EVERY COPY SEES T AS IT WAS, ONE ROW
    try (final ResultSet rs = database.command("sql",
        "EXPLAIN INSERT INTO T FROM SELECT id + 100 AS id, (SELECT count(*) AS c FROM T)[0].c AS a FROM A")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("MATERIALIZE");
    }
    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, (SELECT count(*) AS c FROM T)[0].c AS a FROM A")).isEqualTo(1);
    assertThat(ids("T")).containsExactly(7L, 101L);
  }

  @Test
  void everySpellingOfASubQueryOnTheTargetTakesTheBarrier() {
    createType("A", "");
    createType("T", "");
    // THE SELECT ITSELF READS A ONLY: WHAT READS T IS A SUBQUERY, WRITTEN IN EACH POSITION AND SPELLING THE PARSER ACCEPTS
    final String[] selects = {//
        "SELECT id + 100 AS id, (\n   select count(*) AS c FROM T)[0].c AS a FROM A",//
        "SELECT id + 100 AS id, coalesce((SELECT max(id) AS m FROM T)[0].m, 0) AS a FROM A",//
        "SELECT id + 100 AS id FROM A WHERE id IN (SELECT id FROM T)",//
        "SELECT id + 100 AS id FROM A WHERE id NOT IN ( SELECT id FROM T WHERE a = 1 )",//
        "SELECT id + 100 AS id, $t.size() AS a FROM A LET $t = (SELECT FROM T)",//
        "SELECT id + 100 AS id, ifnull((SELECT max(id) AS m FROM T WHERE a = $parent.$current.a)[0].m, 1) AS a FROM A",//
        "SELECT id + 100 AS id FROM A LET $c = (SELECT FROM T WHERE id = $parent.$current.id) WHERE $c.size() = 0" };
    for (final String select : selects)
      try (final ResultSet rs = database.command("sql", "EXPLAIN INSERT INTO T FROM " + select)) {
        assertThat(rs.next().<String>getProperty("executionPlanAsString")).as(select).contains("MATERIALIZE");
      }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void orderByAndLimitSeeTheStartingState() {
    createType("T", "");
    database.command("sql", "CREATE INDEX ON T (id) NOTUNIQUE").close();
    database.transaction(() -> {
      for (int i = 4; i <= 6; i++)
        database.command("sql", "INSERT INTO T SET id = ?, a = 1", i).close();
    });

    assertThat(insert("INSERT INTO T FROM SELECT id + 100 AS id, a FROM T WHERE id >= 4 ORDER BY id DESC LIMIT 2")).isEqualTo(2);
    assertThat(ids("T")).containsExactly(4L, 5L, 6L, 105L, 106L);
  }

  @Test
  void bucketTargetIsGuardedToo() {
    createType("T", "");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1, a = 3, b = 4").close());
    final String bucket = database.getSchema().getType("T").getBuckets(false).getFirst().getName();

    assertThat(insert("INSERT INTO bucket:" + bucket + " FROM SELECT id + 100 AS id, a, b FROM T WHERE a = 3 OR b = 4")).isEqualTo(1);
    assertThat(ids("T")).containsExactly(1L, 101L);
  }

  /**
   * The barrier holds the rows of a self-copy on the heap: past the budget all queries share, the statement is refused with the
   * setting named, gives the heap back and inserts nothing, where it used to stream.
   */
  @Test
  void aSelfCopyPastTheHeapBudgetIsRefusedCleanly() {
    createType("T", "");
    database.command("sql", "CREATE PROPERTY T.pad STRING").close();
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++)
        database.command("sql", "INSERT INTO T SET id = ?, a = 1, pad = ?", i, "p".repeat(500)).close();
    });

    final long mb = 1024L * 1024L;
    final long baseline = QueryHeapBudget.getReservedBytes();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / mb + 16);
    final QueryHeapTracker others = new QueryHeapTracker();
    try {
      // EVERY OTHER QUERY TOGETHER LEAVES HALF A MEGABYTE: THE ~1.5 MB OF ROWS DO NOT FIT
      others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - mb / 2,
          "other queries");
      assertThatThrownBy(() -> insert("INSERT INTO T FROM SELECT id + 100000 AS id, a, pad FROM T"))
          .isInstanceOf(QueryHeapBudgetExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).as("the refused statement holds nothing")
          .isLessThanOrEqualTo(baseline + others.getReservedBytes());
    } finally {
      others.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }
    assertThat(ids("T")).as("nothing was inserted").hasSize(3_000);

    assertThat(insert("INSERT INTO T FROM SELECT id + 100000 AS id, a, pad FROM T")).isEqualTo(3_000);
    assertThat(ids("T")).hasSize(6_000);
  }

  /**
   * InsertExecutionPlanner.readsAny recognizes the steps that read records by class. A new source step it does not know would let
   * the SELECT read its own inserts again, silently: this fails first, so whoever adds one decides where it belongs.
   */
  @Test
  void everySourceStepIsKnownToTheSelfReadCheck() throws IOException {
    // READ THE TARGET'S BUCKETS: RECOGNIZED BY readsAny, OR (THE COMPOSITES) WALKED THROUGH THEIR SUB-STEPS
    final Set<String> handled = Set.of("FetchFromClusterExecutionStep", "ScanWithFilterStep", "FetchFromIndexStep",
        "FetchFromIndexValuesStep", "FetchFromIndexedFunctionStep", "FetchFromLightweightEdgeTypeStep", "FetchFromTimeSeriesStep",
        "FetchFromTypeExecutionStep", "FetchFromClustersExecutionStep", "FetchFromTypeWithFilterStep");
    // CANNOT MEET A ROW INSERTED BY THE STATEMENT: EXPLICIT RIDS, VARIABLES AND RESULT SETS COMPUTED BEFORE, THE SCHEMA, AND
    // THE EDGES OF A VERTEX, WHICH AN INSERT DOES NOT ADD
    final Set<String> unaffected = Set.of("FetchFromRidsStep", "FetchFromVariableStep", "FetchFromResultsetStep",
        "FetchEdgesFromVertexStep");

    final List<String> unknown = new ArrayList<>();
    try (final Stream<Path> files = Files.list(Path.of("src/main/java/com/arcadedb/query/sql/executor"))) {
      files.map(f -> f.getFileName().toString()).filter(n -> (n.startsWith("Fetch") || n.startsWith("Scan")) && n.endsWith("Step.java"))
          .map(n -> n.substring(0, n.length() - ".java".length()))
          .filter(n -> !handled.contains(n) && !unaffected.contains(n) && !n.startsWith("FetchFromSchema")).forEach(unknown::add);
    }
    assertThat(unknown).as("source steps InsertExecutionPlanner.readsAny does not know").isEmpty();
  }

  @Test
  void copyBetweenUnrelatedTypesStillStreams() {
    createType("A", "");
    createType("B", "");
    database.transaction(() -> database.command("sql", "INSERT INTO A SET id = 1, a = 3, b = 4").close());

    // NO BARRIER WHEN THE SELECT CANNOT SEE THE TARGET: THE ROWS FLOW STRAIGHT FROM THE SOURCE TO THE INSERT
    try (final ResultSet rs = database.command("sql", "EXPLAIN INSERT INTO B FROM SELECT id + 100 AS id, a, b FROM A WHERE a = 3 OR b = 4")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).doesNotContain("MATERIALIZE");
    }
    // A STRING LITERAL THAT READS LIKE A SUBQUERY IS NOT ONE
    try (final ResultSet rs = database.command("sql",
        "EXPLAIN INSERT INTO B FROM SELECT id + 100 AS id, a, b, '(select x from A)' AS s FROM A WHERE a = 3 OR b = 4")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).doesNotContain("MATERIALIZE");
    }
    try (final ResultSet rs = database.command("sql", "EXPLAIN INSERT INTO A FROM SELECT id + 100 AS id, a, b FROM A WHERE a = 3 OR b = 4")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("MATERIALIZE");
    }

    assertThat(insert("INSERT INTO B FROM SELECT id + 100 AS id, a, b FROM A WHERE a = 3 OR b = 4")).isEqualTo(1);
    assertThat(ids("A")).containsExactly(1L);
    assertThat(ids("B")).containsExactly(101L);
  }
}
