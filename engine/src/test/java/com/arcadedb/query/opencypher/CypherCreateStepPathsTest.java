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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The write loop of the CREATE step. A CREATE that follows another step (an UNWIND, a MATCH, the dummy row of a
 * standalone statement) takes its rows {@code arcadedb.opencypher.bulkCreateBatchSize} at a time, one transaction block
 * per batch; a batch size of 0 turns the batching off and every row is written on its own. Both loops must answer the
 * same rows, count the same statistics and leave the same graph, whatever the batch size is cut at. The PROFILE
 * breakdown of the step is checked by the invariants a clock cannot break: a phase is part of the step's own time.
 */
class CypherCreateStepPathsTest extends TestHelper {
  private static final Pattern TIME = Pattern.compile("(-?[\\d,.]+)(ns|μs|ms|s)\\b");

  private int savedBatchSize;

  @BeforeEach
  void rememberTheBatchSize() {
    savedBatchSize = (Integer) GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.getValue();
  }

  @AfterEach
  void restoreTheBatchSize() {
    GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(savedBatchSize);
  }

  @Test
  void everyBatchSizeCreatesTheSameRowsCountsThemOnceAndLeavesTheSameGraph() {
    for (final int batchSize : new int[] { 0, 1, 2, 3, 5, 20_000 }) {
      GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(batchSize);
      run("MATCH (n) DETACH DELETE n", new ArrayList<>());

      final List<String> rows = new ArrayList<>();
      final QueryStatistics stats = run(
          "UNWIND range(1, 5) AS i CREATE (n:P {i: i})-[r:R {w: i * 10}]->(m:Q) SET n.j = i * 2 "
              + "RETURN n.i AS i, n.j AS j, r.w AS w, labels(m) AS l ORDER BY i", rows);

      assertThat(rows).as("rows at batch size %d", batchSize).containsExactly(
          "i=1, j=2, w=10, l=[Q]", "i=2, j=4, w=20, l=[Q]", "i=3, j=6, w=30, l=[Q]", "i=4, j=8, w=40, l=[Q]", "i=5, j=10, w=50, l=[Q]");
      assertThat(stats.getNodesCreated()).as("nodes at batch size %d", batchSize).isEqualTo(10);
      assertThat(stats.getRelationshipsCreated()).as("relationships at batch size %d", batchSize).isEqualTo(5);
      assertThat(stats.getLabelsAdded()).as("labels at batch size %d", batchSize).isEqualTo(10);
      assertThat(stats.getPropertiesSet()).as("properties at batch size %d", batchSize).isEqualTo(15);

      assertThat(count("MATCH (n:P) RETURN count(n) AS c")).isEqualTo(5);
      assertThat(count("MATCH (:P)-[r:R]->(:Q) RETURN count(r) AS c")).isEqualTo(5);
    }
  }

  @Test
  void aCreateOnMatchedRowsKeepsTheBindingsOfItsInputAtEveryBatchSize() {
    run("UNWIND [1, 2, 3] AS k CREATE (:A {k: k})", new ArrayList<>());
    for (final int batchSize : new int[] { 0, 2, 20_000 }) {
      GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(batchSize);

      final List<String> rows = new ArrayList<>();
      final QueryStatistics stats = run("MATCH (a:A) CREATE (a)-[:R]->(b:B {k: a.k * 10}) RETURN a.k AS ak, b.k AS bk ORDER BY ak", rows);

      assertThat(rows).as("rows at batch size %d", batchSize).containsExactly("ak=1, bk=10", "ak=2, bk=20", "ak=3, bk=30");
      assertThat(stats.getNodesCreated()).isEqualTo(3);
      assertThat(stats.getRelationshipsCreated()).isEqualTo(3);
      run("MATCH (b:B) DETACH DELETE b", new ArrayList<>());
    }
  }

  @Test
  void aResultThatIsReadToItsEndHasNoMoreRowsAtEveryBatchSize() {
    for (final int batchSize : new int[] { 0, 2, 20_000 }) {
      GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(batchSize);
      database.begin();
      try (final ResultSet rs = database.command("opencypher", "UNWIND [1, 2, 3] AS i CREATE (n:P {i: i}) RETURN n.i AS i")) {
        for (int i = 1; i <= 3; i++) {
          assertThat(rs.hasNext()).as("row %d at batch size %d", i, batchSize).isTrue();
          assertThat(rs.next().<Number>getProperty("i").intValue()).isEqualTo(i);
        }
        assertThat(rs.hasNext()).as("batch size %d", batchSize).isFalse();
        assertThat(rs.hasNext()).as("asked again, batch size %d", batchSize).isFalse();
        assertThatThrownBy(rs::next).isInstanceOf(NoSuchElementException.class);
      } finally {
        database.rollback();
      }
    }
  }

  @Test
  void aVariableBoundToABareRidIsTheVertexForTheRestOfTheStatement() {
    run("CREATE (:A {k: 1})", new ArrayList<>());
    final RID rid = database.query("opencypher", "MATCH (a:A) RETURN a").next().<Vertex>getProperty("a").getIdentity();

    final List<Object> bound = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "WITH $rid AS a CREATE (a)-[:R]->(b:B) RETURN a, b", Map.of("rid", rid))) {
        final Result row = rs.next();
        bound.add(row.getProperty("a"));
        bound.add(row.getProperty("b"));
      }
    });

    assertThat(bound.get(0)).isInstanceOf(Vertex.class);
    assertThat(((Vertex) bound.get(0)).getIdentity()).isEqualTo(rid);
    assertThat(bound.get(1)).isInstanceOf(Vertex.class);
    assertThat(count("MATCH (:A)-[r:R]->(:B) RETURN count(r) AS c")).isEqualTo(1);
  }

  @Test
  void profileCountsTheVerticesTheEdgesAndTheBatchesAndEveryPhaseIsPartOfTheStep() {
    final String profile = profile("PROFILE UNWIND range(1, 5) AS i CREATE (a:P {i: i})-[:R {w: i}]->(b:Q)");

    assertThat(profile).contains("Vertices: 10 (").contains("Edges: 5 (").contains("Batches: 1 (tx commits: ~1), avg 15/batch");
    assertThat(profile).containsPattern("\\+ CREATE \\([\\d,]+μs, 5 rows\\)");
    assertPhasesAreParts(profile);
  }

  @Test
  void profileCutsTheRowsIntoTheBatchesTheSizeAsks() {
    GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(2);
    final String profile = profile("PROFILE UNWIND range(1, 5) AS i CREATE (:P {i: i})");

    assertThat(profile).contains("Vertices: 5 (").contains("Batches: 3 (tx commits: ~3), avg 1/batch").doesNotContain("Edges:");
    assertThat(profile).containsPattern("\\+ CREATE \\([\\d,]+μs, 5 rows\\)");
    assertPhasesAreParts(profile);
  }

  @Test
  void profileOfTheRowAtATimeLoopCountsTheRowsAndTheVerticesButHasNoBatches() {
    GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(0);
    final String profile = profile("PROFILE UNWIND range(1, 5) AS i CREATE (:P {i: i})");

    assertThat(profile).contains("Vertices: 5 (").doesNotContain("Batches:").doesNotContain("Edges:");
    assertThat(profile).containsPattern("\\+ CREATE \\([\\d,]+μs, 5 rows\\)");
    assertPhasesAreParts(profile);
  }

  /** The cost of the CREATE step covers every phase it times, and a phase never exceeds the percentage of the whole. */
  private static void assertPhasesAreParts(final String profile) {
    final Matcher header = Pattern.compile("\\+ CREATE \\(([\\d,]+)μs").matcher(profile);
    assertThat(header.find()).as(profile).isTrue();
    // The step's cost is printed in whole microseconds, truncated
    final long costNanos = (Long.parseLong(header.group(1).replace(",", "")) + 1) * 1_000;

    final long vertices = timeOf(profile, "Vertices: \\d+ \\(");
    assertThat(vertices).as("vertex time in %s", profile).isBetween(1L, costNanos + roundingOf(profile, "Vertices: \\d+ \\("));
    if (profile.contains("Edges:"))
      assertThat(timeOf(profile, "Edges: \\d+ \\(")).as("edge time in %s", profile).isBetween(1L, costNanos + roundingOf(profile, "Edges: \\d+ \\("));

    assertThat(profile).containsPattern("Property eval: [^\\n]*\\(\\d+\\.\\d%\\)").containsPattern("Save ops: [^\\n]*\\(\\d+\\.\\d%\\)");
    final Matcher percents = Pattern.compile("(?:Property eval|Save ops): [^\\n]*\\((\\d+\\.\\d)%\\)").matcher(profile);
    while (percents.find())
      assertThat(Double.parseDouble(percents.group(1))).as(profile).isBetween(0.0, 100.0);
  }

  /** The largest amount the printed time may be above the time measured: half of its last printed digit (formatTime prints ns, then 1, 1 and 2 decimals of us, ms and s). */
  private static long roundingOf(final String profile, final String prefix) {
    final Matcher matcher = Pattern.compile(prefix + TIME.pattern() + "\\)").matcher(profile);
    assertThat(matcher.find()).as("%s in %s", prefix, profile).isTrue();
    return switch (matcher.group(2)) {
      case "ns" -> 1;
      case "μs" -> 50;
      case "ms" -> 50_000;
      default -> 5_000_000;
    };
  }

  private static long timeOf(final String profile, final String prefix) {
    final Matcher matcher = Pattern.compile(prefix + TIME.pattern() + "\\)").matcher(profile);
    assertThat(matcher.find()).as("%s in %s", prefix, profile).isTrue();
    final double value = Double.parseDouble(matcher.group(1).replace(",", ""));
    return Math.round(switch (matcher.group(2)) {
      case "ns" -> value;
      case "μs" -> value * 1_000;
      case "ms" -> value * 1_000_000;
      default -> value * 1_000_000_000;
    });
  }

  private String profile(final String statement) {
    database.begin();
    try (final ResultSet rs = database.command("opencypher", statement)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    } finally {
      database.commit();
    }
  }

  private QueryStatistics run(final String statement, final List<String> rows) {
    final QueryStatistics[] stats = new QueryStatistics[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
        stats[0] = rs.getStatistics().orElseThrow();
      }
    });
    return stats[0];
  }

  private long count(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static String render(final Result row) {
    final StringBuilder sb = new StringBuilder();
    for (final String name : row.getPropertyNames()) {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append(name).append('=').append(row.<Object>getProperty(name));
    }
    return sb.toString();
  }
}
