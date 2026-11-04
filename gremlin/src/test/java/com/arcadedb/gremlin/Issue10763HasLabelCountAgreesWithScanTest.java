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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10763: {@code g.V().hasLabel('bucket:<missing>').count()} raised a {@code SchemaException} while the same
 * {@code hasLabel()} without the {@code count()} answered no rows. The count step ({@link ArcadeCountGlobalStep}) and the scan step
 * ({@link ArcadeFilterByTypeStep}) resolved a label each in their own way, and only the scan step guarded the missing bucket.
 * Both now resolve it through {@link ArcadeLabels}, and this test states the contract over every shape of label, on both element
 * kinds: the counted answer is the listed one, which is the expected one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10763HasLabelCountAgreesWithScanTest {
  private static final String PATH = "./target/test-gremlin-10763-label-census";

  private ArcadeGraph graph;
  private Database    database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    database = (Database) graph.getDatabase();
    database.command("sql", "CREATE VERTEX TYPE Movie BUCKETS 2").close();
    database.command("sql", "CREATE VERTEX TYPE Film EXTENDS Movie").close();
    database.command("sql", "CREATE EDGE TYPE Acted BUCKETS 2").close();
    database.command("sql", "CREATE EDGE TYPE Light LIGHTWEIGHT").close();
    // A BUCKET NO TYPE OWNS
    database.getSchema().createBucket("Orphan");

    database.transaction(() -> {
      final Vertex m0 = graph.addVertex("bucket:Movie_0");
      graph.addVertex("bucket:Movie_0");
      graph.addVertex("bucket:Movie_0");
      final Vertex m1 = graph.addVertex("bucket:Movie_1");
      graph.addVertex("Film");
      m0.addEdge("bucket:Acted_0", m1);
      m1.addEdge("bucket:Acted_1", m0);
      m1.addEdge("bucket:Acted_1", m1);
      m0.addEdge("Light", m1);
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null) {
      if (graph.tx().isOpen())
        graph.tx().rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  void countAndScanAgreeForEveryShapeOfLabel() {
    // label -> { vertices matched, edges matched }
    final Map<String, long[]> expected = new LinkedHashMap<>();
    expected.put("Movie", new long[] { 5, 0 });             // POLYMORPHIC: 4 MOVIES + 1 FILM
    expected.put("Film", new long[] { 1, 0 });
    expected.put("bucket:Movie_0", new long[] { 3, 0 });
    expected.put("bucket:Movie_1", new long[] { 1, 0 });
    expected.put("Acted", new long[] { 0, 3 });             // WRONG KIND ON g.V()
    expected.put("Light", new long[] { 0, 1 });             // LIGHTWEIGHT: NO RECORD, ONE EDGE
    expected.put("bucket:Acted_0", new long[] { 0, 1 });    // WRONG-KIND BUCKET ON g.V()
    expected.put("bucket:Acted_1", new long[] { 0, 2 });
    expected.put("NoSuchType", new long[] { 0, 0 });
    expected.put("bucket:NoSuchBucket_0", new long[] { 0, 0 });
    expected.put("bucket:Orphan", new long[] { 0, 0 });
    expected.put("bucket:", new long[] { 0, 0 });

    final GraphTraversalSource g = graph.traversal();
    for (final Map.Entry<String, long[]> entry : expected.entrySet()) {
      final String label = entry.getKey();
      assertAgree(label, "g.V()", entry.getValue()[0], () -> g.V().hasLabel(label));
      assertAgree(label, "g.E()", entry.getValue()[1], () -> g.E().hasLabel(label));
    }
  }

  @Test
  void theReportedQueriesAnswerZeroThroughTheTextualEngine() {
    assertThat(countOf("g.V().hasLabel('bucket:NoSuchBucket_0').count()")).isZero();
    assertThat(countOf("g.E().hasLabel('bucket:NoSuchBucket_0').count()")).isZero();
    assertThat(countOf("g.V().hasLabel('bucket:Movie_0').count()")).isEqualTo(3L);
  }

  @Test
  void aBucketCreatedAfterTheCountIsCompiledIsCounted() {
    // THE COUNT STEP RESOLVES THE LABEL WHEN IT RUNS, NOT WHEN IT IS COMPILED (#9335): COMPILED WHILE THE BUCKET DOES NOT EXIST,
    // EXECUTED ONCE IT DOES
    final GraphTraversal<?, Long> counted = graph.traversal().V().hasLabel("bucket:Late_0").count();
    assertThat(stepsAfterStrategies(counted)).anyMatch(s -> s instanceof ArcadeCountGlobalStep);

    database.command("sql", "CREATE VERTEX TYPE Late").close();
    database.transaction(() -> graph.addVertex("bucket:Late_0"));

    assertThat(counted.next()).isEqualTo(1L);
  }

  @Test
  void aWriteToABucketThatDoesNotExistIsRefusedByName() {
    assertThatThrownBy(() -> database.transaction(() -> graph.addVertex("bucket:NoSuchBucket_0")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("NoSuchBucket_0");
    assertThatThrownBy(() -> database.transaction(() -> graph.addVertex("bucket:Orphan")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Orphan");
    assertThatThrownBy(() -> database.transaction(() -> {
      final Vertex v = graph.addVertex("Movie");
      v.addEdge("bucket:NoSuchBucket_0", v);
    })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("NoSuchBucket_0");
    assertThatThrownBy(() -> database.transaction(() -> {
      final Vertex v = graph.addVertex("Movie");
      v.addEdge("bucket:Movie_0", v);
    })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Movie");
    assertThatThrownBy(() -> database.transaction(() -> graph.addVertex("bucket:Acted_0")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Acted");

    // NOTHING WAS WRITTEN BY THE REFUSED CALLS
    assertThat(graph.traversal().V().count().next()).isEqualTo(5L);
    assertThat(graph.traversal().E().count().next()).isEqualTo(4L);
  }

  private void assertAgree(final String label, final String start, final long expected,
      final Supplier<GraphTraversal<?, ?>> traversal) {
    final String what = start + ".hasLabel('" + label + "')";

    final GraphTraversal<?, ?> listed = traversal.get();
    assertThat(stepsAfterStrategies(listed)).as(what + " is answered by the scan step")
        .anyMatch(s -> s instanceof ArcadeFilterByTypeStep);
    final List<?> rows = listed.toList();

    final GraphTraversal<?, Long> counted = traversal.get().count();
    assertThat(stepsAfterStrategies(counted)).as(what + ".count() is answered by the count step")
        .anyMatch(s -> s instanceof ArcadeCountGlobalStep);
    final Long count = counted.next();

    assertThat(rows).as(what).hasSize((int) expected);
    assertThat(count).as(what + ".count()").isEqualTo(expected);
  }

  private static List<Step> stepsAfterStrategies(final Traversal<?, ?> traversal) {
    final Traversal.Admin<?, ?> admin = traversal.asAdmin();
    if (!admin.isLocked())
      admin.applyStrategies();
    return admin.getSteps();
  }

  private long countOf(final String query) {
    final long[] result = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = graph.gremlin(query).execute()) {
        result[0] = ((Number) rs.next().getProperty("result")).longValue();
      }
    });
    return result[0];
  }
}
