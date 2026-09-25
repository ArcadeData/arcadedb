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

import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8299: the index push-down opened its cursors, and drained them into a set, while the traversal was being
 * compiled. The set an indexed {@code has()} answered with was therefore frozen before the first step ran, and a write
 * made by an earlier step of the same traversal was invisible to it - while the unindexed form of the same traversal,
 * which reads when it executes, saw it.
 * <p>
 * Type {@code P} with {@code n} INDEXED and {@code m} NOT indexed, and one pre-existing vertex {@code n = m = 1}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8299IndexPushDownReadsAtExecutionTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-8299-lazy-index");
    final DocumentType type = graph.getDatabase().getSchema().createVertexType("P");
    type.createProperty("n", Type.INTEGER);
    type.createProperty("m", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");
    graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("P").set("n", 1).set("m", 1).save());
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void anIndexedHasSeesAVertexAddedEarlierInTheSameTraversal() {
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("P").has("n", 1), ArcadeFilterByIndexStep.class)).isTrue();

    final long indexed = count("g.addV('P').property('n',1).property('m',1).V().hasLabel('P').has('n',1).count()");
    final long unindexed = count("g.addV('P').property('n',1).property('m',1).V().hasLabel('P').has('m',1).count()");

    // THE FIRST STATEMENT SEES THE PRE-EXISTING VERTEX AND ITS OWN; THE SECOND SEES THOSE TWO AND ITS OWN
    assertThat(indexed).isEqualTo(2L);
    assertThat(unindexed).isEqualTo(3L);
    assertThat(count("g.V().hasLabel('P').has('n',1).count()")).isEqualTo(count("g.V().hasLabel('P').has('m',1).count()"));
  }

  @Test
  void anIndexedHasDoesNotSeeAVertexDroppedEarlierInTheSameTraversal() {
    graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("P").set("n", 2).set("m", 2).save());

    assertThat(count("g.V().hasLabel('P').has('m',2).drop().V().hasLabel('P').has('n',2).count()")).isEqualTo(0L);
  }

  @Test
  void theFluentApiReadsAtExecutionToo() {
    graph.getDatabase().transaction(() -> {
      final Traversal<?, Long> traversal = graph.traversal().V().hasLabel("P").has("n", P.gte(1)).count();
      // COMPILED BEFORE THE WRITE, EXECUTED AFTER IT
      traversal.asAdmin().applyStrategies();
      graph.getDatabase().newVertex("P").set("n", 5).set("m", 5).save();
      assertThat(traversal.next()).isEqualTo(2L);
    });
  }

  @Test
  void aRangeScanStopsAtTheLimit() {
    graph.getDatabase().transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        graph.getDatabase().newVertex("P").set("n", 10 + i).set("m", 0).save();
    });
    assertThat(graph.traversal().V().hasLabel("P").has("n", P.gt(0)).limit(3).toList()).hasSize(3);
    assertThat(graph.traversal().V().hasLabel("P").has("n", P.gt(0)).count().next()).isEqualTo(1_001L);
  }

  private long count(final String query) {
    final long[] result = new long[1];
    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin(query).execute()) {
        result[0] = ((Number) rs.next().getProperty("result")).longValue();
      }
    });
    return result[0];
  }
}
