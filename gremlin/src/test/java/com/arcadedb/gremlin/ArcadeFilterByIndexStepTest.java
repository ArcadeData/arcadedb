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

import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the index-backed rewrite. Ages are 10, 20, 30, 40, 50 and every range predicate is probed
 * AT a stored boundary value, so an inverted inclusive/exclusive flag on the index cursor changes
 * the result set and the differential comparison catches it.
 */
class ArcadeFilterByIndexStepTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-filterbyindex");
    final var personType = graph.getDatabase().getSchema().createVertexType("Person");
    personType.createProperty("age", Type.INTEGER);
    personType.createProperty("city", Type.STRING);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "age");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "city");

    graph.getDatabase().transaction(() -> {
      final int[] ages = { 10, 20, 30, 40, 50 };
      final String[] cities = { "Rome", "Rome", "Milan", "Milan", "Turin" };
      for (int i = 0; i < ages.length; i++) {
        final ArcadeVertex v = graph.addVertex("Person");
        v.property("name", "P" + ages[i]);
        v.property("age", ages[i]);
        v.property("city", cities[i]);
      }
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void anIndexedEqualityInstallsTheIndexStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", 30), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", 30)))
        .isTrue();
  }

  @Test
  void equalityMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", 30).values("name"));
  }

  @Test
  void greaterThanAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(30)).values("name"));
  }

  @Test
  void greaterThanOrEqualAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gte(30)).values("name"));
  }

  @Test
  void lessThanAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lt(30)).values("name"));
  }

  @Test
  void lessThanOrEqualAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lte(30)).values("name"));
  }

  @Test
  void greaterThanTheMaximumReturnsNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(50)).values("name"));
  }

  @Test
  void lessThanTheMinimumReturnsNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lt(10)).values("name"));
  }

  @Test
  void twoIndexedPredicatesIntersectCorrectly() {
    // Exercises the multi-cursor intersection loop, where cursor 0 seeds the set and later cursors
    // retainAll. A wrong intersection silently widens or empties the result.
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Rome").has("age", 20).values("name"));
  }

  @Test
  void twoIndexedPredicatesWithAnEmptyIntersectionReturnNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Turin").has("age", 10).values("name"));
  }

  @Test
  void anIndexedAndAnUnindexedPredicateCombineCorrectly() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", 30).has("name", "P30").values("name"));
  }

  @Test
  void aRangeAndAnEqualityCombineCorrectly() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Milan").has("age", P.gte(30)).values("name"));
  }
}
