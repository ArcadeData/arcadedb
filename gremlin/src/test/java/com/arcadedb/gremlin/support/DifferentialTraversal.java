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
package com.arcadedb.gremlin.support;

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs one query twice against the same database: once through the normal ArcadeGraph strategy set,
 * and once with {@link ArcadeTraversalStrategy} removed. The optimized and unoptimized paths are two
 * implementations of a single specification, so any disagreement is a defect by construction and no
 * hand-authored expected value is required.
 */
public class DifferentialTraversal {

  private final ArcadeGraph graph;

  private DifferentialTraversal(final ArcadeGraph graph) {
    this.graph = graph;
  }

  public static DifferentialTraversal on(final ArcadeGraph graph) {
    return new DifferentialTraversal(graph);
  }

  public List<Object> optimized(final Function<GraphTraversalSource, Traversal<?, ?>> query) {
    return drain(query.apply(graph.traversal()));
  }

  public List<Object> unoptimized(final Function<GraphTraversalSource, Traversal<?, ?>> query) {
    return drain(query.apply(graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class)));
  }

  public void assertSameResults(final Function<GraphTraversalSource, Traversal<?, ?>> query) {
    assertResultsMatch(optimized(query), unoptimized(query));
  }

  public void assertResultsMatch(final List<Object> optimizedResults, final List<Object> unoptimizedResults) {
    assertThat(optimizedResults)
        .as("optimized path returned different rows than the unoptimized path")
        .containsExactlyInAnyOrderElementsOf(unoptimizedResults);
  }

  private static List<Object> drain(final Traversal<?, ?> traversal) {
    final List<Object> results = new ArrayList<>();
    while (traversal.hasNext())
      results.add(traversal.next());
    return results;
  }
}
