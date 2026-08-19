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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct tests for the machinery behind a Cypher label write, so a regression in it is localised here rather than
 * inferred from a failing query: the replacement chain a twice-relabelled node builds, the edges and edge
 * properties carried over to the new record, and the row values - plain, nested, and path-shaped - redirected onto
 * it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LabelReplacementsTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-label-replacements").create();
    database.getSchema().getOrCreateVertexType("A");
    database.getSchema().getOrCreateVertexType("B");
    database.getSchema().getOrCreateVertexType("C");
    database.getSchema().getOrCreateEdgeType("E");
    database.getSchema().buildEdgeType().withName("Light").withLightweight(true).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void resolveFollowsAChainOfReplacements() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex original = database.newVertex("A").set("name", "n").save();

      final Vertex once = replacements.replace(original, "B");
      final Vertex twice = replacements.replace(once, "C");

      assertThat(replacements.resolve(original)).isSameAs(twice);
      assertThat(replacements.resolve(once)).isSameAs(twice);
      assertThat(replacements.resolve(twice)).isSameAs(twice);
      assertThat(twice.getTypeName()).isEqualTo("C");
      assertThat(twice.<String>get("name")).isEqualTo("n");
    });
  }

  @Test
  void resolveLeavesAnUntouchedVertexAlone() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex untouched = database.newVertex("A").save();

      assertThat(replacements.isEmpty()).isTrue();
      assertThat(replacements.resolve(untouched)).isSameAs(untouched);
      assertThat(replacements.resolve(null)).isNull();
    });
  }

  @Test
  void replaceCarriesEdgesAndTheirPropertiesOver() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex target = database.newVertex("A").set("name", "target").save();
      final MutableVertex other = database.newVertex("A").set("name", "other").save();
      target.newEdge("E", other, "w", 7);
      other.newEdge("E", target, "w", 9);
      target.newEdge("E", target, "w", 11);
      target.newEdge("Light", other);

      final Vertex replacement = replacements.replace(target, "B");

      assertThat(replacement.getTypeName()).isEqualTo("B");
      assertThat(edgeWeights(replacement, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(7, 11, null);
      assertThat(edgeWeights(replacement, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(9, 11);
      // The self-loop is one edge, seen once from each side, not two.
      assertThat(replacement.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(2);
    });
  }

  @Test
  void redirectRewritesPlainNestedAndPathShapedRowValues() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex start = database.newVertex("A").set("name", "start").save();
      final MutableVertex end = database.newVertex("A").set("name", "end").save();
      final Edge edge = start.newEdge("E", end, "w", 1);

      final TraversalPath path = new TraversalPath(start);
      path.addStep(edge, end);

      final ResultInternal row = new ResultInternal();
      row.setProperty("n", start);
      row.setProperty("untouched", end);
      row.setProperty("r", edge);
      row.setProperty("p", path);
      row.setProperty("collected", List.of(start, end));
      row.setProperty("mapped", Map.of("k", start));
      row.setProperty("scalar", 42);

      final Vertex replacement = replacements.replace(start, "B");
      replacements.redirect(row);

      assertThat(row.<Vertex>getProperty("n")).isSameAs(replacement);
      assertThat(row.<Vertex>getProperty("untouched")).isSameAs(end);
      assertThat(row.<Edge>getProperty("r").getIdentity()).isNotEqualTo(edge.getIdentity());
      assertThat(row.<Edge>getProperty("r").<Integer>get("w")).isEqualTo(1);
      assertThat(row.<Integer>getProperty("scalar")).isEqualTo(42);

      final TraversalPath redirected = row.getProperty("p");
      assertThat(redirected.getVertices().get(0)).isSameAs(replacement);
      assertThat(redirected.getVertices().get(1)).isSameAs(end);
      assertThat(redirected.getEdges().get(0).getIdentity()).isEqualTo(row.<Edge>getProperty("r").getIdentity());

      assertThat(row.<List<Vertex>>getProperty("collected").get(0)).isSameAs(replacement);
      assertThat(row.<List<Vertex>>getProperty("collected").get(1)).isSameAs(end);
      assertThat(row.<Map<String, Vertex>>getProperty("mapped").get("k")).isSameAs(replacement);
    });
  }

  @Test
  void redirectRewritesALightweightEdgeOntoTheReplacement() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex start = database.newVertex("A").set("name", "start").save();
      final MutableVertex end = database.newVertex("A").set("name", "end").save();
      final Edge light = start.newEdge("Light", end);

      final ResultInternal row = new ResultInternal();
      row.setProperty("r", light);

      final Vertex replacement = replacements.replace(start, "B");
      replacements.redirect(row);

      // A lightweight edge has no record to address, but its identity is the (type, out, in) triple - so the one
      // hanging off the replacement is a different edge, and the row has to be pointed at it.
      final Edge redirected = row.getProperty("r");
      assertThat(redirected).isNotSameAs(light);
      assertThat(redirected.getOut()).isEqualTo(replacement.getIdentity());
      assertThat(redirected.getIn()).isEqualTo(end.getIdentity());
      assertThat(light.getOut()).isNotEqualTo(replacement.getIdentity());
    });
  }

  @Test
  void redirectLeavesARowWithNothingToRewriteUntouched() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex vertex = database.newVertex("A").save();
      final List<Vertex> collected = List.of(vertex);

      final ResultInternal row = new ResultInternal();
      row.setProperty("n", vertex);
      row.setProperty("collected", collected);

      replacements.redirect(row);

      assertThat(row.<Vertex>getProperty("n")).isSameAs(vertex);
      // Nothing inside moved, so the list is the same instance rather than a rebuilt copy.
      assertThat(row.<List<Vertex>>getProperty("collected")).isSameAs(collected);
    });
  }

  @Test
  void restoreDiscardsReplacementsRecordedAfterTheSnapshot() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex first = database.newVertex("A").set("name", "first").save();
      final MutableVertex second = database.newVertex("A").set("name", "second").save();

      final Vertex firstReplacement = replacements.replace(first, "B");
      final LabelReplacements.Snapshot snapshot = replacements.copy();

      // Simulates issue #6367's finding: a failed, rolled-back MergeStep attempt records a replacement
      // (here, of `second`) that the retried attempt must not see - the vertex it points at was rolled
      // back along with everything else that attempt wrote.
      replacements.replace(second, "C");
      assertThat(replacements.resolve(second)).isNotSameAs(second);

      replacements.restore(snapshot);

      // The entry recorded before the snapshot survives the restore...
      assertThat(replacements.resolve(first)).isSameAs(firstReplacement);
      // ...but the one recorded after it is gone, exactly as if that attempt had never run.
      assertThat(replacements.resolve(second)).isSameAs(second);
    });
  }

  private static List<Object> edgeWeights(final Vertex vertex, final Vertex.DIRECTION direction) {
    final List<Object> weights = new ArrayList<>();
    for (final Edge edge : vertex.getEdges(direction))
      weights.add(edge.get("w"));
    return weights;
  }
}
