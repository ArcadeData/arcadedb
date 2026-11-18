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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.engine.ScanReadAheadBudget;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11639: {@code ArcadeFilterByTypeStep} wrapped the scan of a vertex type in a plain {@code Iterator}, so closing the
 * traversal reached nothing of it. What a type or bucket scan holds is not a file or a cursor but a batch it read ahead, taken
 * from the JVM-wide pool of read-ahead ({@link ScanReadAheadBudget}) and given back by {@code releaseReadAhead()}: a scan the
 * caller stopped before its end (a {@code limit()}) kept it until the garbage collector found the scan, which shrinks the
 * read-ahead of every other scan meanwhile. The edge branch reached the raw iterator through {@code CloseableIterator}, which
 * does nothing for the iterators of the engine either (they are not {@code AutoCloseable}).
 * <p>
 * The records are larger than a page, because a record that fits its page is a view of the cached page and copies nothing, so
 * a scan of those holds nothing to give back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11639FilterByTypeScanReleaseTest {
  private static final int    RECORDS = 4;
  // MORE THAN A PAGE (64KB), SO EACH RECORD IS A CHAIN OF CHUNKS ASSEMBLED INTO A BUFFER OF ITS OWN; RECORDS x PAYLOAD STAYS UNDER
  // THE BATCH BOUND (1MB), SO THE FIRST BATCH READS ALL OF THEM
  private static final String PAYLOAD = "x".repeat(200_000);

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-11639-scan-release");
    final BasicDatabase database = graph.getDatabase();
    database.getSchema().createVertexType("Seed", 1);
    database.getSchema().createVertexType("Big", 1).createProperty("k", Type.INTEGER);
    database.getSchema().createEdgeType("Link", 1);
    database.transaction(() -> {
      final MutableVertex anchor = database.newVertex("Seed").set("n", 1).save();
      for (int i = 0; i < RECORDS; i++) {
        final MutableVertex big = database.newVertex("Big").set("i", i).set("k", i).set("payload", PAYLOAD).save();
        anchor.newEdge("Link", big, "i", i, "payload", PAYLOAD);
      }
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void aVertexTypeScanStoppedHalfWayIsGivenBackWhenTheTraversalIsClosed() throws Exception {
    final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("Big");
    assertThat(filterByTypeSteps(graph.traversal().V().hasLabel("Big"))).isEqualTo(1);

    assertStoppedHalfWayScanIsGivenBackByClose(traversal);
  }

  @Test
  void aVertexBucketScanStoppedHalfWayIsGivenBackWhenTheTraversalIsClosed() throws Exception {
    final String bucketLabel = ArcadeLabels.BUCKET_PREFIX + graph.getDatabase().getSchema().getType("Big").getBuckets(false).get(0).getName();
    final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel(bucketLabel);
    assertThat(filterByTypeSteps(graph.traversal().V().hasLabel(bucketLabel))).isEqualTo(1);

    assertStoppedHalfWayScanIsGivenBackByClose(traversal);
  }

  @Test
  void anEdgeTypeScanStoppedHalfWayIsGivenBackWhenTheTraversalIsClosed() throws Exception {
    final GraphTraversal<Edge, Edge> traversal = graph.traversal().E().hasLabel("Link");
    assertThat(filterByTypeSteps(graph.traversal().E().hasLabel("Link"))).isEqualTo(1);

    assertStoppedHalfWayScanIsGivenBackByClose(traversal);
  }

  @Test
  void aScanReopenedMidTraversalIsGivenBackWhenTheTraversalIsClosed() throws Exception {
    // MID-TRAVERSAL V() OPENS ONE SCAN OF 'Big' PER INCOMING TRAVERSER: THE FIRST ONE IS READ TO THE END, THE SECOND ONE STOPS AFTER
    // ONE ELEMENT. (THE REWRITE ONLY HAPPENS FOR THE FIRST GraphStep FOLLOWED BY hasLabel() OF A TRAVERSAL, HENCE inject() IN FRONT)
    assertThat(filterByTypeSteps(graph.traversal().inject(1, 2, 3).V().hasLabel("Big"))).isEqualTo(1);
    final long before = ScanReadAheadBudget.getReservedBytes();
    final GraphTraversal<Integer, Vertex> traversal = graph.traversal().inject(1, 2, 3).V().hasLabel("Big");

    for (int i = 0; i < RECORDS; i++)
      assertThat(traversal.next()).isNotNull();
    assertThat(traversal.next()).as("the first element of the scan the second incoming traverser opened").isNotNull();
    assertThat(ScanReadAheadBudget.getReservedBytes()).as("the second scan holds the batch it read ahead").isGreaterThan(before);

    traversal.close();

    assertThat(ScanReadAheadBudget.getReservedBytes()).as("closing the traversal gives the read-ahead of its scan back")
        .isLessThanOrEqualTo(before);
  }

  @Test
  void aTypeScanOfTheIndexStepIsGivenBackWhenTheTraversalIsClosed() throws Exception {
    // THE SAME ADJACENT GAP, IN ArcadeFilterByIndexStep: THE INDEX IS DROPPED AFTER THE TRAVERSAL WAS COMPILED, SO ITS SCAN IS A SCAN OF THE TYPE
    graph.getDatabase().getSchema().getType("Big").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Big").has("k", P.gte(0)), ArcadeFilterByIndexStep.class)).isTrue();
    final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("Big").has("k", P.gte(0));
    traversal.asAdmin().applyStrategies();
    graph.getDatabase().getSchema().dropIndex("Big[k]");

    assertStoppedHalfWayScanIsGivenBackByClose(traversal);
  }

  @Test
  void aScanIsGivenBackWhenTheRunningTraversalIsReset() {
    final long before = ScanReadAheadBudget.getReservedBytes();
    final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("Big");

    assertThat(traversal.next()).isNotNull();
    assertThat(ScanReadAheadBudget.getReservedBytes()).as("the scan holds the batch it read ahead").isGreaterThan(before);

    traversal.asAdmin().reset();

    assertThat(ScanReadAheadBudget.getReservedBytes()).as("a reset drops the scan: its batch goes back to the pool")
        .isLessThanOrEqualTo(before);
  }

  @Test
  void aClonedRunningTraversalDoesNotCloseTheScanOfTheOriginal() throws Exception {
    final GraphTraversal<Vertex, Vertex> original = graph.traversal().V().hasLabel("Big");
    assertThat(original.next()).isNotNull();

    // AbstractStep.clone() RESETS THE CLONE WHILE IT STILL SHARES THE ITERATOR OF THE ORIGINAL: THE RESET MUST NOT CLOSE IT
    final GraphTraversal<Vertex, Vertex> closedClone = original.asAdmin().clone();
    closedClone.close();
    // A SEPARATE CLONE THAT IS NEVER CLOSED: ITERATING A CLOSED TRAVERSAL IS NOT WHAT IS UNDER TEST
    final GraphTraversal<Vertex, Vertex> clone = original.asAdmin().clone();

    assertThat(original.toList()).as("the rest of the scan of the original").hasSize(RECORDS - 1);
    original.close();
    assertThat(clone.toList()).as("the clone starts its own scan").hasSize(RECORDS);
  }

  @Test
  void aScanReadToTheEndHoldsNothingAndAnswersEveryElement() throws Exception {
    final long before = ScanReadAheadBudget.getReservedBytes();
    final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel("Big");

    final List<Vertex> vertices = traversal.toList();

    assertThat(vertices).hasSize(RECORDS);
    assertThat(ScanReadAheadBudget.getReservedBytes()).isLessThanOrEqualTo(before);
    traversal.close();
    // CLOSING TWICE IS HARMLESS
    traversal.close();
    assertThat(ScanReadAheadBudget.getReservedBytes()).isLessThanOrEqualTo(before);
  }

  /**
   * Reads one element of the traversal, which leaves the scan behind it holding a batch, closes the traversal and checks that the
   * batch went back to the pool without the garbage collector.
   */
  private static void assertStoppedHalfWayScanIsGivenBackByClose(final GraphTraversal<?, ?> traversal) throws Exception {
    final long before = ScanReadAheadBudget.getReservedBytes();

    assertThat(traversal.next()).isNotNull();
    // NOT VACUOUS: THE SCAN HOLDS THE BATCH IT READ AHEAD BEFORE IT IS CLOSED
    assertThat(ScanReadAheadBudget.getReservedBytes()).as("the scan holds the batch it read ahead").isGreaterThan(before);

    traversal.close();

    assertThat(ScanReadAheadBudget.getReservedBytes()).as("closing the traversal gives the read-ahead of its scan back")
        .isLessThanOrEqualTo(before);
  }

  private static int filterByTypeSteps(final GraphTraversal<?, ?> traversal) {
    int count = 0;
    for (final Step<?, ?> step : TraversalPlans.stepsOf(traversal))
      if (step instanceof ArcadeFilterByTypeStep)
        count++;
    return count;
  }
}
