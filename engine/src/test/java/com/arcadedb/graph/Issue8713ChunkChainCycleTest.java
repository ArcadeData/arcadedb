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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8713: the chunk-chain walks stopped only on a chunk pointing at ITSELF, so a two-chunk cycle still hung
 * {@code count()}, {@code toJSON()}, the removal walks and the iterators. Each walk runs on a daemon thread so a
 * regression is a red test rather than a hung build; the bound is a hang detector, not a latency bound.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8713ChunkChainCycleTest extends TestHelper {
  private static final String VERTEX_TYPE = "Issue8713Node";
  private static final String EDGE_TYPE   = "Issue8713Link";
  private static final int    DEGREE      = 400;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
    });
  }

  @Test
  void walksEndOnATwoChunkCycle() throws Exception {
    final RID hub = createHubWithTwoChunkCycle();
    final RID stranger = inTx(() -> database.newVertex(VERTEX_TYPE).save().getIdentity());

    assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count()))).isPositive();
    assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count(EDGE_TYPE)))).isPositive();
    assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).toJSON()))).isNotNull();
    assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).containsVertex(stranger, null)))).isFalse();
    assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).containsEdge(new RID(0, 0))))).isFalse();

    runBounded(() -> {
      database.transaction(() -> edgeLinkedListFor(hub).removeVertex(stranger));
      return null;
    });
    runBounded(() -> {
      database.transaction(() -> edgeLinkedListFor(hub).removeEdgeRID(new RID(0, 0)));
      return null;
    });
  }

  /**
   * A walk that deletes what it visits must not meet a chunk it already deleted: with the A to B to A cycle it would
   * resolve A again after deleting it.
   */
  @Test
  void deleteAllSurvivesATwoChunkCycle() throws Exception {
    final RID hub = createHubWithTwoChunkCycle();

    runBounded(() -> {
      database.transaction(() -> edgeLinkedListFor(hub).deleteAll());
      return null;
    });
  }

  @Test
  void iteratorsEndOnATwoChunkCycle() throws Exception {
    final RID hub = createHubWithTwoChunkCycle();
    final int cap = 100 * DEGREE;

    for (final String[] types : new String[][] { {}, { EDGE_TYPE } }) {
      final Integer walked = runBounded(() -> inTx(() -> {
        final Iterator<RID> rids = edgeLinkedListFor(hub).ridIterator(types);
        int count = 0;
        while (count <= cap && rids.hasNext()) {
          rids.next();
          ++count;
        }
        return count;
      }));
      assertThat(walked).isLessThanOrEqualTo(cap);
    }

    final Long degree = runBounded(() -> inTx(() -> hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)));
    assertThat(degree).isPositive();
  }

  @Test
  void aLongHealthyChainIsNotTruncated() {
    final RID hub = createHub();
    final long[] counted = new long[1];
    database.transaction(() -> counted[0] = edgeLinkedListFor(hub).count());
    assertThat(counted[0]).isEqualTo(DEGREE);
  }

  private <T> T inTx(final Supplier<T> read) {
    final List<T> result = new ArrayList<>(1);
    database.transaction(() -> result.add(read.get()));
    return result.get(0);
  }

  private <T> T runBounded(final Callable<T> walk) throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      final Thread thread = new Thread(r, "Issue8713-walk");
      thread.setDaemon(true);
      return thread;
    });
    try {
      final Future<T> future = executor.submit(walk);
      return future.get(60, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private RID createHub() {
    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex target = hub[0].asVertex(true).modify();
      for (int i = 0; i < DEGREE; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
    });
    return hub[0];
  }

  /**
   * One hub whose IN chain has at least two chunks, the oldest pointing back at the head: head -> ... -> tail -> head.
   */
  private RID createHubWithTwoChunkCycle() {
    final RID hub = createHub();
    database.transaction(() -> {
      final RID head = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
      final MutableEdgeSegment headSegment = (MutableEdgeSegment) database.lookupByRID(head, true);
      final RID behind = headSegment.getPreviousRID();
      assertThat(behind).as("the chain needs at least two chunks").isNotNull();
      final MutableEdgeSegment tail = (MutableEdgeSegment) database.lookupByRID(behind, true);
      tail.setPrevious(headSegment);
      ((DatabaseInternal) database).updateRecord(tail);
    });
    return hub;
  }

  private EdgeLinkedList edgeLinkedListFor(final RID hub) {
    return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
  }
}
