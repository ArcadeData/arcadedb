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
import com.arcadedb.serializer.json.JSONArray;
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
 * Regression test for issue #8568: {@code EdgeLinkedList.count()} walked the chunk chain with no guard against a chunk
 * whose previous pointer names itself, so on that corruption - which every other walker survives - a degree query
 * never returned. Every walk of the class now hops through one guarded helper; {@code toJSON()}, the removal walks and
 * {@code RIDIteratorFilter} were unguarded too.
 * <p>
 * A regression must come back as a red test rather than a hung build, so each walk runs on a daemon thread and the
 * test waits for it with a bound far above what the walk needs: it is a hang detector, not a latency bound.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8568EdgeListCountSelfLoopTest extends TestHelper {
  private static final String VERTEX_TYPE = "Issue8568Node";
  private static final String EDGE_TYPE   = "Issue8568Link";
  private static final int    DEGREE      = 50;
  private static final int    CAP         = 8 * DEGREE;

  /**
   * Every test plants a self-referencing chunk on purpose, so the teardown integrity check would only re-assert it.
   */
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
  void countEndsOnASelfReferencingChunk() throws Exception {
    final RID hub = createHubWithSelfLoopHead();

    final Long total = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count()));
    assertThat(total).isBetween(1L, (long) DEGREE);

    final Long filtered = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count(EDGE_TYPE)));
    assertThat(filtered).isEqualTo(total);

    // The query-facing path: the vertex degree
    final Long degree = runBounded(() -> inTx(() -> hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)));
    assertThat(degree).isEqualTo(total);
  }

  @Test
  void toJSONEndsOnASelfReferencingChunk() throws Exception {
    final RID hub = createHubWithSelfLoopHead();

    final JSONArray json = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).toJSON()));
    // Its content is a separate defect (the entries are not serialized); what matters here is that the walk returns
    assertThat(json.length()).isLessThanOrEqualTo(DEGREE);
  }

  @Test
  void ridIteratorFilterEndsOnASelfReferencingChunk() throws Exception {
    final RID hub = createHubWithSelfLoopHead();

    final Integer walked = runBounded(() -> inTx(() -> {
      // Passing an edge type routes ridIterator() to RIDIteratorFilter, which hopped chunks without the guard
      final Iterator<RID> rids = edgeLinkedListFor(hub).ridIterator(EDGE_TYPE);
      int count = 0;
      while (count <= CAP && rids.hasNext()) {
        rids.next();
        ++count;
      }
      return count;
    }));
    assertThat(walked).isLessThanOrEqualTo(DEGREE);
  }

  @Test
  void removalWalksEndOnASelfReferencingChunk() throws Exception {
    final RID hub = createHubWithSelfLoopHead();
    final RID stranger = inTx(() -> database.newVertex(VERTEX_TYPE).save().getIdentity());

    // A vertex and an edge the list does not hold: the removal walks visit every chunk and must still terminate
    runBounded(() -> {
      database.transaction(() -> edgeLinkedListFor(hub).removeVertex(stranger));
      return null;
    });
    runBounded(() -> {
      database.transaction(() -> edgeLinkedListFor(hub).removeEdgeRID(new RID(0, 0)));
      return null;
    });
    final Boolean contains = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).containsVertex(stranger, null)));
    assertThat(contains).isFalse();
  }

  /**
   * A self-referencing chunk that is NOT the head and that a removal empties must not be deleted and relinked around:
   * the chunk in front of it would be left pointing at the deleted record.
   */
  @Test
  void emptiedSelfReferencingChunkIsNotDeletedAndRelinkedAround() throws Exception {
    final RID[] hub = new RID[1];
    final List<RID> spokes = new ArrayList<>();
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex target = hub[0].asVertex(true).modify();
      for (int i = 0; i < DEGREE; i++) {
        final MutableVertex spoke = database.newVertex(VERTEX_TYPE).set("i", i).save();
        spoke.newEdge(EDGE_TYPE, target);
        spokes.add(spoke.getIdentity());
      }
    });

    // Point the chunk BEHIND the head at itself
    final RID second = inTx(() -> {
      final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
      final RID behind = ((EdgeSegment) database.lookupByRID(head, true)).getPreviousRID();
      final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(behind, true);
      segment.setPrevious(segment);
      ((DatabaseInternal) database).updateRecord(segment);
      return behind;
    });
    assertThat(second).isNotNull();

    runBounded(() -> {
      database.transaction(() -> {
        for (final RID spoke : spokes)
          edgeLinkedListFor(hub[0]).removeVertex(spoke);
      });
      return null;
    });

    database.transaction(() -> {
      final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
      final RID behind = ((EdgeSegment) database.lookupByRID(head, true)).getPreviousRID();
      assertThat(behind).isEqualTo(second);
      assertThat(database.existsRecord(behind)).as("the head must not point at a deleted chunk").isTrue();
    });
  }

  private <T> T inTx(final Supplier<T> read) {
    final List<T> result = new ArrayList<>(1);
    database.transaction(() -> result.add(read.get()));
    return result.getFirst();
  }

  private <T> T runBounded(final Callable<T> walk) throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      final Thread thread = new Thread(r, "Issue8568-walk");
      thread.setDaemon(true);
      return thread;
    });
    try {
      final Future<T> future = executor.submit(walk);
      // Hang detector, not a latency bound: the walk covers a single chunk
      return future.get(60, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * One hub with {@code DEGREE} spokes, its IN head chunk pointing at itself.
   */
  private RID createHubWithSelfLoopHead() {
    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex target = hub[0].asVertex(true).modify();
      for (int i = 0; i < DEGREE; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
    });

    database.transaction(() -> {
      final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
      final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(head, true);
      segment.setPrevious(segment);
      ((DatabaseInternal) database).updateRecord(segment);
    });

    return hub[0];
  }

  private EdgeLinkedList edgeLinkedListFor(final RID hub) {
    return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
  }
}
