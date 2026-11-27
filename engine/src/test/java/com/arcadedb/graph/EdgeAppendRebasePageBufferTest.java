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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A commit that loses the page-version race on the page of an edge chunk only because a concurrent commit appended to the same chunk
 * replays its appends on the committed page (the edge-append merge). The copy of the committed page it reloads is written into the array
 * of the page image the merge dropped, which the page manager counts: without that the merge copies a page it has just discarded.
 */
class EdgeAppendRebasePageBufferTest extends TestHelper {
  private static final long HANDOFF_TIMEOUT_SECONDS = 60;

  @Test
  void anEdgeAppendMergeCopiesTheCommittedPageIntoTheArrayItDropped() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Spoke", 1);
      database.getSchema().createEdgeType("LINK", 1);
    });
    final RID[] hubHolder = new RID[1];
    database.transaction(() -> hubHolder[0] = database.newVertex("Hub").save().getIdentity());
    final RID hub = hubHolder[0];
    // the hub has its edge chunk from here on: the next edges are appends to it
    database.transaction(() -> database.newVertex("Spoke").save().newEdge("LINK", hub));

    final CountDownLatch mainAppended = new CountDownLatch(1);
    final CountDownLatch competitorCommitted = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final Thread competitor = new Thread(() -> {
      try {
        if (!mainAppended.await(HANDOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS))
          throw new AssertionError("the main transaction never appended");
        database.transaction(() -> database.newVertex("Spoke").save().newEdge("LINK", hub), true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        competitorCommitted.countDown();
      }
    }, "competitor");
    competitor.start();

    final var before = ((DatabaseInternal) database).getPageManager().getStats();
    final long mergesBefore = before.edgeAppendMerges;
    final long slotMergesBefore = before.txPageSlotMerges;
    final long recycledBefore = before.txPageBuffersRecycled;

    database.begin();
    try {
      try {
        database.newVertex("Spoke").save().newEdge("LINK", hub);
      } finally {
        mainAppended.countDown();
      }
      if (!competitorCommitted.await(HANDOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        throw new AssertionError("the competitor never committed");
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      competitor.join(TimeUnit.SECONDS.toMillis(HANDOFF_TIMEOUT_SECONDS));
    }
    if (!errors.isEmpty())
      throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

    final var after = ((DatabaseInternal) database).getPageManager().getStats();
    assertThat(after.edgeAppendMerges - mergesBefore).as("edge-append merges").isEqualTo(1);
    // every page the commit rebased, the edge chunk's by the append merge and the others by the slot merge, was copied into the array of
    // the image it dropped
    assertThat(after.txPageBuffersRecycled - recycledBefore).as("page copies written into a dropped page's array")
        .isEqualTo((after.edgeAppendMerges - mergesBefore) + (after.txPageSlotMerges - slotMergesBefore));
    assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(3);
  }
}
