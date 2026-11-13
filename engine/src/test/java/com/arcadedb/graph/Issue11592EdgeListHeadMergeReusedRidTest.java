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
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11592: a transaction that created the first relationship to a vertex committed although a concurrent transaction deleted
 * that vertex, when a third one inserted a vertex that took the deleted one's RID before the commit. The edge-list head merge of
 * #11498 put the relationship on the new vertex, which did not exist when the relationship was created. The commit is refused
 * with a retryable conflict again, as it was before #11498 and as it still is with the slot merge off.
 */
class Issue11592EdgeListHeadMergeReusedRidTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("E", 1);
    });
  }

  @Test
  void aRelationshipToAVertexReplacedConcurrentlyUnderItsRidConflicts() throws Exception {
    assertRelationshipToReplacedVertexConflicts("new");
  }

  @Test
  void aRelationshipToAVertexReplacedConcurrentlyByAnIdenticalOneConflicts() throws Exception {
    // the new vertex has the same bytes as the one the transaction read: no comparison of the two images can tell them apart
    assertRelationshipToReplacedVertexConflicts("b");
  }

  @Test
  void aVertexReplacedBeforeTheTransactionBeganStillMergesItsEdgeListHead() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final RID c = newVertex("c");
    database.transaction(() -> vertex(b).delete());
    final RID replacement = newVertex("new");
    assertThat(replacement).isEqualTo(b);
    final long mergesBefore = stats().edgeListHeadMerges;

    // the transaction reads the vertex that holds the RID now, so its relationship is to that one and merges with a concurrent one
    final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(replacement)),
        () -> database.transaction(() -> vertex(c).newEdge("E", vertex(replacement))));

    assertThat(failure).isNull();
    assertThat(stats().edgeListHeadMerges).isGreaterThan(mergesBefore);
    database.transaction(() -> assertThat(neighbours(replacement, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(a, c));
  }

  private void assertRelationshipToReplacedVertexConflicts(final String replacementName) throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final AtomicReference<RID> replacement = new AtomicReference<>();

    // b gets its first incoming relationship in the first transaction; meanwhile b is deleted and a new vertex takes its RID
    final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b)), () -> {
      database.transaction(() -> vertex(b).delete());
      replacement.set(newVertex(replacementName));
    });

    assertThat(replacement.get()).as("the new vertex reuses the RID of the deleted one").isEqualTo(b);
    assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    database.transaction(() -> {
      assertThat(database.countType("E", false)).isZero();
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).isEmpty();
      assertThat(neighbours(b, Vertex.DIRECTION.IN)).isEmpty();
      assertThat(vertex(b).getString("name")).isEqualTo(replacementName);
    });
  }

  /**
   * Runs {@code first} in a transaction on a thread of its own, then {@code second} on this thread, which commits its own
   * transactions, then commits the first one.
   *
   * @return what the first transaction failed with, or null when it committed
   */
  private Throwable interleave(final Runnable first, final Runnable second) throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch secondCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread thread = new Thread(() -> {
      try {
        database.begin();
        try {
          first.run();
        } finally {
          firstStarted.countDown();
        }
        secondCommitted.await();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "first");
    thread.start();
    firstStarted.await();
    try {
      second.run();
    } finally {
      secondCommitted.countDown();
      thread.join();
    }
    return failure.get();
  }

  private RID newVertex(final String name) {
    return compute(() -> database.newVertex("V").set("name", name).save().getIdentity());
  }

  private <T> T compute(final Supplier<T> supplier) {
    final AtomicReference<T> result = new AtomicReference<>();
    database.transaction(() -> result.set(supplier.get()));
    return result.get();
  }

  private Vertex vertex(final RID rid) {
    return database.lookupByRID(rid, true).asVertex();
  }

  private List<RID> neighbours(final RID rid, final Vertex.DIRECTION direction) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex v : vertex(rid).getVertices(direction, "E"))
      result.add(v.getIdentity());
    return result;
  }

  private PageManager.PPageManagerStats stats() {
    return ((DatabaseInternal) database).getPageManager().getStats();
  }
}
