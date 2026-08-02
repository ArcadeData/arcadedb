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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5666: two overlapping {@link GraphBatch} instances on the same database
 * silently lose edges. Both batches see {@code getOutEdgesHeadChunk() == null} on a shared vertex because
 * the head pointer is only published by {@code close()}, so each creates its own segment chain and the
 * last one to close wins; the loser's edges stay on disk reachable from nothing.
 * <p>
 * The database therefore grants a single batch slot at a time. What is pinned here is not only the
 * rejection but every way the slot has to come back, because a slot left behind stops the database from
 * batching until the process restarts.
 */
class Issue5666ConcurrentGraphBatchTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");
    });
  }

  @Test
  void aSecondOverlappingBatchIsRejected() {
    try (final GraphBatch first = database.batch().build()) {
      assertThat(first).isNotNull();
      assertThatThrownBy(() -> database.batch().build())
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("already in progress");
    }
  }

  @Test
  void aSecondBatchOnAnotherThreadIsRejectedToo() throws Exception {
    final CountDownLatch firstIsOpen = new CountDownLatch(1);
    final CountDownLatch secondHasTried = new CountDownLatch(1);
    final AtomicReference<Throwable> rejection = new AtomicReference<>();

    final Thread contender = new Thread(() -> {
      try {
        firstIsOpen.await(30, TimeUnit.SECONDS);
        try (final GraphBatch ignored = database.batch().build()) {
          // reaching this point means the guard let two batches through
        }
      } catch (final Throwable t) {
        rejection.set(t);
      } finally {
        secondHasTried.countDown();
      }
    }, "issue5666-contender");
    contender.start();

    try (final GraphBatch first = database.batch().build()) {
      assertThat(first).isNotNull();
      firstIsOpen.countDown();
      assertThat(secondHasTried.await(30, TimeUnit.SECONDS)).isTrue();
    }
    contender.join(30_000);

    assertThat(rejection.get()).isInstanceOf(DatabaseOperationException.class);
  }

  /**
   * The documented single-writer pattern: one batch after another, each one connecting both directions.
   */
  @Test
  void sequentialBatchesBothSucceedAndConnectBothDirections() {
    final RID[] vertices = createVertices(4);

    try (final GraphBatch first = database.batch().build()) {
      first.newEdge(vertices[0], "KNOWS", vertices[1]);
    }

    // Would have thrown before the slot was released.
    try (final GraphBatch second = database.batch().build()) {
      second.newEdge(vertices[2], "KNOWS", vertices[3]);
    }

    assertOutAndInEdgeCounts(vertices, 2);
  }

  /**
   * A batch reached through an HA-style wrapper still hands its slot back. The wrapper only delegates
   * {@code batch()}, so a release routed through the instance the batch writes to would never reach the
   * flag and the second load on a replicated database would be refused.
   */
  @Test
  void aBatchTakenThroughAWrapperReleasesItsSlot() {
    final RID[] vertices = createVertices(4);

    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previousWrapper = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(delegatingWrapper(local));
    try {
      try (final GraphBatch first = local.batch().build()) {
        first.newEdge(vertices[0], "KNOWS", vertices[1]);
      }
      try (final GraphBatch second = local.batch().build()) {
        second.newEdge(vertices[2], "KNOWS", vertices[3]);
      }
    } finally {
      local.setWrappedDatabaseInstance(previousWrapper);
    }

    assertOutAndInEdgeCounts(vertices, 2);
  }

  /**
   * The slot is taken by build() and not by batch(): a builder rejected for a bad setting, or simply
   * dropped, must not lock the database out of batching.
   */
  @Test
  void anAbandonedBuilderDoesNotTakeTheSlot() {
    database.batch();
    database.batch().withBatchSize(250_000);
    assertThatThrownBy(() -> database.batch().withCommitRetryDelay(-1))
        .isInstanceOf(IllegalArgumentException.class);

    try (final GraphBatch batch = database.batch().build()) {
      assertThat(batch).isNotNull();
    }
  }

  /**
   * abandon() is what the streaming server paths use when they drop a failed batch on a thread that cannot
   * pay for a full close(). It has to free the slot, and a later close() must not free it a second time and
   * hand away a slot that meanwhile belongs to somebody else.
   */
  @Test
  void abandonReleasesTheSlotAndCloseDoesNotReleaseItTwice() {
    final GraphBatch abandoned = database.batch().build();
    assertThat(database.isReadYourWrites()).as("the batch relaxes read-your-writes for the load").isFalse();
    abandoned.abandon();
    assertThat(database.isReadYourWrites()).as("an abandoned batch must put read-your-writes back").isTrue();

    final GraphBatch next = database.batch().build();
    abandoned.close();

    assertThatThrownBy(() -> database.batch().build())
        .as("the double release must not give away the slot the second batch is holding")
        .isInstanceOf(DatabaseOperationException.class);

    next.close();
    try (final GraphBatch batch = database.batch().build()) {
      assertThat(batch).isNotNull();
    }
  }

  /**
   * A batch whose close() throws still hands the slot back. Reproduced with the duplicate-key failure of
   * issue #4113, which surfaces out of close() while flushing the trailing edge buffer.
   */
  @Test
  void aBatchWhoseCloseFailsStillReleasesTheSlot() {
    database.transaction(() -> {
      final EdgeType paired = database.getSchema().createEdgeType("PAIRED");
      paired.createProperty("from_id", Type.STRING);
      paired.createProperty("to_id", Type.STRING);
      database.getSchema().buildTypeIndex("PAIRED", new String[] { "from_id", "to_id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    });

    final RID[] vertices = createVertices(2);

    assertThatThrownBy(() -> {
      try (final GraphBatch failing = database.batch().withLightEdges(false).build()) {
        failing.newEdge(vertices[0], "PAIRED", vertices[1], "from_id", "a", "to_id", "b");
        failing.newEdge(vertices[0], "PAIRED", vertices[1], "from_id", "a", "to_id", "b");
      }
    }).isInstanceOf(DuplicatedKeyException.class);

    try (final GraphBatch afterFailure = database.batch().build()) {
      assertThat(afterFailure).isNotNull();
    }
  }

  private RID[] createVertices(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final MutableVertex v = database.newVertex("Person");
        v.set("id", i);
        v.save();
        rids[i] = v.getIdentity();
      }
    });
    return rids;
  }

  private void assertOutAndInEdgeCounts(final RID[] vertices, final int expected) {
    database.transaction(() -> {
      long out = 0;
      long in = 0;
      for (final RID rid : vertices) {
        final Vertex v = rid.asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
          out++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, "KNOWS"))
          in++;
      }
      assertThat(out).as("outgoing edges").isEqualTo(expected);
      assertThat(in).as("incoming edges").isEqualTo(expected);
    });
  }

  /**
   * Reproduces the topology the Raft HA plugin installs: a wrapper that becomes the database the batch
   * writes through, while {@code batch()} itself stays on the concrete instance underneath.
   */
  private static DatabaseInternal delegatingWrapper(final DatabaseInternal delegate) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }
}
