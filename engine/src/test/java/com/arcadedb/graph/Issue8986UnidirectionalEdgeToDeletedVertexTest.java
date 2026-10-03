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
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8986: a unidirectional edge created by a transaction that was still open when its target vertex was deleted
 * (and committed) must not commit: the delete could not find the edge, and the commit had nothing to check the target
 * against, so the edge pointed at a deleted record, and at whatever vertex reused its RID. The same holds in the other
 * order: a delete that scanned for the edges ending in the vertex before another transaction committed one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8986UnidirectionalEdgeToDeletedVertexTest extends TestHelper {
  private RID source;
  private RID target;

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("V");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
  }

  private void createVertices() {
    database.transaction(() -> {
      source = database.newVertex("V").set("name", "a").save().getIdentity();
      target = database.newVertex("V").set("name", "b").save().getIdentity();
    });
  }

  @Test
  void edgeCreatedBeforeTheTargetIsDeletedByAnotherTransactionDoesNotCommit() throws Exception {
    createVertices();
    final ExecutorService otherThread = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.lookupByRID(source, true).asVertex().newEdge("U", database.lookupByRID(target, true).asVertex());

      otherThread.submit(() -> database.transaction(() -> database.lookupByRID(target, true).asVertex().delete())).get();

      assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      otherThread.shutdown();
    }

    assertNoDanglingEdge();
  }

  @Test
  void edgeCommittedAfterTheDeleteScannedForItDoesNotLeaveAGhost() throws Exception {
    createVertices();
    final ExecutorService otherThread = Executors.newSingleThreadExecutor();
    final CountDownLatch deleted = new CountDownLatch(1);
    final CountDownLatch edgeCommitted = new CountDownLatch(1);
    try {
      // the deleting transaction scans for the edges ending in the target, then waits for the edge to commit
      final Future<Object> deleter = otherThread.submit(() -> {
        database.begin();
        try {
          database.lookupByRID(target, true).asVertex().delete();
          deleted.countDown();
          assertThat(edgeCommitted.await(30, TimeUnit.SECONDS)).isTrue();
          database.commit();
          return "committed";
        } catch (final NeedRetryException e) {
          return "conflict";
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      });

      assertThat(deleted.await(30, TimeUnit.SECONDS)).isTrue();
      boolean edgeWasCommitted = true;
      try {
        database.transaction(() -> database.lookupByRID(source, true).asVertex()
            .newEdge("U", database.lookupByRID(target, true).asVertex()));
      } catch (final NeedRetryException | RecordNotFoundException e) {
        edgeWasCommitted = false;
      } finally {
        edgeCommitted.countDown();
      }
      final Object outcome = deleter.get(60, TimeUnit.SECONDS);

      if (edgeWasCommitted)
        assertThat(outcome).as("the delete never saw the committed edge, so it must not commit over it").isEqualTo("conflict");
    } finally {
      otherThread.shutdown();
    }

    assertNoDanglingEdge();
  }

  @Test
  void aRetriedDeleteRemovesTheEdgeThatCommittedUnderIt() throws Exception {
    createVertices();
    final ExecutorService otherThread = Executors.newSingleThreadExecutor();
    final CountDownLatch deleted = new CountDownLatch(1);
    final CountDownLatch edgeCommitted = new CountDownLatch(1);
    try {
      final int[] attempts = { 0 };
      final Future<?> deleter = otherThread.submit(() -> database.transaction(() -> {
        database.lookupByRID(target, true).asVertex().delete();
        if (++attempts[0] == 1) {
          deleted.countDown();
          try {
            assertThat(edgeCommitted.await(30, TimeUnit.SECONDS)).isTrue();
          } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
          }
        }
      }, false, 3));

      assertThat(deleted.await(30, TimeUnit.SECONDS)).isTrue();
      database.transaction(() -> database.lookupByRID(source, true).asVertex()
          .newEdge("U", database.lookupByRID(target, true).asVertex()));
      edgeCommitted.countDown();
      deleter.get(60, TimeUnit.SECONDS);

      assertThat(attempts[0]).as("the first attempt must have been refused and run again").isEqualTo(2);
    } finally {
      otherThread.shutdown();
    }

    assertNoDanglingEdge();
    assertThat(database.countType("U", false)).isZero();
  }

  private void assertNoDanglingEdge() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      assertThat(rs.next().<Long>getProperty("totalCorruptedRecords")).isZero();
    }
    // every edge left points at a vertex that exists: expanding it must not hit a deleted record
    database.transaction(() -> {
      final Vertex a = database.lookupByRID(source, true).asVertex();
      for (final Edge edge : a.getEdges(Vertex.DIRECTION.OUT, "U"))
        assertThat(database.existsRecord(edge.getIn())).as("the target of " + edge.getIdentity() + " must exist").isTrue();
    });
  }

  /** Vertices and unidirectional edges created in one transaction: the targets are not committed yet, and must not conflict. */
  @Test
  void targetsCreatedInTheSameTransactionCommit() {
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("V").set("name", "hub").save();
      for (int i = 0; i < 2_000; i++) {
        final MutableVertex v = database.newVertex("V").set("name", "n" + i).save();
        hub.newEdge("U", v);
      }
    });
    assertThat(database.countType("U", false)).isEqualTo(2_000L);
  }

  /** A delete of a vertex in a bucket no unidirectional edge ended in meanwhile is not refused. */
  @Test
  void aDeleteInAnotherBucketIsNotRefusedByAnUnrelatedEdgeCommit() throws Exception {
    database.getSchema().createVertexType("W");
    createVertices();
    final RID[] other = new RID[1];
    database.transaction(() -> other[0] = database.newVertex("W").set("name", "w").save().getIdentity());
    final ExecutorService otherThread = Executors.newSingleThreadExecutor();
    final CountDownLatch deleted = new CountDownLatch(1);
    final CountDownLatch edgeCommitted = new CountDownLatch(1);
    try {
      final Future<Object> deleter = otherThread.submit(() -> {
        database.begin();
        try {
          database.lookupByRID(other[0], true).asVertex().delete();
          deleted.countDown();
          assertThat(edgeCommitted.await(30, TimeUnit.SECONDS)).isTrue();
          database.commit();
          return "committed";
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      });
      assertThat(deleted.await(30, TimeUnit.SECONDS)).isTrue();
      database.transaction(() -> database.lookupByRID(source, true).asVertex()
          .newEdge("U", database.lookupByRID(target, true).asVertex()));
      edgeCommitted.countDown();
      assertThat(deleter.get(60, TimeUnit.SECONDS)).isEqualTo("committed");
    } finally {
      otherThread.shutdown();
    }
  }
}
