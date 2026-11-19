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
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A walk hands out the vertices and edges its adjacency entries name without reading them, and reads one only when a property of
 * it is asked for. A transaction that commits in between can delete it - the delete of a vertex removes the edges to it in the
 * same commit - and the read used to answer a {@link RecordNotFoundException}, which a retry does not absorb: the Cypher half of
 * issue #11713 ({@code MATCH (h)<-[:TRANSFERS]-(s) RETURN s.number} during {@code DETACH DELETE}s). A record reached through the
 * graph now answers the retryable {@link ConcurrentModificationException}; one looked up by the caller's own RID, or deleted by
 * the caller's own transaction, still answers that it is not found.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphReachedRecordConcurrentDeleteTest extends TestHelper {
  // THE ONE TEST THAT LEAVES A DANGLING ENTRY ON PURPOSE TURNS THE AFTER-TEST INTEGRITY CHECK OFF
  private boolean checkIntegrity = true;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return checkIntegrity;
  }

  @Test
  void vertexReachedThroughAnAdjacencyEntryAndDeletedConcurrentlyIsARetryableConflict() throws Exception {
    final RID[] rids = buildPair();
    database.begin();
    try {
      final Iterator<Vertex> walk = rids[0].asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS").iterator();
      final Vertex source = walk.next();
      assertThat(source.getIdentity()).isEqualTo(rids[1]);

      deleteInAnotherTransaction(rids[1]);

      assertThatThrownBy(() -> source.get("number")).isInstanceOf(ConcurrentModificationException.class)
          .hasCauseInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }

  @Test
  void endpointOfAnEdgeDeletedConcurrentlyIsARetryableConflict() throws Exception {
    final RID[] rids = buildPair();
    database.begin();
    try {
      final Iterator<Edge> walk = rids[0].asVertex(true).getEdges(Vertex.DIRECTION.IN, "TRANSFERS").iterator();
      final Vertex source = walk.next().getOutVertex();

      deleteInAnotherTransaction(rids[1]);

      assertThatThrownBy(() -> source.get("number")).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
  }

  /**
   * Under REPEATABLE_READ the lookup reads the record at once, so a concurrent delete fails there, before any mark is set. The
   * source is of another type than the hub, so its page is not one the transaction pinned by reading the hub: a pinned page would
   * serve it from the snapshot, which is the repeatable read working.
   */
  @Test
  void repeatableReadLookupOfAVertexDeletedConcurrentlyIsARetryableConflict() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub");
      database.getSchema().createVertexType("Source");
      database.getSchema().createEdgeType("TRANSFERS");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub").set("number", 0).save();
      final MutableVertex source = database.newVertex("Source").set("number", 1).save();
      source.newEdge("TRANSFERS", hub);
      rids[0] = hub.getIdentity();
      rids[1] = source.getIdentity();
    });

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // THE ITERATOR HOLDS THE HEAD SEGMENT, AND SO THE ENTRY, BEFORE THE DELETE COMMITS
      final Iterator<Vertex> walk = rids[0].asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS").iterator();

      deleteInAnotherTransaction(rids[1]);

      assertThatThrownBy(walk::hasNext).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
  }

  /**
   * With no transaction, the endpoint of an edge a walk reached carries the walk's mark, not one taken when the endpoint is looked
   * up: only the endpoint record is deleted, after the walk, and the endpoint read is the conflict it is.
   */
  @Test
  void endpointOfAnEdgeReachedWithoutATransactionCarriesTheWalkMark() throws Exception {
    checkIntegrity = false;
    final RID[] rids = buildPair();
    final Edge edge = rids[0].asVertex(true).getEdges(Vertex.DIRECTION.IN, "TRANSFERS").iterator().next();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(() -> database.getSchema().getBucketById(rids[1].getBucketId()).deleteRecord(rids[1]));
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "concurrent-deleter");
    writer.start();
    writer.join();
    assertThat(failure.get()).isNull();

    final Vertex source = edge.getOutVertex();
    assertThatThrownBy(() -> source.get("number")).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void sqlTraversalOfAVertexDeletedConcurrentlyIsARetryableConflict() throws Exception {
    final RID[] rids = buildPair();
    database.begin();
    try {
      final Vertex source;
      try (final ResultSet rs = database.query("sql", "SELECT expand(in('TRANSFERS')) FROM " + rids[0])) {
        source = rs.next().getVertex().orElseThrow();
      }

      deleteInAnotherTransaction(rids[1]);

      assertThatThrownBy(() -> source.get("number")).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
  }

  @Test
  void recordLookedUpByItsRidStillAnswersNotFound() throws Exception {
    final RID[] rids = buildPair();
    database.begin();
    try {
      final Document lazy = (Document) database.lookupByRID(rids[1], false);

      deleteInAnotherTransaction(rids[1]);

      // THE CALLER NAMED THE RID: A RECORD THAT IS NOT THERE IS THE ANSWER, NOT A CONFLICT TO RETRY
      assertThatThrownBy(() -> lazy.get("number")).isInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }

  /**
   * A reference that was dangling before the walk read it - the record removed without the edge - is not a conflict: nothing
   * committed since, and the read answers that the record is not found, which is what the checks and the walks that skip a
   * dangling entry rely on.
   */
  @Test
  void danglingReferenceStillAnswersNotFound() {
    checkIntegrity = false;
    final RID[] rids = buildPair();
    database.transaction(() -> database.getSchema().getBucketById(rids[1].getBucketId()).deleteRecord(rids[1]));

    database.begin();
    try {
      final Vertex source = rids[0].asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS").iterator().next();
      assertThatThrownBy(() -> source.get("number")).isInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }

  @Test
  void vertexDeletedByTheOwnTransactionStillAnswersNotFound() {
    final RID[] rids = buildPair();
    database.begin();
    try {
      final Vertex source = rids[0].asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS").iterator().next();
      rids[1].asVertex(true).delete();

      assertThatThrownBy(() -> source.get("number")).isInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }

  /** The 16-bit mark never reads as "not marked" and folds 65535 commits apart onto the same value, as its Javadoc says. */
  @Test
  void commitMarkIsNeverZeroAndWrapsEvery65535Commits() {
    assertThat((int) ImmutableDocument.commitMark(0)).isEqualTo(1);
    assertThat((int) ImmutableDocument.commitMark(65534)).isEqualTo(65535);
    assertThat((int) ImmutableDocument.commitMark(65535)).isEqualTo(1);
    assertThat(ImmutableDocument.commitMark(Long.MAX_VALUE)).isNotEqualTo((char) 0);
    for (long published = 0; published < 200_000; published += 997)
      assertThat(ImmutableDocument.commitMark(published)).isNotEqualTo((char) 0)
          .isEqualTo(ImmutableDocument.commitMark(published + 65535));
    assertThat(ImmutableDocument.commitMark(41)).isNotEqualTo(ImmutableDocument.commitMark(42));
  }

  /** The hub and one source vertex with an edge into it. */
  private RID[] buildPair() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createEdgeType("TRANSFERS");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account").set("number", 0).save();
      final MutableVertex source = database.newVertex("Account").set("number", 1).save();
      source.newEdge("TRANSFERS", hub);
      rids[0] = hub.getIdentity();
      rids[1] = source.getIdentity();
    });
    return rids;
  }

  private void deleteInAnotherTransaction(final RID vertex) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(() -> vertex.asVertex(true).delete());
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "concurrent-deleter");
    writer.start();
    writer.join();
    if (failure.get() != null)
      throw new AssertionError("the concurrent delete failed", failure.get());
  }
}
