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
import com.arcadedb.database.Document;
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
