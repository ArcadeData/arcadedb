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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

// Regression: #13163
/**
 * Issue #13163: {@link GraphBatch#abandon()} gave the batch slot back and dropped what was buffered, but never connected the
 * deferred incoming side of the edges an earlier {@code flush()} had already committed, nor wrote the head pointers of their
 * vertices' edge lists. Those edges were committed, counted by {@link GraphBatch#getTotalEdgesCreated()}, and reachable from no
 * vertex in either direction. The gRPC {@code GraphBatchLoad} drops a failed or terminated load this way.
 * <p>
 * abandon() now finishes what was flushed - the deferred incoming edges and the head pointers - and still drops what is only
 * buffered, so the committed part of the load is made of whole edges and the buffered tail is not written.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13163GraphBatchAbandonTest extends TestHelper {
  private static final String NODE = "Node13163";
  private static final String LINK = "Link13163";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType(NODE);
    database.getSchema().createEdgeType(LINK);
  }

  /**
   * The issue's reproduction: a buffer of two edges, so the second one flushes both, and a third that stays buffered when the load
   * is abandoned. The two flushed edges must be reachable from both of their endpoints, the third must not be written.
   */
  @ParameterizedTest(name = "parallelFlush={0}, verticesOfTheBatch={1}")
  @CsvSource({ "true,true", "false,true", "true,false", "false,false" })
  void abandonKeepsTheFlushedEdgesWholeAndDropsTheBufferedOnes(final boolean parallelFlush, final boolean verticesOfTheBatch) {
    final RID[] existing = verticesOfTheBatch ? null : createVerticesOutsideTheBatch(4);
    final GraphBatch batch = database.batch().withBatchSize(2).withParallelFlush(parallelFlush).build();
    final RID[] v = verticesOfTheBatch ? batch.createVertices(NODE, namedVertices(4)) : existing;

    loadTwoFlushedEdgesAndOneBuffered(batch, v);
    batch.abandon();

    assertTheFlushedEdgesAreWholeAndTheBufferedOneIsGone(batch, v);
  }

  /** abandon() is documented to run on a thread other than the one that opened the batch: the gRPC termination path does. */
  @Test
  void abandonFromAnotherThreadKeepsTheFlushedEdgesWhole() throws InterruptedException {
    final GraphBatch batch = database.batch().withBatchSize(2).build();
    final RID[] v = batch.createVertices(NODE, namedVertices(4));
    loadTwoFlushedEdgesAndOneBuffered(batch, v);

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread other = new Thread(() -> {
      try {
        batch.abandon();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "abandon-13163");
    other.start();
    other.join(60_000);

    assertThat(other.isAlive()).as("abandon() must return").isFalse();
    assertThat(failure.get()).as("abandon() never throws").isNull();
    assertTheFlushedEdgesAreWholeAndTheBufferedOneIsGone(batch, v);
  }

  /**
   * A thread holding a transaction of its own on the database cannot run the batch's writes: they would be committed together with
   * the caller's. abandon() still gives the slot back and leaves the caller's transaction alone, and a close() afterwards, outside
   * that transaction, finishes the flushed edges.
   */
  @Test
  void abandonInsideACallerTransactionLeavesItAloneAndCloseFinishesTheFlushedEdges() {
    final GraphBatch batch = database.batch().withBatchSize(2).build();
    final RID[] v = batch.createVertices(NODE, namedVertices(4));
    loadTwoFlushedEdgesAndOneBuffered(batch, v);

    database.begin();
    final MutableDocument callersVertex = database.newVertex(NODE).set("name", "caller");
    callersVertex.save();
    batch.abandon();
    assertThat(database.isTransactionActive()).as("the caller's transaction is still the caller's").isTrue();
    database.rollback();

    assertThat(countRecords(NODE)).as("nothing of the caller's transaction was committed by abandon()").isEqualTo(4);

    // The slot was given back
    try (final GraphBatch next = database.batch().build()) {
      assertThat(next).isNotNull();
    }

    batch.close();
    assertTheFlushedEdgesAreWholeAndTheBufferedOneIsGone(batch, v);
  }

  private void loadTwoFlushedEdgesAndOneBuffered(final GraphBatch batch, final RID[] v) {
    batch.newEdge(v[0], LINK, v[1]);
    // The buffer of two is full here: both edges are written and connected outgoing, their incoming side is deferred
    batch.newEdge(v[1], LINK, v[2]);
    // Buffered, never flushed
    batch.newEdge(v[2], LINK, v[3]);

    assertThat(batch.getTotalEdgesCreated()).as("the second edge flushed the buffer of two").isEqualTo(2);
    assertThat(batch.getBufferedEdgeCount()).isEqualTo(1);
  }

  private void assertTheFlushedEdgesAreWholeAndTheBufferedOneIsGone(final GraphBatch batch, final RID[] v) {
    assertThat(batch.getTotalEdgesCreated()).as("abandon() does not flush the buffered edge").isEqualTo(2);
    assertThat(countRecords(LINK)).as("the edge records written are the two flushed ones").isEqualTo(2);

    database.transaction(() -> {
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, LINK)).as("e0 -> e1 is reachable from e0").isEqualTo(1);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, LINK)).as("e0 -> e1 is reachable from e1").isEqualTo(1);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.OUT, LINK)).as("e1 -> e2 is reachable from e1").isEqualTo(1);
      assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, LINK)).as("e1 -> e2 is reachable from e2").isEqualTo(1);
      assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.OUT, LINK)).as("the buffered e2 -> e3 was dropped").isZero();
      assertThat(v[3].asVertex().countEdges(Vertex.DIRECTION.IN, LINK)).as("the buffered e2 -> e3 was dropped").isZero();
    });

    try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
      final Result row = result.next();
      assertThat(((Number) row.getProperty("unreachableEdgeRecords")).longValue()).as("unreachable edges: %s", row.toJSON()).isZero();
      assertThat(row.<Collection<?>>getProperty("corruptedRecords")).as("corruptedRecords: %s", row.toJSON()).isEmpty();
      assertThat(((Number) row.getProperty("invalidLinks")).longValue()).as("invalidLinks: %s", row.toJSON()).isZero();
    }

    // The slot was given back, and a close() after abandon() writes nothing more
    try (final GraphBatch next = database.batch().build()) {
      assertThat(next).isNotNull();
    }
    batch.close();
    assertThat(countRecords(LINK)).as("a close() after abandon() does not bring the dropped edge back").isEqualTo(2);
  }

  private RID[] createVerticesOutsideTheBatch(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids[i] = database.newVertex(NODE).set("name", "e" + i).save().getIdentity();
    });
    return rids;
  }

  private static Object[][] namedVertices(final int count) {
    final Object[][] props = new Object[count][];
    for (int i = 0; i < count; i++)
      props[i] = new Object[] { "name", "e" + i };
    return props;
  }

  private long countRecords(final String type) {
    final long[] count = { 0 };
    database.iterateType(type, true).forEachRemaining(r -> count[0]++);
    return count[0];
  }
}
