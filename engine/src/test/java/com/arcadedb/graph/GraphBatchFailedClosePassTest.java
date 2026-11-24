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
import com.arcadedb.exception.RecordNotFoundException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The passes {@link GraphBatch#close()} runs after the last flush (the deferred incoming edges and the vertex head pointers) write in
 * transactions of the batch's own. When one of them fails, the thread must be left without a transaction and the database must be
 * able to run the next batch.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphBatchFailedClosePassTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
  }

  /** A close() that fails before the head pointers are written leaves its edges unlinked, as it always did: not what is checked here. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @AfterEach
  void clearHook() {
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
  }

  @Test
  void failedIncomingPassLeavesNoTransactionOpen() {
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      throw new IllegalStateException("simulated incoming commit failure");
    };

    final GraphBatch batch = newSequentialBatch();
    loadTwoEdges(batch);

    assertThatThrownBy(batch::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("simulated incoming commit failure");
    assertThat(database.isTransactionActive()).as("the failed pass must not leave its transaction on the thread").isFalse();

    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
    assertAnotherBatchRuns();
  }

  @Test
  void failureThatIsNotARuntimeExceptionLeavesNoTransactionOpen() {
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      throw new LinkageError("simulated incoming commit failure");
    };

    final GraphBatch batch = newSequentialBatch();
    loadTwoEdges(batch);

    assertThatThrownBy(batch::close).isInstanceOf(LinkageError.class);
    assertThat(database.isTransactionActive()).as("the failed pass must not leave its transaction on the thread").isFalse();

    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
    assertAnotherBatchRuns();
  }

  @Test
  void failedHeadPointerPassLeavesNoTransactionOpen() {
    final GraphBatch batch = newSequentialBatch();
    final RID[] vertices = loadTwoEdges(batch);

    // The edges are written and the head pointers of their source vertices are deferred to close(): a source that is gone by
    // then makes the pointer pass fail on its first vertex
    batch.flush();
    database.transaction(() -> database.deleteRecord(vertices[0].asVertex(true)));

    assertThatThrownBy(batch::close).isInstanceOf(RecordNotFoundException.class);
    assertThat(database.isTransactionActive()).as("the failed pass must not leave its transaction on the thread").isFalse();

    assertAnotherBatchRuns();
  }

  @Test
  void failedParallelIncomingPassLeavesNoTransactionOpen() {
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      throw new IllegalStateException("simulated incoming commit failure");
    };

    final GraphBatch batch = database.batch().withBatchSize(100).withBidirectional(true).withParallelFlush(true)
        .withMaxDeferredIncomingEdges(0).build();
    loadTwoEdges(batch);

    assertThatThrownBy(batch::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("simulated incoming commit failure");
    assertThat(database.isTransactionActive()).isFalse();

    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
    assertAnotherBatchRuns();
  }

  private GraphBatch newSequentialBatch() {
    return database.batch().withBatchSize(100).withBidirectional(true).withParallelFlush(false).withCommitEvery(2)
        .withMaxDeferredIncomingEdges(0).build();
  }

  private RID[] loadTwoEdges(final GraphBatch batch) {
    final RID[] vertices = batch.createVertices("V", 4);
    batch.newEdge(vertices[0], "E", vertices[1]);
    batch.newEdge(vertices[2], "E", vertices[3]);
    return vertices;
  }

  /** The slot of the database was handed back and the thread is free: a new batch builds, loads and closes. */
  private void assertAnotherBatchRuns() {
    final long edgesBefore = database.countType("E", true);

    try (final GraphBatch next = newSequentialBatch()) {
      final RID[] vertices = next.createVertices("V", 2);
      next.newEdge(vertices[0], "E", vertices[1]);
    }

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType("E", true)).isEqualTo(edgesBefore + 1);
  }
}
