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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9242: {@link GraphBatch} committed (or, on a retry, rolled back) a transaction the
 * caller had opened, and applied its own WAL policy to the caller's writes. The batch manages its own transactions, so
 * it now refuses to run inside one, and its WAL policy never reaches a transaction that is not its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9242GraphBatchCallerTransactionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Note");
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
  }

  private void saveNote(final String tag) {
    final MutableDocument note = database.newDocument("Note");
    note.set("tag", tag);
    note.set("payload", "x".repeat(4000));
    note.save();
  }

  private long notes() {
    return database.countType("Note", true);
  }

  private long walBytes() {
    return ((Number) ((DatabaseInternal) database).getTransactionManager().getStats().get("bytesWritten")).longValue();
  }

  @Test
  void createVerticesRefusedInsideCallerTransaction() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      database.begin();
      saveNote("A");

      assertThatThrownBy(() -> batch.createVertices("V", 2)).isInstanceOf(IllegalStateException.class);
      assertThat(database.isTransactionActive()).isTrue();

      database.rollback();
    }
    assertThat(notes()).isZero();
    assertThat(database.countType("V", true)).isZero();
  }

  @Test
  void createVerticesWithPropertiesRefusedInsideCallerTransaction() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      database.begin();
      saveNote("A2");

      assertThatThrownBy(() -> batch.createVertices("V", new Object[][] { { "name", "a" }, { "name", "b" } })).isInstanceOf(
          IllegalStateException.class);
      assertThat(database.isTransactionActive()).isTrue();

      database.rollback();
    }
    assertThat(notes()).isZero();
  }

  @Test
  void flushRefusedInsideCallerTransaction() {
    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(100).build()) {
      final RID[] v = batch.createVertices("V", 2);
      batch.newEdge(v[0], "E", v[1]);

      database.begin();
      saveNote("C1");

      assertThatThrownBy(batch::flush).isInstanceOf(IllegalStateException.class);
      assertThat(database.isTransactionActive()).isTrue();
      database.rollback();

      // Once the caller's transaction is gone the buffered edge is still there and flushes normally
      batch.flush();
    }
    assertThat(notes()).isZero();
    assertThat(database.countType("E", true)).isEqualTo(1);
  }

  @Test
  void closeRefusedInsideCallerTransactionKeepsTheBatchOpenAndItsWork() {
    final GraphBatch batch = GraphBatch.builder(database).withBatchSize(100).build();
    final RID[] v = batch.createVertices("V", 2);
    batch.newEdge(v[0], "E", v[1]);

    database.begin();
    saveNote("C3");

    assertThatThrownBy(batch::close).isInstanceOf(IllegalStateException.class);
    assertThat(database.isTransactionActive()).isTrue();
    database.rollback();

    assertThat(notes()).isZero();
    assertThat(database.countType("E", true)).isZero();

    // Nothing was dropped: closing again outside the caller's transaction writes the pending edge, in both directions
    batch.close();
    assertThat(database.countType("E", true)).isEqualTo(1);
    assertThat(database.lookupByRID(v[1], true).asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);

    // The guard was released by the real close: a new batch can start
    try (final GraphBatch next = GraphBatch.builder(database).build()) {
      assertThat(next.createVertices("V", 1)).hasSize(1);
    }
  }

  @Test
  void batchWalPolicyDoesNotReachTheCallersTransactions() {
    database.transaction(() -> saveNote("warm-up"));

    try (final GraphBatch batch = GraphBatch.builder(database).withWAL(false).withWALFlush(WALFile.FlushType.NO).build()) {
      batch.createVertices("V", 2);

      final long before = walBytes();
      database.transaction(() -> saveNote("between-batch-calls"));
      assertThat(walBytes() - before).as("the caller's own commit must reach the WAL while the batch is open")
          .isGreaterThanOrEqualTo(4000);
    }

    final long before = walBytes();
    database.transaction(() -> saveNote("after-close"));
    assertThat(walBytes() - before).isGreaterThanOrEqualTo(4000);
  }

  @Test
  void refusalLeavesTheCallersWalFlushSettingAlone() {
    try (final GraphBatch batch = GraphBatch.builder(database).withWALFlush(WALFile.FlushType.NO).build()) {
      database.begin();
      ((DatabaseInternal) database).getTransaction().setWALFlush(WALFile.FlushType.YES_FULL);

      assertThatThrownBy(() -> batch.createVertices("V", 1)).isInstanceOf(IllegalStateException.class);
      assertThat(((DatabaseInternal) database).getTransaction().getThreadWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);

      database.rollback();
    }
  }
}
