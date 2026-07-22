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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5378: {@link GraphBatch} relaxes the WAL policy of the current transaction
 * for bulk-load speed, but on {@code close()} it must put back exactly the values that were in effect
 * before. The {@code TransactionContext} is reused across transactions on the same thread, so leaving
 * {@code useWAL=false} / {@code walFlush=NO} behind silently downgrades the durability contract of every
 * later transaction on that thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchWALRestoreTest extends TestHelper {

  private static final String VERTEX_TYPE = "WalNode";
  private static final String EDGE_TYPE    = "WalLink";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });
  }

  @Test
  void restoresConfiguredWALSettingsAfterClose() {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    db.getTransaction().setWALFlush(WALFile.FlushType.YES_FULL);
    db.getTransaction().setUseWAL(true);
    database.commit();

    runBatch();

    assertThat(db.getTransaction().getWALFlush()).as("WAL flush strategy after GraphBatch.close()")
        .isEqualTo(WALFile.FlushType.YES_FULL);
    assertThat(db.getTransaction().isUseWAL()).as("useWAL after GraphBatch.close()").isTrue();
  }

  @Test
  void restoresWALSettingsEvenWhenTransactionEndsInsideBatch() {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getTransaction().setWALFlush(WALFile.FlushType.YES_FULL);

    runBatch();

    // A new transaction on the same (reused) context must not inherit the relaxed policy.
    database.begin();
    try {
      assertThat(db.getTransaction().getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
    } finally {
      database.commit();
    }
  }

  @Test
  void buildsOnThreadWithoutTransactionContext() throws InterruptedException {
    // Regression for the CI breakage introduced by the first #5378 fix: PostBatchHandler builds the
    // batch on an HTTP worker thread that has never run a transaction, so no TransactionContext exists
    // yet and the constructor must not require one. The saved policy falls back to the configured
    // defaults (arcadedb.txWAL / arcadedb.txWalFlush), which is what a fresh context initializes from.
    final DatabaseInternal db = (DatabaseInternal) database;
    final boolean configuredUseWAL = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);
    final WALFile.FlushType configuredWALFlush = WALFile.getWALFlushType(
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH));

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        runBatch();

        // The batch's own transactions created the context: it must not keep the relaxed policy.
        final TransactionContext tx = db.getTransactionIfExists();
        assertThat(tx).as("TransactionContext created by the batch on the worker thread").isNotNull();
        assertThat(tx.isUseWAL()).as("useWAL after GraphBatch.close() on a fresh thread").isEqualTo(configuredUseWAL);
        assertThat(tx.getWALFlush()).as("WAL flush strategy after GraphBatch.close() on a fresh thread")
            .isEqualTo(configuredWALFlush);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    worker.start();
    worker.join();

    assertThat(failure.get()).as("GraphBatch on a thread without a TransactionContext").isNull();
  }

  private void runBatch() {
    try (final GraphBatch batch = GraphBatch.builder(database)
        .withWAL(false)
        .withWALFlush(WALFile.FlushType.NO)
        .build()) {
      database.begin();
      final MutableVertex v1 = batch.newVertex(VERTEX_TYPE).save();
      final MutableVertex v2 = batch.newVertex(VERTEX_TYPE).save();
      database.commit();
      batch.newEdge(v1.getIdentity(), EDGE_TYPE, v2.getIdentity());
    }
  }
}
