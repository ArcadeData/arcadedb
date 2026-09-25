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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8352: {@link Database#setWALFlush}, {@link Database#setUseWAL} and {@link Database#setAsyncFlush} changed
 * only the calling thread's transactions, so a multi-threaded application that asked for a flush on every commit got
 * it on one thread only. They are database-wide now, as their javadoc and neighbouring setters read; a thread keeps
 * a setting of its own through its transaction, which wins for that thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8352WALSettingsScopeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
  }

  @Test
  void databaseSettersReachEveryThread() throws Exception {
    // The issue's repro: set on one thread, committed from another. The same worker checks again after a second
    // change, so a transaction context it already created must follow too, not only a fresh one
    final CountDownLatch firstChecked = new CountDownLatch(1);
    final CountDownLatch changedAgain = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    database.setWALFlush(WALFile.FlushType.YES_NOMETADATA);
    database.setUseWAL(false);
    database.setAsyncFlush(false);

    final Thread worker = new Thread(() -> {
      try {
        commitAndCheck(tx -> {
          assertThat(tx.getWALFlush()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);
          assertThat(tx.isUseWAL()).isFalse();
          assertThat(tx.isAsyncFlush()).isFalse();
        });
        firstChecked.countDown();
        changedAgain.await();
        commitAndCheck(tx -> {
          assertThat(tx.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
          assertThat(tx.isUseWAL()).isTrue();
          assertThat(tx.isAsyncFlush()).isTrue();
        });
      } catch (final Throwable t) {
        failure.set(t);
        firstChecked.countDown();
      }
    });
    worker.start();
    firstChecked.await();

    database.setWALFlush(WALFile.FlushType.YES_FULL);
    database.setUseWAL(true);
    database.setAsyncFlush(true);
    changedAgain.countDown();
    worker.join();
    assertThat(failure.get()).isNull();

    final TransactionContext own = ((DatabaseInternal) database).getTransaction();
    assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
    assertThat(database.isAsyncFlush()).isTrue();
  }

  @Test
  void aThreadsOwnSettingWinsForThatThreadOnly() throws Exception {
    database.setWALFlush(WALFile.FlushType.YES_NOMETADATA);

    final TransactionContext own = ((DatabaseInternal) database).getTransaction();
    own.setWALFlush(WALFile.FlushType.NO);
    own.setUseWAL(false);
    own.setAsyncFlush(false);

    database.transaction(() -> database.newDocument("T").set("k", 1).save());
    assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.NO);
    assertThat(own.isUseWAL()).isFalse();
    assertThat(own.isAsyncFlush()).isFalse();

    onAnotherThread(tx -> {
      assertThat(tx.getWALFlush()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);
      assertThat(tx.isUseWAL()).isTrue();
      assertThat(tx.isAsyncFlush()).isTrue();
    });

    // Handing the thread back to the database's settings
    own.setWALFlush(null);
    own.setThreadUseWAL(null);
    assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_NOMETADATA);
    assertThat(own.isUseWAL()).isTrue();
  }

  @Test
  void aGraphBatchLeavesTheThreadFollowingTheDatabase() {
    // The batch relaxes the thread's WAL for the load and puts back what it found. Putting back the value in effect
    // instead of "follows the database" would pin the thread, deaf to a later database-wide change
    final TransactionContext own = ((DatabaseInternal) database).getTransaction();
    try (final GraphBatch batch = GraphBatch.builder(database).withWAL(false).withWALFlush(WALFile.FlushType.NO).build()) {
      database.begin();
      final MutableVertex v1 = batch.newVertex("V").save();
      final MutableVertex v2 = batch.newVertex("V").save();
      database.commit();
      batch.newEdge(v1.getIdentity(), "E", v2.getIdentity());
    }
    assertThat(own.getThreadUseWAL()).isNull();
    assertThat(own.getThreadWALFlush()).isNull();

    database.setWALFlush(WALFile.FlushType.YES_FULL);
    assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
  }

  @Test
  void theAsyncExecutorLeavesTheDatabaseSettingsAlone() {
    // Its worker threads stamp their own policy on their transactions: that must not become the database's
    database.setWALFlush(WALFile.FlushType.YES_FULL);
    database.async().setTransactionSync(WALFile.FlushType.NO);
    database.async().setTransactionUseWAL(false);
    for (int i = 0; i < 50; i++)
      database.async().createRecord(database.newDocument("T").set("k", i), null);
    database.async().waitCompletion();

    final TransactionContext own = ((DatabaseInternal) database).getTransaction();
    assertThat(own.getWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);
    assertThat(own.isUseWAL()).isTrue();
    assertThat(database.countType("T", false)).isEqualTo(50);
  }

  private void commitAndCheck(final Consumer<TransactionContext> check) {
    database.transaction(() -> {
      database.newDocument("T").set("k", 0).save();
      check.accept(((DatabaseInternal) database).getTransaction());
    });
  }

  private void onAnotherThread(final Consumer<TransactionContext> check) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> {
          database.newDocument("T").set("k", 0).save();
          check.accept(((DatabaseInternal) database).getTransaction());
        });
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    thread.start();
    thread.join();
    assertThat(failure.get()).isNull();
  }
}
