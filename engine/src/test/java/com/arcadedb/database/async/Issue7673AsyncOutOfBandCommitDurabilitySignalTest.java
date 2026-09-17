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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7673: three of the six sites that durably commit an async worker's shared batch never fired
 * the executor-wide {@code onOk()} that #6470 established as THE durability signal for that batch.
 * <p>
 * {@code commitBatch()} (the periodic {@code commitEvery} boundary), {@code closeTransactionBoundaryIfDurability
 * PolicyChanged()} and the worker's shutdown commit all do {@code commit(); onOk(); clearBatchState();}. The three
 * out-of-band sites - {@link DatabaseAsyncIndexCompaction}, {@link DatabaseAsyncParkWorker} and
 * {@link DatabaseAsyncTransaction}'s flush of a dangling prior batch - committed and cleared, but skipped the
 * signal, so a caller counting durable batches (the use case #6470 was filed for) silently missed them.
 * <p>
 * Each test below submits writes that stay in the worker's still-open batch (a {@code commitEvery} far above the
 * number submitted, so no periodic boundary can fire), triggers ONE of the three paths, and asserts that
 * {@code onOk()} arrived. Deliberately asserted BEFORE {@code waitCompletion()}: the completion marker and the
 * shutdown commit both fire {@code onOk()} on their own, so a test that only counted at the end would pass against
 * the unfixed code.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7673AsyncOutOfBandCommitDurabilitySignalTest extends TestHelper {

  private static final String TYPE = "Issue7673Item";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE, 1).createProperty("seq", Type.INTEGER);
  }

  /**
   * {@code db.async().transaction(...)} landing on a worker that still holds an uncommitted batch flushes that
   * batch first. That flush is a real, durable commit of somebody else's writes and must say so.
   */
  @Test
  void asyncTransactionFlushOfADanglingBatchFiresTheDurabilitySignal() throws Exception {
    final CountDownLatch signalled = new CountDownLatch(1);
    final AtomicInteger  onOkCount = prepareBatch(signalled);

    final CountDownLatch txDone = new CountDownLatch(1);
    database.async().transaction(txDone::countDown);

    assertThat(txDone.await(30, TimeUnit.SECONDS)).as("the async transaction ran").isTrue();
    assertThat(signalled.await(30, TimeUnit.SECONDS))
        .as("the flush of the dangling batch durably committed it, so onOk() must have fired (#6470/#7673)").isTrue();
    assertThat(onOkCount.get()).isPositive();
  }

  /**
   * Quiescing the workers commits each one's open batch before parking it - the commit is half the point of
   * {@link DatabaseAsyncParkWorker} (issue #6281), and it is just as durable as the periodic one.
   */
  @Test
  void parkingAWorkerFiresTheDurabilitySignalForTheBatchItPublishes() throws Exception {
    final CountDownLatch signalled = new CountDownLatch(1);
    final AtomicInteger  onOkCount = prepareBatch(signalled);

    try (final AsyncQuiesce ignored = ((DatabaseInternal) database).quiesceAsync()) {
      assertThat(signalled.await(30, TimeUnit.SECONDS))
          .as("the park worker durably committed the batch, so onOk() must have fired (#6470/#7673)").isTrue();
    }
    assertThat(onOkCount.get()).isPositive();
  }

  /**
   * An index compaction task landing mid-batch commits whatever the worker had already run against its shared
   * transaction before it can compact. Same durable commit, same signal.
   */
  @Test
  void indexCompactionFiresTheDurabilitySignalForTheBatchItPublishes() throws Exception {
    final IndexInternal index = (IndexInternal) database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "seq");

    final CountDownLatch signalled = new CountDownLatch(1);
    final AtomicInteger  onOkCount = prepareBatch(signalled);

    // The task itself rather than compact(index): scheduleCompaction()'s AVAILABLE -> COMPACTION_SCHEDULED
    // reservation is about throttling repeated compactions, and this test is about the commit that precedes one.
    ((DatabaseAsyncExecutorImpl) database.async())
        .scheduleTask(0, new DatabaseAsyncIndexCompaction(index), true, 0);

    assertThat(signalled.await(30, TimeUnit.SECONDS))
        .as("the compaction task durably committed the pending batch, so onOk() must have fired (#6470/#7673)")
        .isTrue();
    assertThat(onOkCount.get()).isPositive();
  }

  /**
   * One worker holding an open batch of three writes, a {@code commitEvery} no periodic boundary can reach, and an
   * {@code onOk()} listener that has not been told anything yet.
   *
   * @return the listener's invocation count, guaranteed still zero when this returns
   */
  private AtomicInteger prepareBatch(final CountDownLatch signalled) throws InterruptedException {
    database.async().setParallelLevel(1);
    database.async().setCommitEvery(10_000);

    final CountDownLatch applied = new CountDownLatch(3);
    for (int i = 0; i < 3; i++)
      database.async().createRecord(database.newDocument(TYPE).set("seq", i), r -> applied.countDown());

    assertThat(applied.await(30, TimeUnit.SECONDS)).as("the three writes reached the worker's open batch").isTrue();

    // Registered only now, so nothing that happened while the batch was filling can count towards the assertion.
    final AtomicInteger onOkCount = new AtomicInteger();
    database.async().onOk(() -> {
      onOkCount.incrementAndGet();
      signalled.countDown();
    });
    return onOkCount;
  }
}
