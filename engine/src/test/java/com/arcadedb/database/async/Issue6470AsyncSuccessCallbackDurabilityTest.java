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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.WALFile;
import com.arcadedb.event.AfterRecordCreateListener;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #6470: an async {@code createRecord}/{@code updateRecord}/{@code deleteRecord}'s own success
 * callback (e.g. {@link NewRecordCallback}) fires as soon as the write is applied to the worker's current batch
 * transaction, which is not necessarily durable yet - a later sibling failure in the same still-open batch can still
 * roll the whole thing back, discarding a write whose callback already fired.
 * <p>
 * That per-record timing is unchanged by this fix (and is exercised, unchanged, by
 * {@code Issue6281AsyncBatchIndexBuildTest} and others that rely on it as a "this task has run" signal rather than a
 * durability signal - changing it broke that suite). What was missing, and what {@code Issue6470} is actually about,
 * is a reliable way to know a write IS durable: {@link DatabaseAsyncExecutor#onOk(OkCallback)} - previously fired
 * only at executor shutdown and from the {@code waitCompletion()} marker - now also fires at the periodic
 * {@code setCommitEvery(int)} boundary and at a durability-setting boundary commit, so it is a complete signal for
 * every point this shared batch can actually commit.
 */
class Issue6470AsyncSuccessCallbackDurabilityTest extends TestHelper {

  private static final String TYPE = "Issue6470Item";

  /**
   * Documents the (intentional, unchanged) provisional nature of the per-record callback: it fires for the 1st and
   * 2nd creates below even though the 3rd's failure rolls the whole still-open batch back and discards them.
   */
  @Test
  void perRecordCallbackFiresBeforeTheBatchIsNecessarilyDurable() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1));

    final AtomicInteger errors    = new AtomicInteger();
    final List<RID>     confirmed = new CopyOnWriteArrayList<>();

    // Keyed off content rather than a raw listener-call count, so the trigger is unambiguous regardless of how many
    // times the engine happens to invoke an after-create listener per create.
    final AfterRecordCreateListener throwOnMarked = record -> {
      if ("fail".equals(((Document) record).getString("marker")))
        throw new RuntimeException("injected error on third create");
    };

    database.async().setCommitEvery(10);
    database.async().onError(e -> errors.incrementAndGet());

    database.getEvents().registerListener(throwOnMarked);
    try {
      // 1st and 2nd creates succeed and stay in the still-open batch; the 3rd's listener throws, rolling the whole
      // batch back (1st and 2nd included); the 4th runs in the fresh transaction begun after that rollback.
      database.async().createRecord(database.newDocument(TYPE).set("marker", "ok"), r -> confirmed.add(r.getIdentity()));
      database.async().createRecord(database.newDocument(TYPE).set("marker", "ok"), r -> confirmed.add(r.getIdentity()));
      database.async().createRecord(database.newDocument(TYPE).set("marker", "fail"), r -> confirmed.add(r.getIdentity()));
      database.async().createRecord(database.newDocument(TYPE).set("marker", "ok"), r -> confirmed.add(r.getIdentity()));

      database.async().waitCompletion();
    } finally {
      database.getEvents().unregisterListener(throwOnMarked);
    }

    assertThat(errors.get()).isEqualTo(1);

    // By design: the per-record callback already fired for the 1st and 2nd creates too, even though the 3rd's
    // failure rolled the whole still-open batch back and discarded them - it reports "applied", not "durable".
    assertThat(confirmed).hasSize(3);

    // Only the 4th create - the one in the fresh transaction begun after the rollback - actually exists.
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(1));
  }

  /**
   * The actual fix: {@code onOk()} now fires at the periodic {@code commitEvery} boundary, so a caller has a
   * reliable durability signal without needing to wait for the executor to go idle or shut down.
   */
  @Test
  void executorWideOnOkFiresAtThePeriodicCommitBoundary() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1));

    database.async().setCommitEvery(5);

    final CountDownLatch firstBoundaryCommitted = new CountDownLatch(1);
    final AtomicInteger  onOkInvocations        = new AtomicInteger();
    database.async().onOk(() -> {
      onOkInvocations.incrementAndGet();
      firstBoundaryCommitted.countDown();
    });

    for (int i = 0; i < 12; i++)
      database.async().createRecord(database.newDocument(TYPE), null);

    // Proves the periodic commitEvery boundary itself signals durability - not just the final waitCompletion()
    // marker, which has not been called yet at this point.
    assertThat(firstBoundaryCommitted.await(30, TimeUnit.SECONDS))
        .as("onOk() must fire at the periodic commitEvery boundary, before waitCompletion() is even called")
        .isTrue();

    database.async().waitCompletion();

    // 12 creates over a batch of 5 cross the periodic boundary twice (at the 5th and 10th), so at least 2 by the
    // time the loop above already observed 1 of them. (waitCompletion()'s own completion marker also triggers onOk()
    // on every worker, including idle ones, so the final count is not asserted precisely here.)
    assertThat(onOkInvocations.get()).isGreaterThanOrEqualTo(2);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(12));
  }

  /**
   * The other real commit point {@code onOk()} was extended to cover: a durability-setting change
   * ({@code setTransactionSync}/{@code setTransactionUseWAL}) mid-batch forces the worker to commit its
   * already-open transaction under its OLD flags before applying the new ones - see
   * {@code DatabaseAsyncExecutorImpl.closeTransactionBoundaryIfDurabilityPolicyChanged()}.
   */
  @Test
  void executorWideOnOkFiresAtTheDurabilityPolicyBoundary() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1));
    // Large enough that the periodic commitEvery boundary this test is NOT about never fires here.
    database.async().setCommitEvery(100);

    final AtomicInteger  onOkInvocations = new AtomicInteger();
    final CountDownLatch fired           = new CountDownLatch(1);
    database.async().onOk(() -> {
      onOkInvocations.incrementAndGet();
      fired.countDown();
    });

    // Single bucket, so both creates land on the same worker in submission order: the 1st opens a
    // transaction stamped with the CURRENT durability flags and leaves it open (commitEvery is nowhere
    // near reached); the flag flip in between is picked up only when the 2nd create is dispatched, at
    // which point that still-open transaction's stamped flags no longer match and must be committed first.
    database.async().createRecord(database.newDocument(TYPE), null);
    database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
    database.async().createRecord(database.newDocument(TYPE), null);

    assertThat(fired.await(30, TimeUnit.SECONDS))
        .as("onOk() must fire when a durability-setting change forces a boundary commit mid-batch")
        .isTrue();

    database.async().waitCompletion();
    assertThat(onOkInvocations.get()).isGreaterThanOrEqualTo(1);
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(2));
  }
}
