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
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13437: a refusal of commits that never left this node ({@link RefusedBeforePublishingException}) stays retryable through
 * the retry loops, unless the block also committed something before the refused commits started.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13437RefusedBeforePublishingTest extends TestHelper {

  @BeforeEach
  void createType() {
    if (!database.getSchema().existsType("Doc"))
      database.getSchema().createDocumentType("Doc");
  }

  @Test
  void aBlockWhoseCommitsAllFollowTheWatermarkGetsTheRefusalAsItIs() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicReference<RefusedBeforePublishingException> raised = new AtomicReference<>();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      final RefusedBeforePublishingException.Watermark watermark = watermark();
      database.newDocument("Doc").set("n", 1).save();
      database.commit();
      raised.set(refused(watermark));
      throw raised.get();
    })).isSameAs(raised.get()).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("never replayed on this node, which discards the commit").isEqualTo(1);
  }

  @Test
  void aBlockThatCommittedBeforeTheWatermarkIsStillNotReplayable() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument("Doc").set("n", 1).save();
      // Published before the refused commits started: a replay would apply it twice
      database.commit();
      database.begin();
      final RefusedBeforePublishingException.Watermark watermark = watermark();
      database.newDocument("Doc").set("n", 2).save();
      database.commit();
      throw refused(watermark);
    })).isInstanceOf(TransactionException.class)
        .satisfies(e -> assertThat(e.getSuppressed()).singleElement().isInstanceOf(RefusedBeforePublishingException.class));
  }

  @Test
  void theDecisionIsTheWatermarkOfTheBlocksOwnTransaction() throws Exception {
    database.begin();
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final long before = tx.getCommitCount();
      final RefusedBeforePublishingException.Watermark watermark = watermark();
      final RefusedBeforePublishingException refusal = refused(watermark);

      assertThat(TransactionContext.notReplayable(refusal, "Block", tx, before)).isSameAs(refusal);
      assertThat(TransactionContext.notReplayable(refusal, "Block", tx, before - 1)).as("a commit before the watermark")
          .isInstanceOf(TransactionException.class);
      assertThat(TransactionContext.notReplayable(refusal, "Block", new TransactionContext((DatabaseInternal) database), 0L))
          .as("a transaction stacked after the watermark").isSameAs(refusal);
      assertThat(TransactionContext.notReplayable(new NeedRetryException("plain"), "Block", tx, before))
          .as("any other refusal is not replayable, as before").isInstanceOf(TransactionException.class);

      // The watermark describes the thread it was taken on: another thread's commits have nothing to do with it
      final AtomicReference<ArcadeDBException> onAnotherThread = new AtomicReference<>();
      final Thread other = new Thread(() -> onAnotherThread.set(TransactionContext.notReplayable(refusal, "Block", tx, before)));
      other.start();
      other.join();
      assertThat(onAnotherThread.get()).isInstanceOf(TransactionException.class);
    } finally {
      database.rollback();
    }
  }

  @Test
  void itIsNeverRetriedOnThisNode() {
    final RefusedBeforePublishingException refusal = refused(watermark());
    assertThat(refusal.isRetryableOnThisNode()).isFalse();
    assertThat(refusal.getCause()).isInstanceOf(NeedRetryException.class);
  }

  private RefusedBeforePublishingException.Watermark watermark() {
    return RefusedBeforePublishingException.Watermark.ofCurrentThread(((DatabaseInternal) database).getDatabasePath());
  }

  private static RefusedBeforePublishingException refused(final RefusedBeforePublishingException.Watermark watermark) {
    return new RefusedBeforePublishingException(new NeedRetryException("Refused before anything was published"), watermark);
  }
}
