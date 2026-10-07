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
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8784: the replication layer ends a transaction whose pages another thread publishes (a Raft replica, a leader
 * whose entry its state machine applies from the WAL bytes) and a transaction with nothing to publish without running
 * phase 2. {@link TransactionContext#concludeCommitWithoutPublishing()} is that end, and it must be a COMMIT: the
 * after-commit callbacks fire, the commit counter moves, the saved records are clean, the context is released.
 */
class Issue8784ConcludeCommitWithoutPublishingTest extends TestHelper {
  private static final String TYPE_NAME = "Issue8784Doc";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE_NAME);
      database.newDocument(TYPE_NAME).set("name", "seed").save();
    });
  }

  /** The replica tail: phase 1 ran, the pages are published elsewhere, this thread concludes. */
  @Test
  void aTransactionWhosePagesArePublishedElsewhereEndsAsACommit() {
    final AtomicInteger fired = new AtomicInteger();

    database.begin();
    final MutableDocument doc = database.iterateType(TYPE_NAME, false).next().asDocument().modify();
    doc.set("name", "updated").save();
    assertThat(doc.isDirty()).isTrue();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(fired::incrementAndGet);
    final long commitCountBefore = tx.getCommitCount();

    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(false);
    assertThat(phase1).as("the update must leave something to publish").isNotNull();

    tx.concludeCommitWithoutPublishing();

    assertThat(fired.get()).as("the after-commit callback fires exactly once").isEqualTo(1);
    assertThat(tx.getCommitCount()).as("the commit is counted (#7916 retry guard)").isEqualTo(commitCountBefore + 1);
    assertThat(TransactionContext.isPartiallyCommitted(tx, commitCountBefore)).isTrue();
    assertThat(doc.isDirty()).as("the committed record is clean").isFalse();
    assertThat(tx.isActive()).isFalse();
    assertThat(database.isTransactionActive()).isFalse();

    // The file locks phase 1 took are released: the next transaction on the same type commits.
    database.transaction(() -> database.newDocument(TYPE_NAME).set("name", "after").save());
  }

  /** The read-only arm: phase 1 found nothing to publish, which is still a commit. */
  @Test
  void aTransactionWithNothingToPublishEndsAsACommit() {
    final AtomicInteger fired = new AtomicInteger();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(fired::incrementAndGet);
    final long commitCountBefore = tx.getCommitCount();

    assertThat(tx.commit1stPhase(true)).isNull();
    tx.concludeCommitWithoutPublishing();

    assertThat(fired.get()).isEqualTo(1);
    assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** The baseline the HA path must match: a plain embedded commit with nothing to write fires and counts the same way. */
  @Test
  void theEmbeddedReadOnlyCommitIsTheBaseline() {
    final AtomicInteger fired = new AtomicInteger();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(fired::incrementAndGet);
    final long commitCountBefore = tx.getCommitCount();
    database.commit();

    assertThat(fired.get()).isEqualTo(1);
    assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
  }
}
