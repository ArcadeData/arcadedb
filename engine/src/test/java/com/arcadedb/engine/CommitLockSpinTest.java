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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code arcadedb.commitLockSpin} (issue #10501) reaches the commit's file locks: by default (0) a commit that queues
 * behind a held file lock parks at once; with a spin it spins first and, the lock being held far longer than the spin,
 * runs out and parks. The transaction manager's stats count both.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CommitLockSpinTest extends TestHelper {
  private static final String TYPE = "SpinDoc";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
  }

  @Test
  @Timeout(60)
  void aQueuedCommitParksAtOnceByDefault() throws Exception {
    final HandOffs stats = commitBehindAHeldFileLock();

    assertThat(stats.spinsExpired).isZero();
    assertThat(stats.spinHandOffs).isZero();
    assertThat(stats.parkHandOffs).isGreaterThanOrEqualTo(1);
  }

  @Test
  @Timeout(60)
  void aQueuedCommitSpinsBeforeItParksWithASpin() throws Exception {
    final Object previous = GlobalConfiguration.COMMIT_LOCK_SPIN.getValue();
    GlobalConfiguration.COMMIT_LOCK_SPIN.setValue(50L);
    try {
      // read when the database opens
      reopenDatabase();
      final HandOffs stats = commitBehindAHeldFileLock();

      // The held file lock was taken fresh, so it has no hold history yet and its first waiter spins (a resource's average
      // starts at its first release); the spin is forced off on a single core
      if (Runtime.getRuntime().availableProcessors() > 1)
        assertThat(stats.spinsExpired).isGreaterThanOrEqualTo(1);
      assertThat(stats.parkHandOffs).isGreaterThanOrEqualTo(1);
    } finally {
      GlobalConfiguration.COMMIT_LOCK_SPIN.setValue(previous);
    }
  }

  private record HandOffs(long spinHandOffs, long parkHandOffs, long spinsExpired) {
  }

  /**
   * Holds the type's bucket file lock while another thread commits an insert into it, until that commit is queued on the
   * lock, then for a while longer than any spin, and releases it. Returns the change in the lock stats.
   */
  private HandOffs commitBehindAHeldFileLock() throws Exception {
    final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();
    final int fileId = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
    final Map<String, Object> before = txManager.getStats();

    final Object holder = new Object();
    assertThat(txManager.tryLockFiles(List.of(fileId), 0, holder)).containsExactly(fileId);

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread committer = new Thread(() -> {
      try {
        database.transaction(() -> database.newDocument(TYPE).set("v", 1).save());
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "committer");
    try {
      committer.start();
      awaitQueued(txManager);
      // far longer than a 50us spin: a queued commit that spins runs out of it and parks
      Thread.sleep(50);
    } finally {
      txManager.unlockFilesInOrder(List.of(fileId), holder);
    }
    committer.join(30_000);

    assertThat(failure.get()).isNull();
    assertThat(database.countType(TYPE, false)).isEqualTo(1);

    final Map<String, Object> after = txManager.getStats();
    return new HandOffs(delta(before, after, "lockSpinHandOffs"), delta(before, after, "lockParkHandOffs"),
        delta(before, after, "lockSpinsExpired"));
  }

  private static void awaitQueued(final TransactionManager txManager) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 20_000;
    while (System.currentTimeMillis() < deadline) {
      for (final LockManager.LockStats s : txManager.getLockStats())
        if (s.waiters() > 0)
          return;
      Thread.sleep(1);
    }
    assertThat(txManager.getLockStats()).as("a commit queued on the held file lock")
        .anySatisfy(s -> assertThat(s.waiters()).isPositive());
  }

  private static long delta(final Map<String, Object> before, final Map<String, Object> after,
      final String key) {
    return ((Number) after.get(key)).longValue() - ((Number) before.get(key)).longValue();
  }
}
