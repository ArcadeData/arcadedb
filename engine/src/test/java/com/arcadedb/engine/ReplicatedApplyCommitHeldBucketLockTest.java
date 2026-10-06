/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A replicated apply must not wait for a bucket lock held by a COMMIT.
 * <p>
 * While a bucket's record counter is unknown, the apply takes the bucket's file lock so a {@code count()} recompute
 * cannot scan under it (#8640). A commit holds the same lock, and on a replica it holds it until the state machine has
 * applied the commit's own entry (#5503): the apply thread then waited the whole {@code COMMIT_LOCK_TIMEOUT} behind a
 * committer that was waiting for that very apply. Under writes issued on a follower this stalled every entry on the
 * bucket for 5 seconds, ran out the committer's read-your-writes wait, and turned
 * {@code RaftChunkChainConcurrentResizeIT} into a test of 6 to 28 minutes. A commit already excludes every recompute
 * while it holds the lock, so there is nothing to wait for: the apply goes ahead without the lock, and still makes an
 * overlapping recompute refuse its publish.
 */
class ReplicatedApplyCommitHeldBucketLockTest extends TestHelper {
  /** The test plants an unknown counter on purpose, so the blanket end-of-test integrity check would report it. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Counted");
  }

  @Test
  void anApplyDoesNotWaitForACommitHoldingTheBucketLock() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).get(0);
    final int fileId = bucket.getFileId();
    final TransactionManager txManager = db.getTransactionManager();

    bucket.setCachedRecordCount(-1);
    // Far longer than the bound below, so only an apply that does not wait at all can meet it
    final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 120_000L);

    // A committer on a replica: it holds the bucket lock until this node applies its entry
    final Object committer = new Object();
    assertThat(txManager.tryLockFile(fileId, 1000, committer)).isEqualTo(LockManager.LOCK_STATUS.YES);

    final ExecutorService applier = Executors.newSingleThreadExecutor();
    try {
      final long stampBefore = bucket.getUnlockedApplyStamp();
      final int version = (int) db.getPageManager()
          .getImmutablePage(new PageId(db, fileId, 0), ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize(),
              false, true).getVersion();
      final Future<Boolean> apply = applier.submit(
          () -> txManager.applyChanges(walTx(db, fileId, version + 1), Map.of(fileId, 5), false));

      // A hang detector, not a latency bound: the wait it separates from is the 120s commit timeout
      assertThat(apply.get(60, TimeUnit.SECONDS)).isTrue();

      // Applied without the lock, so an overlapping recompute refuses its publish and the counter stays unknown
      assertThat(bucket.getUnlockedApplyStamp()).isGreaterThan(stampBefore);
      assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);
      // Not a contention episode with a recompute: the state that throttles waits for recomputes is untouched
      assertThat(bucket.isApplyLockContended()).isFalse();
      assertThat(bucket.isApplyLockPatient()).isFalse();
    } finally {
      txManager.unlockFile(fileId, committer);
      applier.shutdownNow();
      db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
    }

    // Once the commit lets go, the next apply takes the lock as before
    final long stampBefore = bucket.getUnlockedApplyStamp();
    final int version = (int) db.getPageManager()
        .getImmutablePage(new PageId(db, fileId, 0), ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize(),
            false, true).getVersion();
    assertThat(txManager.applyChanges(walTx(db, fileId, version + 1), Map.of(fileId, 5), false)).isTrue();
    assertThat(bucket.getUnlockedApplyStamp()).isEqualTo(stampBefore);
  }

  /** A one-page entry rewriting the first bytes of the content area of page 0 with their current value. */
  private static WALFile.WALTransaction walTx(final DatabaseInternal db, final int fileId, final int version) throws Exception {
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final ImmutablePage page = db.getPageManager().getImmutablePage(new PageId(db, fileId, 0), file.getPageSize(), false, true);

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = version;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = version;
    tx.timestamp = System.currentTimeMillis();
    tx.pages = new WALFile.WALPage[] { walPage };
    return tx;
  }
}
