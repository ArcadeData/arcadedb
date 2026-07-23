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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalTransactionExplicitLock;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the explicit-lock vs background-compaction race surfaced by the single-server load
 * test. A compaction that migrates a mutable index file between the moment a {@code LOCK TYPE} snapshots the
 * file ids and the moment those locks are acquired also produces a fresh compacted sub-index file. The lock
 * acquisition transparently re-resolves the migrated mutable, but the transaction must still commit cleanly:
 * every current component file of the type's indexes has to be covered, not just the migrated mutable.
 */
class ExplicitLockCompactionRaceTest extends TestHelper {

  private static final int PAGE_SIZE = 2 * 1024;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
  }

  @Test
  void commitSucceedsWhenCompactionMigratesLockedFileMidLock() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Photo").withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex("Photo", new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("Photo").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Photo[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    assertThat(bucketIndex.getMutableIndex().getTotalPages()).isGreaterThanOrEqualTo(2);

    final int oldMutableFid = bucketIndex.getMutableIndex().getFileId();

    // Snapshot the file ids BEFORE compaction by building the explicit lock but not yet acquiring it.
    database.begin();
    final LocalTransactionExplicitLock explicitLock = (LocalTransactionExplicitLock) database.acquireLock().type("Photo");

    // Migrate the snapshotted mutable from a background thread (splitIndex must run outside an active TX).
    final CountDownLatch compactionDone = new CountDownLatch(1);
    final AtomicReference<Throwable> compactionError = new AtomicReference<>();
    new Thread(() -> {
      try {
        bucketIndex.scheduleCompaction();
        if (!bucketIndex.compact())
          compactionError.set(new AssertionError("compact() returned false - mutable must have >= 2 pages"));
      } catch (final Throwable e) {
        compactionError.set(e);
      } finally {
        compactionDone.countDown();
      }
    }, "compaction-thread").start();

    assertThat(compactionDone.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(compactionError.get()).isNull();
    assertThat(((DatabaseInternal) database).getFileManager().existsFile(oldMutableFid)).isFalse();

    // The lock acquisition re-resolves the migrated mutable, but the racing compaction produced a fresh
    // compacted sub-index the snapshot could not know. The commit-time coverage check must surface this as a
    // RETRYABLE conflict (a component of a locked index changed), not a hard TransactionException, so that
    // COMMIT RETRY re-runs the LOCK block against the now-stable component set.
    assertThatThrownBy(() -> {
      explicitLock.lock();
      database.newDocument("Photo").set("id", 1_000_000).save();
      database.commit();
    }).isInstanceOf(NeedRetryException.class);

    // Emulate the COMMIT RETRY: with the compaction settled, a fresh LOCK TYPE + write commits cleanly.
    database.transaction(() -> {
      database.acquireLock().type("Photo").lock();
      database.newDocument("Photo").set("id", 1_000_000).save();
    });

    assertThat(database.countType("Photo", true)).isEqualTo(2_001);
  }
}
