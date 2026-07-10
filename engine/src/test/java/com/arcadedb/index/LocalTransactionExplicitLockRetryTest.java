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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalTransactionExplicitLock;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the internal-retry behavior of LocalTransactionExplicitLock.lock().
 * When a compaction migrates an indexed file between snapshot (.type()) and lock acquisition
 * (.lock()), lock() must re-resolve current file IDs from the schema and retry transparently
 * rather than surfacing ConcurrentModificationException to the caller.
 */
class LocalTransactionExplicitLockRetryTest extends TestHelper {

  private static final int PAGE_SIZE = 2 * 1024;

  @Test
  void lockSucceedsAfterCompactionMigratesSnapshottedFile() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Article").withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex("Article", new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    // Populate so the mutable index has >= 2 pages (compaction requires >= 2 pages).
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Article").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Article[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(bucketIndex.getMutableIndex().getTotalPages()).isGreaterThanOrEqualTo(2);

    final int oldMutableFid = bucketIndex.getMutableIndex().getFileId();

    // Snapshot file IDs BEFORE compaction by calling .type() on the explicit-lock builder.
    // This emulates the production race: a LOCK TYPE statement reads idx.getFileId() and
    // the index then gets compacted before lockFilesInOrder runs.
    database.begin();
    final LocalTransactionExplicitLock explicitLock = (LocalTransactionExplicitLock) database.acquireLock().type("Article");

    // Force compaction from a background thread (transactions are thread-local; splitIndex must run
    // outside an active TX). After compaction, oldMutableFid is gone and migrated to a new file ID.
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

    // Confirm the race condition is set up: snapshotted file is gone, migration entry exists.
    assertThat(((DatabaseInternal) database).getFileManager().existsFile(oldMutableFid)).isFalse();
    assertThat(database.getSchema().getEmbedded().getMigratedFileId(oldMutableFid)).isNotNull();

    // The actual assertion: lock() must converge via internal retry with re-resolved file IDs.
    // Before the fix this would surface ConcurrentModificationException; after the fix it succeeds silently.
    explicitLock.lock();

    database.rollback();
  }
}
