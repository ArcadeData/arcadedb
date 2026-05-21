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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #4278: TransactionContext.lockFilesInOrder must throw ConcurrentModificationException
 * when a locked file has been migrated by LSM compaction, matching checkExplicitLocks() behavior.
 * The thrown exception must identify the migration (not just say "file removed") so callers can retry
 * with correct file references.
 */
class LockFilesInOrderFileMigrationTest extends TestHelper {

  private static final int PAGE_SIZE = 2 * 1024;

  @Test
  void lockFilesInOrderThrowsWithMigrationMessageWhenFileMigratedByCompaction() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Article").withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex("Article", new String[]{ "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    // Insert enough records so the mutable index has >= 2 pages (compaction requires >= 2 pages)
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Article").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Article[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(bucketIndex.getMutableIndex().getTotalPages())
        .as("Mutable index must have >= 2 pages for compaction to proceed")
        .isGreaterThanOrEqualTo(2);

    final int oldMutableFid = bucketIndex.getMutableIndex().getFileId();

    // Begin a transaction and inject the old mutable file ID into modifiedPages by
    // reading and touching its root page. This simulates a transaction that captured
    // a stale file reference before compaction ran and swapped the underlying file.
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final var rootPage = tx.getPageToModify(new PageId(database, oldMutableFid, 0), PAGE_SIZE, false);
    // writeByte calls checkBoundariesOnWrite which updates modifiedRange so the page
    // passes the "range[1] > 0" gate in commit1stPhase and reaches lockFilesInOrder
    rootPage.writeByte(0, rootPage.readByte(0));

    // Run compaction from a background thread. Transactions are thread-local, so the
    // background thread sees no active transaction and splitIndex() can proceed.
    final CountDownLatch compactionDone = new CountDownLatch(1);
    final AtomicReference<Throwable> compactionError = new AtomicReference<>();

    new Thread(() -> {
      try {
        bucketIndex.scheduleCompaction();
        final boolean compacted = bucketIndex.compact();
        if (!compacted)
          compactionError.set(new AssertionError("compact() returned false — mutable must have >= 2 pages"));
      } catch (final Throwable e) {
        compactionError.set(e);
      } finally {
        compactionDone.countDown();
      }
    }, "compaction-thread").start();

    assertThat(compactionDone.await(30, TimeUnit.SECONDS))
        .as("Compaction thread did not complete within 30 s")
        .isTrue();
    assertThat(compactionError.get())
        .as("Compaction thread threw: %s", compactionError.get())
        .isNull();

    // Verify compaction set up the migration state
    final Integer newMutableFid = database.getSchema().getEmbedded().getMigratedFileId(oldMutableFid);
    assertThat(newMutableFid)
        .as("Migration entry must be present after compaction")
        .isNotNull();
    assertThat(((DatabaseInternal) database).getFileManager().existsFile(oldMutableFid))
        .as("Old mutable file must be gone after compaction")
        .isFalse();

    // Commit the transaction: lockFilesInOrder will detect the migration.
    // Before the fix it silently continued (or threw a generic "file removed" later from
    // checkPageVersion). After the fix it throws immediately with a migration-specific message.
    assertThatThrownBy(database::commit)
        .as("commit must throw ConcurrentModificationException with a migration-specific message when the mutable file was migrated by compaction")
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("migrated");
  }
}
