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
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.IntHashSet;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4529: a background index compaction migrates an indexed file between the moment its
 * file id is resolved for locking and the moment the commit acquires + verifies that lock. Before the fix the
 * implicit (non-explicit-lock) commit path surfaced a {@code ConcurrentModificationException} ("file has been
 * migrated to ... Please retry the operation"). After the fix {@code TransactionContext.lockFilesInOrder} re-resolves
 * the migrated id and retries transparently, mirroring the explicit-lock internal-retry behavior.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LocalTransactionCommitLockRetryTest extends TestHelper {

  private static final int PAGE_SIZE = 2 * 1024;

  @Test
  void commitLockSucceedsAfterCompactionMigratesSnapshottedFile() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("City").withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex("City", new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    // Populate so the mutable index has >= 2 pages (compaction requires >= 2 pages).
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("City").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("City[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(bucketIndex.getMutableIndex().getTotalPages()).isGreaterThanOrEqualTo(2);

    final int staleMutableFid = bucketIndex.getMutableIndex().getFileId();

    // Compact OUTSIDE any active transaction: this migrates staleMutableFid to a new file id, removes the old file,
    // and registers the migration entry. This reproduces the post-condition of the production race where a commit
    // had already snapshotted the now-stale file id before the compaction ran.
    bucketIndex.scheduleCompaction();
    assertThat(bucketIndex.compact()).isTrue();

    assertThat(((DatabaseInternal) database).getFileManager().existsFile(staleMutableFid)).isFalse();
    final Integer migratedFid = database.getSchema().getEmbedded().getMigratedFileId(staleMutableFid);
    assertThat(migratedFid).isNotNull();

    // Acquire the commit lock over the STALE file id, emulating lockFilesFromChanges having resolved it just before
    // the compaction migrated it. lockFilesInOrder must converge via internal retry on the migrated id instead of
    // throwing ConcurrentModificationException.
    database.begin();
    try {
      final TransactionContext tx = (TransactionContext) ((DatabaseInternal) database).getTransaction();

      final IntHashSet staleFiles = new IntHashSet(4);
      staleFiles.add(staleMutableFid);

      final Method lockFilesInOrder = TransactionContext.class.getDeclaredMethod("lockFilesInOrder", IntHashSet.class);
      lockFilesInOrder.setAccessible(true);

      @SuppressWarnings("unchecked")
      final List<Integer> locked = (List<Integer>) lockFilesInOrder.invoke(tx, staleFiles);

      // The lock was transparently re-resolved to the migrated file id, not the removed stale one.
      assertThat(locked).contains(migratedFid);
      assertThat(locked).doesNotContain(staleMutableFid);

      // Release the locks acquired directly via reflection (the context never recorded them).
      tx.getDatabase().getTransactionManager().unlockFilesInOrder(locked, Thread.currentThread());
    } finally {
      database.rollback();
    }
  }
}
