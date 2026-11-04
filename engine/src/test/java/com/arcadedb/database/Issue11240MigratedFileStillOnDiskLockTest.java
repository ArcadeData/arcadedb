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
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11240: an HA leader keeps the file a compaction replaced until the compaction is published, so for a while the old
 * file is still on disk although the schema already maps it to its replacement. A transaction that resolved the old id
 * before the swap must lock the replacement, not the old file: the index entries it writes at commit land in the
 * replacement, whose lock it would otherwise never hold. The lock acquisition used to re-resolve a migrated id only once the
 * old file was gone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11240MigratedFileStillOnDiskLockTest extends TestHelper {

  @Test
  void aLockOnAMigratedFileStillOnDiskLocksItsReplacement() throws InterruptedException {
    database.getSchema().createDocumentType("Retired", 1);
    database.getSchema().createDocumentType("Replacement", 1);
    final int retired = database.getSchema().getType("Retired").getBuckets(false).getFirst().getFileId();
    final int replacement = database.getSchema().getType("Replacement").getBuckets(false).getFirst().getFileId();

    // what a deferred compaction leaves for a while: both files on disk, the old one mapped to the new one
    ((LocalSchema) database.getSchema()).setMigratedFileId(retired, replacement, false);
    assertThat(((DatabaseInternal) database).getFileManager().existsFile(retired)).isTrue();

    database.begin();
    try {
      database.acquireLock().bucket(database.getSchema().getType("Retired").getBuckets(false).getFirst().getName()).lock();

      assertThat(lockableByAnotherThread(replacement)).as("the replacement is locked").isFalse();
      assertThat(lockableByAnotherThread(retired)).as("the retired file is not held").isTrue();
    } finally {
      database.rollback();
    }
  }

  private boolean lockableByAnotherThread(final int fileId) throws InterruptedException {
    final AtomicReference<LockManager.LOCK_STATUS> status = new AtomicReference<>();
    final Thread other = new Thread(() -> {
      final LockManager.LOCK_STATUS locked = ((DatabaseInternal) database).getTransactionManager()
          .tryLockFile(fileId, 1, Thread.currentThread());
      status.set(locked);
      if (locked == LockManager.LOCK_STATUS.YES)
        ((DatabaseInternal) database).getTransactionManager().unlockFile(fileId, Thread.currentThread());
    });
    other.start();
    other.join();
    return status.get() == LockManager.LOCK_STATUS.YES;
  }
}
