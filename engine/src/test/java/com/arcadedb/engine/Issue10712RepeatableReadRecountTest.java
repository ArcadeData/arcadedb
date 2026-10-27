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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10712, the recompute paths of {@link LocalBucket#count()}: when the counter is unknown the bucket is scanned in the
 * transaction's view, which already holds the transaction's own writes. A REPEATABLE_READ transaction must pin the COMMITTED
 * part of that scan, so its own writes are counted once, on the lock-free fallback (the bucket lock is held elsewhere and the
 * result is not published) as on the path that holds the lock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10712RepeatableReadRecountTest extends TestHelper {
  private static final String TYPE = "Issue10712Recount";

  @Test
  void aRecountPinsTheCommittedPartOfTheTransactionView() {
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    final TransactionManager txManager = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getTransactionManager();

    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 100L);
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // THE TRANSACTION'S OWN WRITE: A DELTA OF +1 THE SCAN OF ITS VIEW ALREADY SEES
      database.newDocument(TYPE).set("id", 1000).save();

      // THE LOCK-FREE FALLBACK: THE COUNTER IS UNKNOWN AND ANOTHER REQUESTER HOLDS THE BUCKET LOCK
      final Object holder = new Object();
      bucket.invalidateCachedRecordCountForUnlockedApply();
      assertThat(txManager.tryLockFile(bucket.getFileId(), 1000, holder)).isEqualTo(LockManager.LOCK_STATUS.YES);
      try {
        assertThat(database.countType(TYPE, true)).as("the committed 100 plus the transaction's own 1").isEqualTo(101);
      } finally {
        txManager.unlockFile(bucket.getFileId(), holder);
      }

      // THE PATH THAT HOLDS THE LOCK: THE COUNTER WAS NOT PUBLISHED, SO THIS RECOUNTS TOO, AND THE PIN IS WHAT ANSWERS
      assertThat(database.countType(TYPE, true)).isEqualTo(101);

      // AND THE O(1) PATH ONCE THE COUNTER IS KNOWN AGAIN
      assertThat(database.countType(TYPE, true)).isEqualTo(101);
    } finally {
      database.rollback();
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getDefValue());
    }

    assertThat(database.countType(TYPE, true)).isEqualTo(100);
  }
}
