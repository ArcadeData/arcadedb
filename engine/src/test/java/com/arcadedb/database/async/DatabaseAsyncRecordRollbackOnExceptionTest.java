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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for DatabaseAsyncCreateRecord, DatabaseAsyncUpdateRecord, and
 * DatabaseAsyncDeleteRecord not rolling back the shared async transaction when an
 * exception is thrown inside execute(). Without the fix, the failed task's dirty
 * pages and transaction cache remain in the long-lived batch transaction, which
 * is then committed by the periodic commit cycle, producing phantom records or
 * corrupted data.
 *
 * <p>Each test injects a RuntimeException via a record-event listener that fires
 * after the storage operation succeeds but before execute() returns. With
 * commitEvery=1 the bug manifests immediately: the outer loop sees an active
 * transaction (not rolled back), calls commit(), and persists the dirty state.
 * With the fix the catch block calls rollback(), the outer loop sees no active
 * transaction, and the dirty state is discarded.
 */
class DatabaseAsyncRecordRollbackOnExceptionTest extends TestHelper {

  private static final String TYPE = "Item";

  /**
   * A RuntimeException thrown inside DatabaseAsyncCreateRecord.execute() (via the
   * AfterRecordCreateListener) must roll back the async transaction. Without the
   * fix, the dirty bucket page is committed at the next commit-every boundary,
   * producing an extra phantom record. With the fix only the two subsequent
   * creates that succeed are persisted.
   */
  @Test
  void createRecordExceptionRollsBackTransaction() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE));

    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger listenerCalls = new AtomicInteger();

    final AfterRecordCreateListener throwOnce = record -> {
      if (listenerCalls.incrementAndGet() == 1)
        throw new RuntimeException("injected error on first create");
    };

    database.async().setCommitEvery(1);
    database.async().onError(e -> errors.incrementAndGet());

    database.getEvents().registerListener(throwOnce);
    try {
      // First create: listener throws; the async transaction must be rolled back.
      database.async().createRecord(database.newDocument(TYPE), null);
      // Two creates that succeed; they must still be committed after the rollback.
      database.async().createRecord(database.newDocument(TYPE), null);
      database.async().createRecord(database.newDocument(TYPE), null);

      database.async().waitCompletion();
    } finally {
      database.getEvents().unregisterListener(throwOnce);
    }

    assertThat(errors.get()).isEqualTo(1);
    // Only the two successful creates must be present; the first (rolled back) must not.
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(2));
  }

  /**
   * A RuntimeException thrown inside DatabaseAsyncUpdateRecord.execute() (via the
   * AfterRecordUpdateListener, fired after updateRecordNoLock) must roll back the
   * async transaction so the dirty update is not committed.
   */
  @Test
  void updateRecordExceptionRollsBackTransaction() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      final MutableDocument doc = database.newDocument(TYPE).set("v", 0);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger listenerCalls = new AtomicInteger();

    final AfterRecordUpdateListener throwOnce = record -> {
      if (listenerCalls.incrementAndGet() == 1)
        throw new RuntimeException("injected error on first update");
    };

    database.async().setCommitEvery(1);
    database.async().onError(e -> errors.incrementAndGet());

    database.getEvents().registerListener(throwOnce);
    try {
      // Update that fails (listener throws after updateRecordNoLock writes the dirty page).
      final MutableDocument dirtyUpdate = ((Document) database.lookupByRID(rid[0], true)).modify();
      dirtyUpdate.set("v", 99);
      database.async().updateRecord(dirtyUpdate, null);

      // Create a new record that must still succeed after the rollback.
      database.async().createRecord(database.newDocument(TYPE).set("v", 1), null);

      database.async().waitCompletion();
    } finally {
      database.getEvents().unregisterListener(throwOnce);
    }

    assertThat(errors.get()).isEqualTo(1);
    // Two records total; the original record must keep v=0 (update was rolled back).
    database.transaction(() -> {
      assertThat(database.countType(TYPE, true)).isEqualTo(2);
      final MutableDocument after = ((Document) database.lookupByRID(rid[0], true)).modify();
      assertThat(after.getInteger("v")).isEqualTo(0);
    });
  }

  /**
   * A RuntimeException thrown inside DatabaseAsyncDeleteRecord.execute() (via the
   * AfterRecordDeleteListener, fired from inside deleteRecordNoLock after the bucket
   * record is marked as deleted) must roll back the async transaction so the record
   * is not permanently deleted.
   */
  @Test
  void deleteRecordExceptionRollsBackTransaction() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      final MutableDocument doc = database.newDocument(TYPE);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger listenerCalls = new AtomicInteger();

    final AfterRecordDeleteListener throwOnce = record -> {
      if (listenerCalls.incrementAndGet() == 1)
        throw new RuntimeException("injected error on first delete");
    };

    database.async().setCommitEvery(1);
    database.async().onError(e -> errors.incrementAndGet());

    database.getEvents().registerListener(throwOnce);
    try {
      // Delete that fails (listener throws after deleteRecordNoLock marks the record deleted).
      database.async().deleteRecord(database.lookupByRID(rid[0], true), null);

      // Create a new record that must still succeed after the rollback.
      database.async().createRecord(database.newDocument(TYPE), null);

      database.async().waitCompletion();
    } finally {
      database.getEvents().unregisterListener(throwOnce);
    }

    assertThat(errors.get()).isEqualTo(1);
    // Original record must still exist (delete was rolled back) plus the new create.
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(2));
  }
}
