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
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.LockManager;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4959: low-severity transaction-layer cleanups.
 * <ul>
 * <li>A deferred update whose record was deleted by a concurrent transaction must FAIL the commit with a retryable
 * conflict instead of being silently skipped while the rest of the transaction commits (silent partial commit).</li>
 * <li>{@code transaction()} must not burn every retry attempt on a deterministic {@link DuplicatedKeyException}:
 * only a concurrency-induced duplicate can succeed on retry, and one retry is enough to disambiguate.</li>
 * <li>{@code executeLockingFiles} must lock on behalf of the current transaction's requester (thread or session),
 * so a thread acting for a session does not time out on locks its own session already holds.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4959TransactionCleanupsTest extends TestHelper {

  private static final String TYPE         = "Issue4959Doc";
  private static final String TYPE_INDEXED = "Issue4959Indexed";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      final DocumentType indexed = database.getSchema().createDocumentType(TYPE_INDEXED);
      indexed.createProperty("id", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_INDEXED, "id");
    });
  }

  @Test
  void deferredUpdateOfConcurrentlyDeletedRecordFailsTheCommit() throws Exception {
    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE);
      doc.set("name", "committed");
      doc.save();
      holder[0] = doc;
    });
    final RID rid = holder[0].getIdentity();

    database.begin();
    final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();

    // Concurrent transaction (own thread = own transaction context) deletes the record and commits. The current
    // transaction holds no locks yet, so the delete goes through.
    final Thread concurrentDelete = new Thread(
        () -> database.transaction(() -> database.deleteRecord(database.lookupByRID(rid, true))));
    concurrentDelete.start();
    concurrentDelete.join();

    // Deferred update: the page is loaded AFTER the delete committed, so the MVCC page-version check cannot see
    // the conflict. Before the fix the commit succeeded and the update was silently dropped.
    doc.set("name", "lost");
    doc.save();

    assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void deterministicDuplicatedKeyIsRetriedAtMostOnce() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE_INDEXED);
      doc.set("id", "K");
      doc.save();
    });

    final AtomicInteger attemptsRun = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attemptsRun.incrementAndGet();
      final MutableDocument dup = database.newDocument(TYPE_INDEXED);
      dup.set("id", "K");
      dup.save();
    }, false, 10)).isInstanceOf(DuplicatedKeyException.class);

    // A genuine duplicate fails identically every attempt: one retry is enough to disambiguate from a
    // concurrency-induced duplicate. Before the fix all 10 attempts (plus retry delays) were burned.
    assertThat(attemptsRun.get()).as("a deterministic duplicate must be retried at most once").isEqualTo(2);
  }

  @Test
  void executeLockingFilesUsesTheTransactionRequester() throws Exception {
    final int fileId = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();

    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    final Object session = new Object();

    // The session (not the thread) holds the file lock, as it happens for server-side transactions keyed by session.
    final List<Integer> locked = transactionManager.tryLockFiles(List.of(fileId), 2_000, session);
    try {
      database.begin();
      ((DatabaseInternal) database).getTransaction().setRequester(session);

      // Before the fix this timed out after 5s: the lock was requested as Thread.currentThread() while the
      // same logical owner (the session) already held it.
      final Boolean executed = ((DatabaseInternal) database).executeLockingFiles(List.of(fileId), () -> true);
      assertThat(executed).isTrue();

      // The session lock must still be held afterwards (executeLockingFiles releases only what it acquired).
      assertThat(transactionManager.tryLockFile(fileId, 100, new Object())).isEqualTo(LockManager.LOCK_STATUS.NO);
    } finally {
      database.rollback();
      transactionManager.unlockFilesInOrder(locked, session);
    }
  }
}
