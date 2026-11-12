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

import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11526: a transaction that writes a UNIQUE key and then updates an EXTERNAL property of the record must still be refused
 * when a concurrent transaction commits the same key into another bucket.
 * <p>
 * The update is deferred to the commit, where it writes the value into the paired {@code <bucket>_ext} bucket. That file was not
 * in the lock set computed before, so the commit found it as a late joiner (#4937) and released EVERY lock it held to take a wider
 * set, after the unique-key check had already passed: in that window the concurrent transaction committed the same key into
 * another bucket's sub-index, and nothing checked the key again. A type with a single bucket was not affected, because both
 * transactions then write the same index page and one of them fails its version check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11526UniqueKeyExternalPropertyUpdateRaceTest {
  private static final int BUCKETS = 8;
  private static final int KEYS    = 40;

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-11526-unique-key-external-update");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The reported shape: create the record, then UPDATE its EXTERNAL property, in each of two transactions racing on one key. */
  @Test
  void concurrentCreateAndExternalUpdateOfTheSameKeyKeepsTheKeyUnique() throws Exception {
    createType(true);
    assertKeysAreUnique(race((name, thread) -> {
      database.newDocument("T").set("name", name).save();
      database.command("sql", "update T set ext = 'x' where name = :name", Map.of("name", name));
    }));
  }

  /**
   * The same window without a record created in the transaction: an existing record takes the key in a deferred update that also
   * writes its EXTERNAL property, while the other transaction inserts the key.
   */
  @Test
  void deferredUpdateOfAnExistingRecordToATakenKeyWithAnExternalPropertyIsRefused() throws Exception {
    createType(true);

    final List<RID> existing = new ArrayList<>(KEYS);
    database.transaction(() -> {
      for (int k = 0; k < KEYS; k++)
        existing.add(database.newDocument("T").set("name", "old-" + k).save().getIdentity());
    });

    // THE BUCKETS ARE PICKED ROUND ROBIN, SO THE RECORD INSERTED FOR key-k LANDS IN THE BUCKET OF existing[k]: THE UPDATED RECORD
    // IS TAKEN A FEW BUCKETS AWAY, OR BOTH WRITE THE SAME INDEX PAGE AND THE VERSION CHECK REFUSES ONE WITHOUT THE KEY CHECK
    final Map<String, RID> recordPerKey = new HashMap<>();
    for (int k = 0; k < KEYS; k++)
      recordPerKey.put("key-" + k, existing.get((k + 3) % KEYS));

    assertKeysAreUnique(race((name, thread) -> {
      if (thread == 0)
        database.lookupByRID(recordPerKey.get(name), true).asDocument().modify().set("name", name).set("ext", "x").save();
      else
        database.newDocument("T").set("name", name).save();
    }));
  }

  /** Control: an inline (non-EXTERNAL) property never joined the lock set late, so the race was always refused. */
  @Test
  void theSameRaceWithAnInlinePropertyIsRefused() throws Exception {
    createType(false);
    assertKeysAreUnique(race((name, thread) -> {
      database.newDocument("T").set("name", name).save();
      database.command("sql", "update T set ext = 'x' where name = :name", Map.of("name", name));
    }));
  }

  /**
   * The lock set a commit takes before its unique-key check already covers the paired external bucket the deferred update will
   * write, so the commit never has to release its locks to take it.
   */
  @Test
  void theLockSetTakenUpFrontCoversThePairedExternalBucketOfADeferredUpdate() throws Exception {
    createType(true);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("T").set("name", "a").save().getIdentity());

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("T");
    final Integer externalBucketId = type.getExternalBucketIdFor(rid[0].getBucketId());
    assertThat(externalBucketId).as("the type pairs an external bucket with bucket %d", rid[0].getBucketId()).isNotNull();

    database.begin();
    try {
      database.lookupByRID(rid[0], true).asDocument().modify().set("ext", "x").save();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final Method lockFilesFromChanges = TransactionContext.class.getDeclaredMethod("lockFilesFromChanges");
      lockFilesFromChanges.setAccessible(true);
      final IntHashSet files = (IntHashSet) lockFilesFromChanges.invoke(tx);

      assertThat(files.contains(rid[0].getBucketId())).as("the record's own bucket is locked").isTrue();
      assertThat(files.contains(externalBucketId)).as("the paired external bucket %d the deferred update writes is locked up front",
          externalBucketId).isTrue();
    } finally {
      database.rollback();
    }

    // And the commit itself goes through, writing the value into the external bucket.
    database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("ext", "y").save());
    assertThat(database.lookupByRID(rid[0], true).asDocument().getString("ext")).isEqualTo("y");
  }

  /**
   * A file whose lock an enclosing frame of the same thread already holds (a sparse vector index flushing its own transaction under
   * its file lock) is left out of the commit's own lock list, so the late-joiner coverage finds it. It is covered, and only that
   * frame may release it: the commit must not take it over and release it on its way out.
   */
  @Test
  void aFileLockHeldByAnEnclosingFrameOfTheSameThreadSurvivesTheCommit() throws Exception {
    database.getSchema().createDocumentType("Plain", 1);
    final int fileId = database.getSchema().getType("Plain").getBuckets(false).getFirst().getFileId();
    final TransactionManager manager = ((DatabaseInternal) database).getTransactionManager();

    assertThat(manager.tryLockFile(fileId, 5_000, Thread.currentThread())).isEqualTo(LockManager.LOCK_STATUS.YES);
    try {
      database.transaction(() -> database.newDocument("Plain").set("name", "a").save());
      assertThat(database.countType("Plain", false)).isEqualTo(1);

      final LockManager.LOCK_STATUS[] fromAnotherThread = new LockManager.LOCK_STATUS[1];
      final Object anotherRequester = new Object();
      final Thread other = new Thread(() -> {
        fromAnotherThread[0] = manager.tryLockFile(fileId, 50, anotherRequester);
        if (fromAnotherThread[0] == LockManager.LOCK_STATUS.YES)
          manager.unlockFile(fileId, anotherRequester);
      });
      other.start();
      other.join();
      assertThat(fromAnotherThread[0]).as("the lock of file %d is still held by the frame that took it", fileId)
          .isEqualTo(LockManager.LOCK_STATUS.NO);
    } finally {
      manager.unlockFile(fileId, Thread.currentThread());
    }
  }

  private void createType(final boolean external) {
    final DocumentType type = database.getSchema().createDocumentType("T", BUCKETS);
    type.createProperty("name", Type.STRING);
    type.createProperty("ext", Type.STRING).setExternal(external);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
  }

  @FunctionalInterface
  private interface TransactionBody {
    void run(String name, int thread) throws Exception;
  }

  /** Two threads per key run {@code body} and commit together; returns the commits that succeeded per key. */
  private Map<String, Integer> race(final TransactionBody body) throws Exception {
    final Map<String, Integer> committed = new HashMap<>();
    final AtomicInteger unexpected = new AtomicInteger();
    for (int k = 0; k < KEYS; k++) {
      final String name = "key-" + k;
      final CyclicBarrier beforeCommit = new CyclicBarrier(2);
      final AtomicInteger commits = new AtomicInteger();
      final Thread[] threads = new Thread[2];
      for (int t = 0; t < 2; t++) {
        final int thread = t;
        threads[t] = new Thread(() -> {
          database.begin();
          try {
            body.run(name, thread);
            beforeCommit.await();
            database.commit();
            commits.incrementAndGet();
          } catch (final DuplicatedKeyException | NeedRetryException e) {
            // REFUSED: THE OTHER TRANSACTION WON THE KEY
            if (database.isTransactionActive())
              database.rollback();
          } catch (final Throwable e) {
            unexpected.incrementAndGet();
            beforeCommit.reset();
            if (database.isTransactionActive())
              database.rollback();
          }
        });
        threads[t].start();
      }
      for (final Thread thread : threads)
        thread.join();
      committed.put(name, commits.get());
    }
    assertThat(unexpected.get()).as("transactions that failed with anything but a refusal of the key").isZero();
    return committed;
  }

  private void assertKeysAreUnique(final Map<String, Integer> committed) {
    // A SCAN OF THE TYPE, NOT AN INDEX LOOKUP: THE DUPLICATE IS A RECORD THE INDEX DOES NOT HOLD
    final Map<String, Integer> recordsPerKey = new HashMap<>();
    final Iterator<Record> scan = database.iterateType("T", false);
    while (scan.hasNext())
      recordsPerKey.merge(scan.next().asDocument().getString("name"), 1, Integer::sum);

    for (final Map.Entry<String, Integer> entry : committed.entrySet()) {
      final int records = recordsPerKey.getOrDefault(entry.getKey(), 0);
      assertThat(records).as("records holding the key %s (commits that succeeded: %d)", entry.getKey(), entry.getValue())
          .isLessThanOrEqualTo(1);
      assertThat(entry.getValue()).as("commits of the key %s: at most one of the two transactions may commit it", entry.getKey())
          .isLessThanOrEqualTo(1);

      // THE INDEX AGREES WITH THE SCAN
      int indexed = 0;
      for (final IndexCursor cursor = database.lookupByKey("T", "name", entry.getKey()); cursor.hasNext(); cursor.next())
        ++indexed;
      assertThat(indexed).as("index entries for the key %s", entry.getKey()).isEqualTo(records);
    }
  }
}
