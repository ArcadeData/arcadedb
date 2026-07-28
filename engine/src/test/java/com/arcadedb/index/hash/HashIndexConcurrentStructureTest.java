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
package com.arcadedb.index.hash;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4743: the hash index used to cache its structural metadata (global depth, directory
 * start page, bucket count) in plain instance fields. The fields were bumped in the middle of a commit, so a
 * concurrent lookup could compute a directory index for a depth whose directory slots were not published yet -
 * reading a 0, walking into the metadata page and failing with "Detected cycle in hash index ... at page 0" - and a
 * rolled back or retried commit left the cached depth permanently ahead of the persisted one, poisoning every later
 * lookup on that index until the database was reopened.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HashIndexConcurrentStructureTest {
  private static final String DB_ROOT = "./target/databases/hash-index-concurrent-structure";

  /**
   * The structural metadata must always come from the metadata page through the current transaction, never from a
   * cached field: a change is visible as soon as it is written, and disappears when the transaction rolls back.
   */
  @Test
  void structuralMetadataIsTransactional() throws Exception {
    final String path = DB_ROOT + "-metadata";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.uid STRING");
        database.command("sql", "CREATE INDEX ON Address (uid) UNIQUE_HASH");

        insert(database, 0, 5_000);

        final HashIndex index = firstSubIndex(database, "Address[uid]");
        final HashIndexBucket bucket = index.bucket;

        final int[] depthHolder = new int[1];
        database.transaction(() -> depthHolder[0] = bucket.getGlobalDepth());
        final int persistedDepth = depthHolder[0];
        assertThat(persistedDepth).withFailMessage("the directory should have been doubled at least once").isPositive();

        // a change written inside a transaction is visible immediately...
        database.begin();
        final MutablePage metaPage = ((DatabaseInternal) database).getTransaction()
            .getPageToModify(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize(), false);
        metaPage.writeInt(HashIndexBucket.META_GLOBAL_DEPTH, persistedDepth + 1);
        assertThat(bucket.getGlobalDepth()).isEqualTo(persistedDepth + 1);

        // ...and is gone once the transaction is rolled back
        database.rollback();
        database.transaction(() -> assertThat(bucket.getGlobalDepth()).isEqualTo(persistedDepth));

        // the index is still fully usable
        database.transaction(() -> {
          for (int i = 0; i < 5_000; i += 100)
            assertThat(database.getSchema().getIndexByName("Address[uid]").get(new Object[] { "uid-" + i }).hasNext())
                .withFailMessage("key uid-" + i + " not found").isTrue();
        });
      } finally {
        if (database.isOpen())
          database.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * Concurrent writers on the same UNIQUE_HASH index: before the fix this failed within a second with
   * "Detected cycle in hash index ... overflow chain at page 0" raised by the unique-constraint check.
   */
  @Test
  void concurrentWritersOnUniqueHashIndex() throws Exception {
    final String path = DB_ROOT + "-writers";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.uid STRING");
        database.command("sql", "CREATE INDEX ON Address (uid) UNIQUE_HASH");

        final int threads = 8;
        final int perThread = 5_000;
        final int batch = 250;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> workers = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
          final int threadId = t;
          final Thread worker = new Thread(() -> {
            try {
              start.await();
              for (int i = 0; i < perThread; i += batch) {
                final int from = i;
                final int to = Math.min(i + batch, perThread);
                database.transaction(() -> {
                  for (int k = from; k < to; k++) {
                    final MutableVertex v = database.newVertex("Address");
                    v.set("uid", threadId + "-" + UUID.randomUUID());
                    v.save();
                  }
                }, true, 500);
              }
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
          }, "hash-writer-" + t);
          workers.add(worker);
          worker.start();
        }

        start.countDown();
        for (final Thread w : workers)
          w.join();

        if (failure.get() != null)
          throw new AssertionError("concurrent writer failed", failure.get());

        validateStructure(database, "Address[uid]", "after the concurrent load");
        assertThat(database.countType("Address", true)).isEqualTo((long) threads * perThread);
      } finally {
        if (database.isOpen())
          database.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * Lookups running while other threads write: a lookup must never observe a half-published directory doubling.
   */
  @Test
  void lookupsWhileTheDirectoryIsDoubling() throws Exception {
    final String path = DB_ROOT + "-readers";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.uid STRING");
        database.command("sql", "CREATE INDEX ON Address (uid) UNIQUE_HASH");

        final int writers = 4;
        final int readers = 4;
        final int perWriter = 10_000;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean stopReaders = new AtomicBoolean();
        final List<Thread> writerThreads = new ArrayList<>();
        final List<Thread> readerThreads = new ArrayList<>();

        for (int t = 0; t < writers; t++) {
          final int threadId = t;
          writerThreads.add(new Thread(() -> {
            try {
              start.await();
              for (int i = 0; i < perWriter; i += 250) {
                final int from = i;
                final int to = Math.min(i + 250, perWriter);
                database.transaction(() -> {
                  for (int k = from; k < to; k++) {
                    final MutableVertex v = database.newVertex("Address");
                    v.set("uid", "w" + threadId + "-" + k);
                    v.save();
                  }
                }, true, 500);
              }
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
          }, "hash-writer-" + t));
        }

        for (int t = 0; t < readers; t++) {
          final int threadId = t % writers;
          readerThreads.add(new Thread(() -> {
            try {
              start.await();
              while (!stopReaders.get())
                database.transaction(() -> {
                  for (int k = 0; k < 200; k++)
                    database.getSchema().getIndexByName("Address[uid]").get(new Object[] { "w" + threadId + "-" + k });
                });
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
          }, "hash-reader-" + t));
        }

        writerThreads.forEach(Thread::start);
        readerThreads.forEach(Thread::start);
        start.countDown();

        for (final Thread t : writerThreads)
          t.join();
        stopReaders.set(true);
        for (final Thread t : readerThreads)
          t.join();

        if (failure.get() != null)
          throw new AssertionError("concurrent lookup failed", failure.get());

        validateStructure(database, "Address[uid]", "after the concurrent read/write load");

        database.transaction(() -> {
          for (int w = 0; w < writers; w++)
            for (int k = 0; k < perWriter; k += 97)
              assertThat(database.getSchema().getIndexByName("Address[uid]").get(new Object[] { "w" + w + "-" + k }).hasNext())
                  .withFailMessage("key w" + w + "-" + k + " not found").isTrue();
        });
      } finally {
        if (database.isOpen())
          database.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * A NOTUNIQUE_HASH index on a low-cardinality property: buckets cannot split, so the overflow chains grow long
   * and every insert walks them. Verifies that the chains stay acyclic and that no entry is lost.
   */
  @Test
  void notUniqueHashIndexWithLongOverflowChains() {
    final String path = DB_ROOT + "-notunique";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.kind STRING");
        database.command("sql", "CREATE INDEX ON Address (kind) NOTUNIQUE_HASH");

        final int total = 20_000;
        final int distinct = 8;

        for (int i = 0; i < total; i += 5_000) {
          final int from = i;
          final int to = Math.min(i + 5_000, total);
          database.transaction(() -> {
            for (int k = from; k < to; k++) {
              final MutableVertex v = database.newVertex("Address");
              v.set("kind", "kind-" + (k % distinct));
              v.save();
            }
          });
          validateStructure(database, "Address[kind]", "after " + to + " inserts");
        }

        database.transaction(() -> {
          long found = 0;
          for (int d = 0; d < distinct; d++) {
            final var cursor = database.getSchema().getIndexByName("Address[kind]").get(new Object[] { "kind-" + d });
            while (cursor.hasNext()) {
              cursor.next();
              found++;
            }
          }
          assertThat(found).isEqualTo(total);
        });
      } finally {
        if (database.isOpen())
          database.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * The directory is relocated (copy on write) on every doubling, so the file grows past the pages the directory
   * itself needs: verify that a reopened index keeps finding every key, i.e. that the persisted directory start
   * page is honoured.
   */
  @Test
  void directoryRelocationSurvivesReopen() {
    final String path = DB_ROOT + "-reopen";
    FileUtils.deleteRecursively(new File(path));
    try {
      try (final DatabaseFactory factory = new DatabaseFactory(path)) {
        final Database database = factory.create();
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.uid STRING");
        database.command("sql", "CREATE INDEX ON Address (uid) UNIQUE_HASH");
        insert(database, 0, 20_000);
        database.close();
      }

      try (final DatabaseFactory factory = new DatabaseFactory(path)) {
        final Database database = factory.open();
        try {
          database.transaction(() -> {
            for (int i = 0; i < 20_000; i += 13)
              assertThat(database.getSchema().getIndexByName("Address[uid]").get(new Object[] { "uid-" + i }).hasNext())
                  .withFailMessage("key uid-" + i + " not found after reopen").isTrue();
          });
          validateStructure(database, "Address[uid]", "after reopen");

          // keep writing on the reopened index
          insert(database, 20_000, 30_000);
          validateStructure(database, "Address[uid]", "after writing on the reopened index");
          assertThat(database.countType("Address", true)).isEqualTo(30_000);
        } finally {
          if (database.isOpen())
            database.drop();
        }
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  private void insert(final Database database, final int from, final int to) {
    for (int i = from; i < to; i += 1_000) {
      final int batchFrom = i;
      final int batchTo = Math.min(i + 1_000, to);
      database.transaction(() -> {
        for (int k = batchFrom; k < batchTo; k++) {
          final MutableVertex v = database.newVertex("Address");
          v.set("uid", "uid-" + k);
          v.save();
        }
      });
    }
  }

  private HashIndex firstSubIndex(final Database database, final String indexName) {
    for (final IndexInternal sub : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getSubIndexes())
      if (sub instanceof HashIndex hashIndex)
        return hashIndex;
    throw new IllegalStateException("no hash sub-index found for '" + indexName + "'");
  }

  private void validateStructure(final Database database, final String indexName, final String phase) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final List<String> problems = new ArrayList<>();
    database.transaction(() -> {
      for (final IndexInternal sub : typeIndex.getSubIndexes())
        if (sub instanceof HashIndex hashIndex)
          for (final String problem : hashIndex.bucket.checkStructuralIntegrity())
            problems.add(hashIndex.getName() + ": " + problem);
    });
    assertThat(problems).withFailMessage("structural corruption " + phase + ": " + problems).isEmpty();
  }
}
