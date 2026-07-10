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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the lock contention issue reported in ops#433.
 *
 * A UNIQUE index on a multi-bucket type was locking ALL data bucket files at commit
 * time, causing "Timeout on locking file" errors when concurrent writes were in
 * progress during CREATE INDEX.  The fix locks only the ONE data bucket being
 * written; cross-bucket uniqueness serialisation is handled through the per-bucket
 * index file locks that are still acquired for every unique-index commit.
 */
class UniqueIndexMultiBucketLockingTest extends TestHelper {

  private static final String TYPE_NAME  = "RECEIVED_BY";
  private static final String INDEX_NAME = "RECEIVED_BY_PK";
  private static final int    BUCKETS    = 8;

  /**
   * Verifies that SELECT FROM schema:indexes returns the TypeIndex entry under
   * its user-supplied name when the type has multiple buckets.
   */
  @Test
  void schemaIndexesShowsCustomNamedTypeIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType()
          .withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      type.createProperty("transaction_id", Long.class);
    });

    database.command("sql", "CREATE INDEX " + INDEX_NAME + " ON " + TYPE_NAME + " (transaction_id) UNIQUE");

    try {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM schema:indexes WHERE name = '" + INDEX_NAME + "'")) {
        assertThat(rs.hasNext()).as("schema:indexes should return the custom-named TypeIndex").isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo(INDEX_NAME);
        assertThat(row.<Boolean>getProperty("unique")).isTrue();
        assertThat(rs.hasNext()).isFalse();
      }
    } finally {
      database.command("sql", "DROP TYPE " + TYPE_NAME + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Verifies that uniqueness is still enforced across all buckets after the
   * per-bucket locking optimisation: inserting the same transaction_id twice
   * (even into different buckets) must throw DuplicatedKeyException.
   */
  @Test
  void uniquenessEnforcedAcrossBucketsAfterOptimisation() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType()
          .withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      type.createProperty("transaction_id", Long.class);
    });

    database.command("sql", "CREATE INDEX " + INDEX_NAME + " ON " + TYPE_NAME + " (transaction_id) UNIQUE");

    try {
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("transaction_id", 42L);
        doc.save();
      });

      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("transaction_id", 42L);
        doc.save();
      })).isInstanceOf(DuplicatedKeyException.class);
    } finally {
      database.command("sql", "DROP TYPE " + TYPE_NAME + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Concurrent variant of {@link #uniquenessEnforcedAcrossBucketsAfterOptimisation()}: many threads
   * race to insert the SAME key at once. With the per-bucket locking fix exactly one insert must win
   * and the others must be rejected, proving the reduced lock scope did not weaken concurrent
   * duplicate detection.
   */
  @Test
  @Tag("slow")
  void uniquenessEnforcedUnderConcurrentDuplicateInserts() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType()
          .withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      type.createProperty("transaction_id", Long.class);
    });

    database.command("sql", "CREATE INDEX " + INDEX_NAME + " ON " + TYPE_NAME + " (transaction_id) UNIQUE");

    try {
      final int             threads   = BUCKETS;
      final long            sharedKey = 777L;
      final CountDownLatch  start     = new CountDownLatch(1);
      final CountDownLatch  done      = new CountDownLatch(threads);
      final AtomicInteger   successes = new AtomicInteger(0);
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();

      for (int t = 0; t < threads; t++) {
        new Thread(() -> {
          try {
            start.await();  // release all racers at once for maximum contention
            database.transaction(() -> {
              final MutableDocument doc = database.newDocument(TYPE_NAME);
              doc.set("transaction_id", sharedKey);
              doc.save();
            }, true, 25);
            successes.incrementAndGet();
          } catch (final DuplicatedKeyException | NeedRetryException e) {
            // Rejected: a committed duplicate was detected, or retries were exhausted under contention
          } catch (final Throwable e) {
            unexpected.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "dup-racer-" + t).start();
      }

      start.countDown();
      final boolean completed = done.await(30, TimeUnit.SECONDS);
      assertThat(completed).as("Threads did not finish within 30 s").isTrue();
      assertThat(unexpected.get()).isNull();
      // Core invariant: the per-bucket locking fix must never let two concurrent inserts of the same key both win.
      assertThat(successes.get()).as("At most one concurrent insert may win").isLessThanOrEqualTo(1);

      // On-disk record count must match the number of winners (0 if every thread retried out, else 1).
      try (final ResultSet rs = database.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE transaction_id = " + sharedKey)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo((long) successes.get());
      }
    } finally {
      database.command("sql", "DROP TYPE " + TYPE_NAME + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Regression test for ops#433 Error 1: concurrent writers on a multi-bucket
   * type with a UNIQUE index must not time out during CREATE INDEX.
   *
   * Without the fix, each writer transaction locked ALL BUCKETS data files for
   * the unique constraint check.  With 8 writers and 8 buckets, the commit window
   * filled with lock contention and CREATE INDEX batch-commits timed out.
   *
   * After the fix each writer locks only its own bucket's data file, so CREATE
   * INDEX batch-commits can proceed without being blocked by unrelated writes.
   */
  @Test
  @Tag("slow")
  void createUniqueIndexWhileConcurrentWritesDoNotTimeout() throws Exception {
    // Per-database override (not JVM-global) so the short timeout cannot leak into other tests.
    final ContextConfiguration cfg = database.getConfiguration();
    final long origTimeout = cfg.getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);
    cfg.setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 1_000L);  // short: 1 s

    try {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType()
            .withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
        type.createProperty("transaction_id", Long.class);
        // Pre-populate so CREATE INDEX has data to index
        for (int i = 0; i < 5_000; i++) {
          final MutableDocument doc = database.newDocument(TYPE_NAME);
          doc.set("transaction_id", (long) i);
          doc.save();
        }
      });

      final AtomicReference<Throwable> writerError = new AtomicReference<>();
      final AtomicReference<Throwable> unexpectedWriterError = new AtomicReference<>();
      final AtomicInteger writtenCount = new AtomicInteger(0);
      final CountDownLatch writersDone = new CountDownLatch(BUCKETS);

      // Spawn writers that keep inserting while CREATE INDEX runs
      final List<Thread> writers = new ArrayList<>(BUCKETS);
      for (int w = 0; w < BUCKETS; w++) {
        final int base = 100_000 + w * 10_000;
        final Thread t = new Thread(() -> {
          try {
            for (int i = 0; i < 200; i++) {
              try {
                database.transaction(() -> {
                  final MutableDocument doc = database.newDocument(TYPE_NAME);
                  doc.set("transaction_id", (long) (base + writtenCount.getAndIncrement()));
                  doc.save();
                });
              } catch (final Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("Timeout on locking")) {
                  writerError.compareAndSet(null, e);
                  return;
                }
                // NeedRetry/duplicate are acceptable under contention; surface anything else
                if (!(e instanceof NeedRetryException) && !(e instanceof DuplicatedKeyException)) {
                  unexpectedWriterError.compareAndSet(null, e);
                  return;
                }
              }
            }
          } finally {
            writersDone.countDown();
          }
        }, "writer-" + w);
        writers.add(t);
      }

      writers.forEach(Thread::start);

      // CREATE INDEX while the writers are running
      database.command("sql", "CREATE INDEX " + INDEX_NAME + " ON " + TYPE_NAME + " (transaction_id) UNIQUE");

      final boolean completed = writersDone.await(30, TimeUnit.SECONDS);
      assertThat(completed).as("Writer threads did not finish within 30 s").isTrue();

      assertThat(writerError.get())
          .as("No lock-timeout should occur with the per-bucket locking fix")
          .isNull();
      assertThat(unexpectedWriterError.get())
          .as("No unexpected error should occur in the writer threads")
          .isNull();

      // Index must be visible via schema:indexes
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM schema:indexes WHERE name = '" + INDEX_NAME + "'")) {
        assertThat(rs.hasNext()).isTrue();
      }

    } finally {
      cfg.setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, origTimeout);
      database.command("sql", "DROP TYPE " + TYPE_NAME + " IF EXISTS UNSAFE");
    }
  }
}
