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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5503: concurrent transactions issued through a shared follower
 * {@link Database} handle corrupted records and silently lost committed writes.
 * <p>
 * A replica captured its transaction's pages in {@code commit1stPhase} without taking the per-file
 * commit locks the leader takes, so the MVCC page-version check was a non-atomic check-then-act. Two
 * threads writing into the same bucket page both validated against the same base version and both
 * shipped a partial page delta for it. Applying both deltas left the page physically inconsistent -
 * the engine reported {@code Invalid record size} and deleted the affected records - and the writes
 * one delta carried were lost even though its transaction had reported success.
 * <p>
 * The contract asserted here is that every transaction either commits or fails retryably, and that a
 * caller that honours the retry contract ends up with every record present and identical on all nodes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5503ConcurrentFollowerWritesIT extends BaseRaftHATest {

  private static final String TYPE_NAME       = "Concurrent";
  private static final int    WRITER_THREADS  = 3;
  private static final int    RECORDS_PER_THREAD = 150;
  private static final int    TOTAL_RECORDS   = WRITER_THREADS * RECORDS_PER_THREAD;
  private static final int    MAX_RETRIES     = 100;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void concurrentWritesOnAFollowerHandleKeepEveryRecord() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected before the test starts").isGreaterThanOrEqualTo(0);

    getServer(leaderIndex).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    waitForAllServers();

    // ALL WRITES GO THROUGH A FOLLOWER, FROM SEVERAL THREADS SHARING THE SAME EMBEDDED HANDLE
    final Database follower = getServer((leaderIndex + 1) % getServerCount()).getDatabase(getDatabaseName());

    final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    final AtomicInteger retries = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final Thread[] writers = new Thread[WRITER_THREADS];

    for (int t = 0; t < WRITER_THREADS; t++) {
      final int threadId = t;
      writers[t] = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < RECORDS_PER_THREAD; i++) {
            final int value = threadId * RECORDS_PER_THREAD + i;
            commitWithRetry(follower, value, retries);
          }
        } catch (final Throwable e) {
          failures.add(e);
        }
      }, "issue5503-writer-" + t);
      writers[t].start();
    }

    start.countDown();
    for (final Thread writer : writers)
      writer.join(TimeUnit.MINUTES.toMillis(5));

    assertThat(failures).as("no writer may fail with a non-retryable error").isEmpty();

    waitForAllServers();

    // EVERY NODE MUST HOLD EVERY RECORD EXACTLY ONCE: A LOST OR CORRUPTED RECORD SHOWS UP AS A GAP
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServer(i).getDatabase(getDatabaseName());
      final String role = i == leaderIndex ? "leader" : "follower";

      assertThat(scanCount(db)).as("record count on the %s (server %d), %d retries observed", role, i, retries.get())
          .isEqualTo(TOTAL_RECORDS);
      assertThat(distinctValues(db)).as("distinct values on the %s (server %d)", role, i).hasSize(TOTAL_RECORDS);
    }
  }

  /**
   * Commits a single-record transaction, retrying the conflicts the engine is allowed to raise. A
   * concurrent writer losing the race for a page must surface a retryable {@link NeedRetryException}
   * rather than silently dropping or corrupting the record.
   */
  private void commitWithRetry(final Database db, final int value, final AtomicInteger retries) {
    for (int attempt = 0; ; attempt++) {
      try {
        db.transaction(() -> db.newDocument(TYPE_NAME).set("value", value).save());
        return;
      } catch (final NeedRetryException e) {
        retries.incrementAndGet();
        if (attempt >= MAX_RETRIES)
          throw e;
      }
    }
  }

  private long scanCount(final Database db) {
    // count(@rid) forces a scan: count(*) reads the cached per-bucket counter, which would hide a
    // record the engine deleted as malformed.
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM " + TYPE_NAME)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private Set<Integer> distinctValues(final Database db) {
    final Set<Integer> values = new HashSet<>();
    try (final ResultSet rs = db.query("sql", "SELECT value FROM " + TYPE_NAME)) {
      while (rs.hasNext())
        values.add(rs.next().<Number>getProperty("value").intValue());
    }
    return values;
  }
}
