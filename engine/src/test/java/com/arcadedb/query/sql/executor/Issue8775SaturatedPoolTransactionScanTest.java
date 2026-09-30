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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.ParallelScanProducerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8775: inside a transaction the caller that takes a scan unit no worker has started must read it from the same
 * committed pages the workers read, in one piece, and must not wait for a worker when the producer pool is held by
 * result sets left open - also after the transaction has written.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8775SaturatedPoolTransactionScanTest extends TestHelper {
  private static final int RECORDS = 100_000;

  private Set<Thread> readersBefore = Set.of();

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 8);
    database.getSchema().createDocumentType("Rating");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Rating").set("id", i, "userId", (long) (i % 610)).save();
    });
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void idleTransactionScanProgressesWhileTheProducerPoolIsHeld() throws Exception {
    assertThat(scanWhilePoolIsHeld(false)).isEqualTo(RECORDS);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void scanProgressesAfterTheTransactionWroteWhileThePoolIsHeld() throws Exception {
    // the rows come from the committed pages: the write made after the first pull is not part of the scan
    assertThat(scanWhilePoolIsHeld(true)).isEqualTo(RECORDS);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void abandonedTransactionScanReleasesItsReaderThread() throws Exception {
    // a scan abandoned in a transaction (never drained, never closed) must not keep its reader thread for ever
    database.getConfiguration().setValue(GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT, 1_000L);
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();
    final List<ResultSet> abandoned = new ArrayList<>();
    readersBefore = readerThreads();
    try {
      for (int i = 0; i < maxThreads; i++) {
        final ResultSet rs = database.query("sql", "SELECT FROM Rating");
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        abandoned.add(rs);
      }
      database.begin();
      try {
        final ResultSet rs = database.query("sql", "SELECT FROM Rating");
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        // NEVER CLOSED, ON PURPOSE
        final long deadline = System.currentTimeMillis() + 60_000;
        boolean seen = false;
        while ((!seen || liveReaders() > 0) && System.currentTimeMillis() < deadline) {
          seen |= liveReaders() > 0;
          Thread.sleep(20);
        }
        assertThat(seen).as("the scan must have used a reader thread, or this test proves nothing").isTrue();
        assertThat(liveReaders()).as("the reader of an abandoned scan must end within the abandonment timeout").isZero();
      } finally {
        database.rollback();
      }
    } finally {
      for (final ResultSet rs : abandoned)
        rs.close();
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void scanPausedPastTheAbandonmentTimeoutFailsInsteadOfHanging() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT, 500L);
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();
    final List<ResultSet> abandoned = new ArrayList<>();
    try {
      for (int i = 0; i < maxThreads; i++) {
        final ResultSet rs = database.query("sql", "SELECT FROM Rating");
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        abandoned.add(rs);
      }
      database.begin();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Rating")) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        // a pause longer than the timeout: the reader gives up, and the scan must fail rather than wait for it
        Thread.sleep(3_000);
        boolean failed = false;
        try {
          while (rs.hasNext())
            rs.next();
        } catch (final RuntimeException e) {
          failed = true;
        }
        assertThat(failed).as("a scan whose reader gave up fails, it does not hang").isTrue();
      } finally {
        database.rollback();
      }
    } finally {
      for (final ResultSet rs : abandoned)
        rs.close();
    }
  }

  private static Set<Thread> readerThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith("ArcadeDB-parallel-scan-unit-reader") && t.isAlive()).collect(Collectors.toSet());
  }

  private long liveReaders() {
    final Set<Thread> now = readerThreads();
    now.removeAll(readersBefore);
    return now.size();
  }

  private long scanWhilePoolIsHeld(final boolean writeAfterFirstRow) throws Exception {
    readersBefore = readerThreads();
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();
    final List<ResultSet> abandoned = new ArrayList<>();
    try {
      for (int i = 0; i < maxThreads; i++) {
        final ResultSet rs = database.query("sql", "SELECT FROM Rating");
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        abandoned.add(rs);
      }
      final long deadline = System.currentTimeMillis() + 20_000;
      while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() < maxThreads && System.currentTimeMillis() < deadline)
        Thread.sleep(10);
      assertThat(ParallelScanProducerPool.getInstance().getPoolStats().activeThreads())
          .as("the abandoned scans must hold every producer thread, or this test proves nothing").isEqualTo(maxThreads);

      database.begin();
      try {
        long rows = 0;
        long maxReaders = 0;
        try (final ResultSet rs = database.query("sql", "SELECT FROM Rating")) {
          while (rs.hasNext()) {
            rs.next();
            if (rows++ == 0 && writeAfterFirstRow)
              database.newDocument("Rating").set("id", -1).save();
            if (rows % 1_000 == 0)
              maxReaders = Math.max(maxReaders, liveReaders());
          }
        }
        // one reader serves every unit the caller claims: the threads do not pile up with the units
        assertThat(maxReaders).isLessThanOrEqualTo(1);
        return rows;
      } finally {
        database.rollback();
      }
    } finally {
      for (final ResultSet rs : abandoned)
        rs.close();
    }
  }
}
