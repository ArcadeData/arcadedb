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
import com.arcadedb.database.Identifiable;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.ParallelScanProducerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10747: closing a parallel scan cancels its producers with an interrupt, and a producer interrupted while it waits
 * for the I/O lock of a page another thread is loading does not return, it throws ({@code InterruptedIOException} wrapped
 * by the read lock as "Error during read lock"). That throw was recorded as a failure of the scan and logged at WARNING
 * with its stack trace, for a cancellation the consumer itself asked for.
 * <p>
 * An after-read listener, which the bucket iterator runs under the same read lock as the page load, stands in for it: on a
 * producer thread it waits until it is interrupted and then throws exactly what the page load throws, so the interrupt
 * deterministically lands inside a read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10747CancelledParallelScanNotLoggedTest extends TestHelper {
  private static final String TYPE_NAME = "D";
  private static final int    RECORDS   = 50_000;

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aProducerInterruptedInsideAReadIsNotReportedAsAFailure() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType(TYPE_NAME);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });

    // THE POSITION OF THE RECORD HALF WAY THROUGH THE BUCKET: EVERY RECORD PAST IT IS IN A LATER UNIT THAN THE FIRST
    final long halfWay;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE id = ?", RECORDS / 2)) {
      halfWay = rs.next().<Identifiable>getProperty("rid").getIdentity().getPosition();
    }

    final CountDownLatch stalled = new CountDownLatch(1);
    final CountDownLatch thrown = new CountDownLatch(1);
    final AfterRecordReadListener stall = record -> {
      // THE ROWS OF THE FIRST UNITS PASS, SO THE CONSUMER GETS A ROW; A PRODUCER ON A LATER UNIT STALLS UNTIL CANCELLED
      if (record.getIdentity().getPosition() < halfWay || !(Thread.currentThread() instanceof ParallelScanProducerPool.ProducerThread))
        return record;
      stalled.countDown();
      try {
        Thread.sleep(60_000);
        return record;
      } catch (final InterruptedException e) {
        // WHAT PageManager.concurrentPageAccess THROWS, AS LocalDatabase.executeInReadLock WRAPS IT
        thrown.countDown();
        throw new DatabaseOperationException("Error during read lock",
            new InterruptedIOException("Interrupted while acquiring I/O lock for page"));
      }
    };

    final List<String> warnings = new CopyOnWriteArrayList<>();
    LogManager.instance().setLogger(new CapturingLogger(warnings));
    try {
      final String query = "SELECT FROM " + TYPE_NAME;
      try (final ResultSet plan = database.query("sql", "EXPLAIN " + query)) {
        assertThat(plan.next().<String>getProperty("executionPlanAsString")).as("the scan must run in parallel, or this test proves nothing")
            .contains("(parallel)");
      }

      database.getEvents().registerListener(stall);
      final ResultSet rs = database.query("sql", query);
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(stalled.await(30, TimeUnit.SECONDS)).as("a producer must be inside the stalled read before the close").isTrue();
      rs.close();
      assertThat(thrown.await(30, TimeUnit.SECONDS)).as("the close must interrupt the stalled producer").isTrue();

      // THE PRODUCER'S CATCH RUNS RIGHT AFTER THE THROW: WAIT FOR THE POOL TO GIVE ITS THREAD BACK
      final long deadline = System.currentTimeMillis() + 20_000;
      while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() > 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(10);

      assertThat(warnings).as("a cancellation the consumer asked for is not a failure of the scan").isEmpty();
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
      database.getEvents().unregisterListener(stall);
    }

    // THE SAME QUERY WITHOUT A STALL STILL ANSWERS EVERY ROW: THE CANCELLED SCAN LEFT NOTHING BEHIND
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE id >= 0")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(RECORDS);
    }
  }

  /** Records the WARNING and SEVERE lines of the parallel scan, the only ones this test is about. */
  private static final class CapturingLogger implements Logger {
    private final List<String> captured;

    private CapturingLogger(final List<String> captured) {
      this.captured = captured;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      record(requester, level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      record(requester, level, message);
    }

    private void record(final Object requester, final Level level, final String message) {
      if (requester instanceof ParallelTypeScan && level.intValue() >= Level.WARNING.intValue())
        captured.add(level + " " + message);
    }

    @Override
    public void flush() {
    }
  }
}
