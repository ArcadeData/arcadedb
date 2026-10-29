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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10857: under HA an append checks whether its samples are behind the watermark of a continuous aggregate BEFORE it commits,
 * to ship their record to the followers ahead of them (#10673). A refresh that began after that check and ended before the commit
 * moved the watermark past the samples (from newer samples already committed): they became late only once committed, and their
 * record shipped in a session that followed the commit, the shape of #10673. The refresh now keeps its watermark at or before the
 * bucket of the oldest sample of an append in flight, so the samples are never late and no session follows the commit.
 * <p>
 * The database is served through a wrapper that reports itself replicated, as the Raft wrapper does. It counts the schema sessions the
 * appending thread opens and the samples the type holds at each, and runs a refresh in another thread when the append commits, after
 * its check for late samples and before its samples are visible.
 */
class Issue10857RefreshDuringAppendCommitTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, double total) {
  }

  /** The samples the type held when each schema session of the appending thread opened, in order. */
  private final List<Long> samplesAtEachSession = new ArrayList<>();

  @BeforeEach
  void createAggregateWithAWatermarkAndANewerSample() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr").close();
    for (int h = 0; h < 3; h++)
      engine().appendSamplesWithoutRefresh(new long[] { H0 + h * HOUR }, new Object[] { "h0" }, new Object[] { 1.0 });
    refresh();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    // committed and not refreshed yet: the next refresh reads it and would move the watermark to its bucket
    engine().appendSamplesWithoutRefresh(new long[] { H0 + 3 * HOUR + 1_000 }, new Object[] { "h0" }, new Object[] { 1.0 });
  }

  /** The repro of the issue: the sample is at the watermark when the append checks it, the refresh would move it one bucket past. */
  @Test
  void aRefreshDuringTheCommitDoesNotMoveTheWatermarkPastTheSamples() throws Exception {
    final AtomicLong watermarkAfterRefresh = new AtomicLong();
    servedAsReplicated(() -> watermarkAfterRefresh.set(aggregate().getWatermarkTs()),
        () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR + 60_000 }, new Object[] { "h0" },
            new Object[] { 1.0 }));

    assertThat(watermarkAfterRefresh.get()).as("kept at the bucket of the sample in flight").isEqualTo(H0 + 2 * HOUR);
    assertThat(samplesAtEachSession).as("the sample is not late, nothing ships after its commit").isEmpty();
    assertThat(aggregate().getInvalidation()).isNull();
    assertThat(engine().oldestInFlightAppendTs()).as("the append is in flight no more").isEqualTo(Long.MAX_VALUE);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
        new Bucket(H0 + 3 * HOUR, 1));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
  }

  /** The watermark a refresh adopts from the rows of the backing type (#10634) is kept at that bucket too. */
  @Test
  void aWatermarkAdoptedDuringTheCommitIsNotPastTheSamples() throws Exception {
    refresh();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
    // the rows of the backing type beside a watermark never set: what a promoted follower holds
    aggregate().restoreWatermark(0, false);

    final AtomicLong watermarkAfterRefresh = new AtomicLong();
    servedAsReplicated(() -> watermarkAfterRefresh.set(aggregate().getWatermarkTs()),
        () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR + 60_000 }, new Object[] { "h0" },
            new Object[] { 1.0 }));

    assertThat(watermarkAfterRefresh.get()).as("adopted at the bucket of the sample in flight").isEqualTo(H0 + 2 * HOUR);
    assertThat(samplesAtEachSession).as("the sample is not late, nothing ships after its commit").isEmpty();

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
        new Bucket(H0 + 3 * HOUR, 1));
  }

  /**
   * Found by the adversarial pass of the fix: the append registers while a refresh is already in progress, after the refresh read the
   * appends in flight. It finds the refresh in progress and records its sample (#10633), and the refresh, which read the source
   * before the commit, must not clear that record: cleared, the check made after the commit finds the sample behind the new watermark
   * and ships its record after the commit.
   */
  @Test
  void aRefreshInProgressDoesNotClearTheRecordOfAnAppendRegisteredAfterIt() throws Exception {
    final AtomicLong watermarkAfterRefresh = new AtomicLong();
    refreshWithAnAppendRegisteredMidway(() -> watermarkAfterRefresh.set(aggregate().getWatermarkTs()), H0 + 2 * HOUR + 60_000);

    assertThat(watermarkAfterRefresh.get()).as("the refresh did not find the append, nothing clips it").isEqualTo(H0 + 3 * HOUR);
    assertThat(samplesAtEachSession).as("one session, before the commit, and none after it").containsExactly(4L);
    assertThat(aggregate().getInvalidation()).as("the record of the sample survives the refresh").isNotNull();
    assertThat(engine().oldestInFlightAppendTs()).isEqualTo(Long.MAX_VALUE);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
        new Bucket(H0 + 3 * HOUR, 1));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  /**
   * A sample already late when the append checks it: its record ships before the commit. A refresh that begins after the check and
   * reads the source before the commit finds the append in flight, so it keeps the record instead of clearing it: cleared, the check
   * made after the commit would record the sample again and ship that record after the commit.
   */
  @Test
  void aRefreshDuringTheCommitKeepsTheRecordOfALateSample() throws Exception {
    servedAsReplicated(() -> assertThat(aggregate().getInvalidation()).as("kept by the refresh").isNotNull(),
        () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + HOUR + 60_000 }, new Object[] { "h0" },
            new Object[] { 1.0 }));

    assertThat(samplesAtEachSession).as("one session, before the commit, and none after it").containsExactly(4L);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 2), new Bucket(H0 + 2 * HOUR, 1),
        new Bucket(H0 + 3 * HOUR, 1));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  @Test
  void aRecordIsKeptWhenAnAppendInFlightMayHaveAddedToIt() {
    final ContinuousAggregateImpl.Invalidation range = new ContinuousAggregateImpl.Invalidation(H0, H0 + HOUR - 1);
    assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, Long.MAX_VALUE)).isFalse();
    assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, H0 + HOUR)).isFalse();
    assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, H0 + HOUR - 1)).isTrue();
    assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, H0 - HOUR)).isTrue();
  }

  @Test
  void theWatermarkIsKeptAtTheBucketOfTheOldestSampleInFlight() {
    assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, Long.MAX_VALUE, HOUR)).isEqualTo(H0 + 3 * HOUR);
    assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, H0 + 3 * HOUR, HOUR)).isEqualTo(H0 + 3 * HOUR);
    assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, H0 + HOUR + 1, HOUR)).isEqualTo(H0 + HOUR);
    // on the grid of the watermark, not the epoch's
    assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR + 60_000, H0 + HOUR + 1, HOUR)).isEqualTo(
        H0 + 60_000);
    // no grid: the sample itself
    assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, H0 + HOUR + 1, 0)).isEqualTo(H0 + HOUR + 1);
  }

  private interface Body {
    void run() throws Exception;
  }

  /**
   * Runs {@code body} with the database served through a wrapper that reports itself replicated. The first commit of the thread
   * running {@code body} first runs a REFRESH in another thread, waits for it to end and runs {@code afterRefresh}.
   */
  private void servedAsReplicated(final Runnable afterRefresh, final Body body) throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal[] self = new DatabaseInternal[1];
    final Thread appending = Thread.currentThread();
    final AtomicBoolean refreshed = new AtomicBoolean();
    final AtomicReference<Throwable> refreshFailure = new AtomicReference<>();
    final InvocationHandler handler = (proxy, method, args) -> {
      final boolean ownThread = Thread.currentThread() == appending;
      switch (method.getName()) {
      case "isReplicated":
        return true;
      case "getWrappedDatabaseInstance":
        return self[0];
      case "commit":
        if (ownThread && refreshed.compareAndSet(false, true)) {
          final Thread refresher = new Thread(() -> {
            try {
              refresh();
            } catch (final Throwable t) {
              refreshFailure.set(t);
            }
          });
          refresher.start();
          refresher.join(30_000);
          assertThat(refresher.isAlive()).isFalse();
          afterRefresh.run();
        }
        return invoke(local, method, args);
      case "recordFileChanges":
        if (ownThread)
          samplesAtEachSession.add(samples());
        return invoke(local, method, args);
      default:
        return invoke(local, method, args);
      }
    };
    self[0] = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, handler);
    local.setWrappedDatabaseInstance(self[0]);
    try {
      body.run();
    } finally {
      local.setWrappedDatabaseInstance(local);
    }
    assertThat(refreshed.get()).as("the refresh ran during the commit").isTrue();
    assertThat(refreshFailure.get()).isNull();
  }

  /**
   * Runs a REFRESH in this thread, served through a wrapper that reports itself replicated. Right after the refresh read the appends
   * in flight, another thread appends a sample at {@code ts}: it checks for late samples, then waits in its commit until the refresh
   * has ended and {@code afterRefresh} has run.
   */
  private void refreshWithAnAppendRegisteredMidway(final Runnable afterRefresh, final long ts) throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal[] self = new DatabaseInternal[1];
    final AtomicReference<Thread> appending = new AtomicReference<>();
    final CountDownLatch inCommit = new CountDownLatch(1);
    final CountDownLatch refreshEnded = new CountDownLatch(1);
    final AtomicReference<Throwable> appendFailure = new AtomicReference<>();
    final InvocationHandler handler = (proxy, method, args) -> {
      final boolean appendingThread = Thread.currentThread() == appending.get();
      switch (method.getName()) {
      case "isReplicated":
        return true;
      case "getWrappedDatabaseInstance":
        return self[0];
      case "commit":
        if (appendingThread) {
          inCommit.countDown();
          assertThat(refreshEnded.await(30, TimeUnit.SECONDS)).isTrue();
        }
        return invoke(local, method, args);
      case "recordFileChanges":
        if (appendingThread)
          samplesAtEachSession.add(samples());
        return invoke(local, method, args);
      default:
        return invoke(local, method, args);
      }
    };
    self[0] = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, handler);
    final Thread appender = new Thread(() -> {
      try {
        engine().appendSamplesWithoutRefresh(new long[] { ts }, new Object[] { "h0" }, new Object[] { 1.0 });
      } catch (final Throwable t) {
        appendFailure.set(t);
        inCommit.countDown();
      }
    });
    appending.set(appender);
    final AtomicBoolean started = new AtomicBoolean();
    ContinuousAggregateRefresher.TEST_AFTER_IN_FLIGHT_READ_HOOK = () -> {
      if (!started.compareAndSet(false, true))
        return;
      appender.start();
      try {
        assertThat(inCommit.await(30, TimeUnit.SECONDS)).isTrue();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    };
    local.setWrappedDatabaseInstance(self[0]);
    try {
      refresh();
      afterRefresh.run();
    } finally {
      ContinuousAggregateRefresher.TEST_AFTER_IN_FLIGHT_READ_HOOK = null;
      refreshEnded.countDown();
      appender.join(30_000);
      local.setWrappedDatabaseInstance(local);
    }
    assertThat(started.get()).as("the append ran during the refresh").isTrue();
    assertThat(appender.isAlive()).isFalse();
    assertThat(appendFailure.get()).isNull();
  }

  private static Object invoke(final Object target, final Method method, final Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
  }

  private long samples() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM sensor")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private List<Bucket> buckets() {
    final List<Bucket> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM hourly ORDER BY hr")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), ((Number) row.getProperty("total")).doubleValue()));
      }
    }
    return result;
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("hourly");
  }
}
