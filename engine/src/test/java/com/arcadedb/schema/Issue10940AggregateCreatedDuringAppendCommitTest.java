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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10940, the residue of #10857: an HA append registered itself in flight only when its pre-commit check found a continuous
 * aggregate over the type. A {@code CREATE CONTINUOUS AGGREGATE} run after that check and before the commit refreshed without the
 * uncommitted samples and installed a watermark past them (from newer samples already committed); the check made after the commit
 * then found them late and shipped their record in a session that followed the commit, the shape of #10673. Every HA append is now
 * registered before it reads the aggregates, and the first refresh of the new aggregate keeps its watermark at the bucket of the
 * oldest sample in flight, which it scans itself since the append, finding no aggregate, did not.
 * <p>
 * The database is served through a wrapper that reports itself replicated, as the Raft wrapper does. It counts the schema sessions the
 * appending thread opens, and creates the aggregate in another thread when the append commits, after its check for late samples and
 * before its samples are visible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10940AggregateCreatedDuringAppendCommitTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, double total) {
  }

  /** The samples the type held when each schema session of the appending thread opened, in order. */
  private final List<Long> samplesAtEachSession = new ArrayList<>();

  @BeforeEach
  void createTypeWithoutAggregateAndANewerSample() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
    for (int h = 0; h < 3; h++)
      engine().appendSamplesWithoutRefresh(new long[] { H0 + h * HOUR }, new Object[] { "h0" }, new Object[] { 1.0 });
    // committed: the creation refresh reads it and would move the watermark to its bucket
    engine().appendSamplesWithoutRefresh(new long[] { H0 + 3 * HOUR + 1_000 }, new Object[] { "h0" }, new Object[] { 1.0 });
  }

  @Test
  void anAggregateCreatedDuringTheCommitDoesNotPutItsWatermarkPastTheSamples() throws Exception {
    final AtomicLong inFlightWhileCommitting = new AtomicLong();
    final AtomicLong watermarkAfterCreation = new AtomicLong();
    servedAsReplicated(() -> {
      inFlightWhileCommitting.set(engine().oldestInFlightAppendTs());
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr")
          .close();
      watermarkAfterCreation.set(aggregate().getWatermarkTs());
    }, () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR + 60_000 }, new Object[] { "h0" },
        new Object[] { 1.0 }));

    assertThat(watermarkAfterCreation.get()).as("kept at the bucket of the sample in flight").isEqualTo(H0 + 2 * HOUR);
    assertThat(samplesAtEachSession).as("the sample is not late, nothing ships after its commit").isEmpty();
    assertThat(inFlightWhileCommitting.get()).as("in flight with no aggregate over the type, scanned by the reader")
        .isEqualTo(H0 + 2 * HOUR + 60_000);
    assertThat(aggregate().getInvalidation()).isNull();
    assertThat(engine().oldestInFlightAppendTs()).as("the append is in flight no more").isEqualTo(Long.MAX_VALUE);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
        new Bucket(H0 + 3 * HOUR, 1));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
  }

  private interface Body {
    void run() throws Exception;
  }

  /**
   * Runs {@code body} with the database served through a wrapper that reports itself replicated. The first commit of the thread
   * running {@code body} first runs {@code inCommit} in another thread and waits for it to end.
   */
  private void servedAsReplicated(final Runnable inCommit, final Body body) throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal[] self = new DatabaseInternal[1];
    final Thread appending = Thread.currentThread();
    final AtomicBoolean ran = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final InvocationHandler handler = (proxy, method, args) -> {
      final boolean ownThread = Thread.currentThread() == appending;
      switch (method.getName()) {
      case "isReplicated":
        return true;
      case "getWrappedDatabaseInstance":
        return self[0];
      case "commit":
        if (ownThread && ran.compareAndSet(false, true)) {
          final Thread other = new Thread(() -> {
            try {
              inCommit.run();
            } catch (final Throwable t) {
              failure.set(t);
            }
          });
          other.start();
          other.join(30_000);
          assertThat(other.isAlive()).isFalse();
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
    assertThat(ran.get()).as("the callback ran during the commit").isTrue();
    assertThat(failure.get()).isNull();
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
