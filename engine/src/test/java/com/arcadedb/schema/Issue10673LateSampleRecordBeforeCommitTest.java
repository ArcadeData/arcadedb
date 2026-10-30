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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10673: under HA the record of the samples an append adds behind the watermark of a continuous aggregate ships to the
 * followers in a schema session BEFORE the samples commit, so a leader lost right after the commit has shipped it already. The check
 * made again after the commit ships a second session only when the record changed since, or the first session failed.
 * <p>
 * The database here is served through a wrapper that reports itself replicated, as the Raft wrapper does, and that counts the schema
 * sessions the append opens and the samples the type holds at each: a session that sees 3 samples ran before the commit of the 4th.
 */
class Issue10673LateSampleRecordBeforeCommitTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  /** The samples the type held when each schema session opened, in order. */
  private final List<Long> samplesAtEachSession = new ArrayList<>();

  @BeforeEach
  void createAggregateWithAWatermark() {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr").close();
    for (int h = 0; h < 3; h++)
      database.command("sql", "INSERT INTO sensor SET ts = ?, host = 'h0', v = 1", H0 + h * HOUR).close();
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    assertThat(aggregate().getInvalidation()).isNull();
  }

  @Test
  void aLateSampleShipsOneSessionBeforeItsCommitAndNoneAfter() throws Exception {
    servedAsReplicated(session -> { }, () -> {
      appendAt(H0 + 60_000L);
      assertThat(samplesAtEachSession).as("one session, before the commit of the 4th sample").containsExactly(3L);
      assertThat(aggregate().getInvalidation()).isNotNull();
      assertThat(aggregate().getInvalidation().fromTs()).isEqualTo(H0);

      // a stream of late samples into a bucket already recorded ships nothing more, before or after the commit
      appendAt(H0 + 120_000L);
      // and nor does a sample at or after the watermark
      appendAt(H0 + 2 * HOUR + 1);
      assertThat(samplesAtEachSession).containsExactly(3L);
    });
  }

  @Test
  void aSessionThatFailsBeforeTheCommitIsShippedAfterIt() throws Exception {
    servedAsReplicated(session -> {
      if (session == 0)
        throw new IllegalStateException("simulated failure of the replication session");
    }, () -> {
      // the append is not reported as failed
      appendAt(H0 + 60_000L);
      assertThat(samplesAtEachSession).as("the failed session before the commit, then the one after it").containsExactly(3L, 4L);
      assertThat(aggregate().getInvalidation()).isNotNull();
    });
  }

  @Test
  void aRecordClearedBeforeTheCommitIsRecordedAndShippedAgainAfterIt() throws Exception {
    servedAsReplicated(session -> {
      if (session == 0) {
        // what a refresh that read the source before the commit does once it has recomputed the buckets: it clears the record
        final ContinuousAggregateImpl.Invalidation read = aggregate().getInvalidation();
        assertThat(read).isNotNull();
        aggregate().clearInvalidation(read);
      }
    }, () -> {
      appendAt(H0 + 60_000L);
      assertThat(samplesAtEachSession).as("shipped before the commit, then again after it").containsExactly(3L, 4L);
      assertThat(aggregate().getInvalidation()).as("recorded again once the sample committed").isNotNull();
      assertThat(aggregate().getInvalidation().fromTs()).isEqualTo(H0);
    });
  }

  private interface Body {
    void run() throws Exception;
  }

  /**
   * Runs {@code body} with the database served through a wrapper that reports itself replicated. Every schema session opens through
   * the real database, after {@code afterSession} is told its ordinal; a failure it throws is the session's.
   */
  private void servedAsReplicated(final IntConsumer afterSession, final Body body) throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal[] self = new DatabaseInternal[1];
    final InvocationHandler handler = (proxy, method, args) -> {
      switch (method.getName()) {
      case "isReplicated":
        return true;
      case "getWrappedDatabaseInstance":
        return self[0];
      case "recordFileChanges":
        final int session = samplesAtEachSession.size();
        samplesAtEachSession.add(samples());
        final Object result = invoke(local, method, args);
        afterSession.accept(session);
        return result;
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
  }

  private static Object invoke(final Object target, final Method method, final Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private void appendAt(final long ts) throws Exception {
    // Without the refresh every ingest path runs once the samples commit (#10686): it recomputes the buckets and clears the record
    // this test asserts on, which is not what ships the record to the followers
    engine().appendSamplesWithoutRefresh(new long[] { ts }, new Object[] { "h0" }, new Object[] { 1.0 });
  }

  private long samples() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM sensor")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("hourly");
  }
}
