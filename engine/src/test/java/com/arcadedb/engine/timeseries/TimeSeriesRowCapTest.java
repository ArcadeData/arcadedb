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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.ResultRowCapScope;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12016. A time series scan with no bound of its own reads the whole series (a descending one lists every
 * row of it in memory), and the HTTP endpoints used to bound it by the LIMIT they appended to the statement text. The row cap of the
 * caller reaches the scan through {@link ResultRowCapScope} instead, under the conditions the statement's own LIMIT would have: a
 * residual predicate, an aggregate or a DISTINCT keeps the scan as it was.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TimeSeriesRowCapTest extends TestHelper {
  private static final int  PER_TAG = 2_000;
  private static final long BASE_TS = 1_700_000_000_000L;
  private static final long STEP_MS = 1_000L;
  private static final int  CAP     = 10;

  /** The rows of the statement run under a cap, and the plan that ran. */
  private record Run(List<Result> rows, String plan) {
  }

  @BeforeEach
  void populate() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

    final int tags = 4;
    final int total = tags * PER_TAG;
    final long[] timestamps = new long[total];
    final Object[] hosts = new Object[total];
    final Object[] values = new Object[total];
    int i = 0;
    for (int t = 0; t < PER_TAG; t++)
      for (int h = 0; h < tags; h++) {
        timestamps[i] = BASE_TS + t * STEP_MS;
        hosts[i] = "host_" + h;
        values[i] = (double) (t * tags + h);
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
    engine.compactAll();
  }

  private Run run(final String sql, final int cap) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(cap); final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
      return new Run(rows, rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse(""));
    }
  }

  private static long epochMillis(final Result row) {
    final Object ts = row.getProperty("ts");
    if (ts instanceof LocalDateTime ldt)
      return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    return ((Number) ts).longValue();
  }

  @Test
  void aDescendingScanStopsAtTheCap() {
    final Run capped = run("SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts DESC", CAP);
    assertThat(capped.plan()).contains("FETCH FROM TIMESERIES DESC Point").contains("TOP " + CAP);
    assertThat(capped.rows()).hasSize(CAP);
    for (int i = 0; i < CAP; i++)
      assertThat(epochMillis(capped.rows().get(i))).isEqualTo(BASE_TS + (PER_TAG - 1 - i) * STEP_MS);

    final Run uncapped = run("SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts DESC", 0);
    assertThat(uncapped.plan()).doesNotContain("TOP ");
    assertThat(uncapped.rows()).hasSize(PER_TAG);
  }

  @Test
  void anAscendingScanStopsAtTheCapAndSkipIsCountedBeforeIt() {
    final Run capped = run("SELECT ts, value FROM Point WHERE host = 'host_2' ORDER BY ts ASC SKIP 7", CAP);
    assertThat(capped.plan()).contains("TOP " + (7 + CAP));
    assertThat(capped.rows()).hasSize(CAP);
    for (int i = 0; i < CAP; i++)
      assertThat(epochMillis(capped.rows().get(i))).isEqualTo(BASE_TS + (7 + i) * STEP_MS);

    final Run bare = run("SELECT ts, value FROM Point WHERE host = 'host_2'", CAP);
    assertThat(bare.plan()).contains("TOP " + CAP);
    assertThat(bare.rows()).hasSize(CAP);
  }

  @Test
  void aStatementWithItsOwnLimitIgnoresTheCap() {
    final Run run = run("SELECT ts FROM Point WHERE host = 'host_1' ORDER BY ts DESC LIMIT 25", CAP);
    assertThat(run.plan()).contains("TOP 25").doesNotContain("TOP " + CAP);
    assertThat(run.rows()).hasSize(25);
  }

  @Test
  void aScanThatCanLoseARowAfterTheFetchIsNotBounded() {
    // A RESIDUAL PREDICATE THE ENGINE NEVER SAW: THE ROWS THE FETCH RETURNS CAN STILL BE DROPPED, SO A BOUND WOULD ANSWER SHORT. THE
    // SORT AFTER THE FILTER STILL KEEPS THE ROWS THE CALLER READS, AND THEY ARE THE FIRST ONES THE FILTER KEPT
    final Run residual = run("SELECT ts, value FROM Point WHERE host = 'host_1' AND value < 1000 ORDER BY ts ASC", CAP);
    assertThat(residual.plan()).doesNotContain("TOP ").contains("buffer size: " + CAP);
    assertThat(residual.rows()).hasSize(CAP);
    for (int i = 0; i < CAP; i++) {
      assertThat(((Number) residual.rows().get(i).getProperty("value")).doubleValue()).isLessThan(1000.0);
      assertThat(epochMillis(residual.rows().get(i))).isEqualTo(BASE_TS + i * STEP_MS);
    }

    final Run distinct = run("SELECT DISTINCT host FROM Point", CAP);
    assertThat(distinct.plan()).doesNotContain("TOP ");

    // THE UNION OF TWO TAGS IS A SUPERSET THE FILTER NARROWS: THE SCAN READS BOTH, AND THE SORT KEEPS THE FIRST ROWS OF THEM
    final Run union = run("SELECT ts FROM Point WHERE host = 'host_1' OR host = 'host_2' ORDER BY ts ASC", CAP);
    assertThat(union.plan()).doesNotContain("TOP ");
    assertThat(union.rows()).hasSize(CAP);
  }
}
