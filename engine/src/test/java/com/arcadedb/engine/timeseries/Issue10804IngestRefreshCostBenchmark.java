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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;

/**
 * Issue #10804: the cost of the continuous aggregate refresh an ingest request triggers, for a sequential client appending one
 * sample per autocommit request (the shape that gets no coalescing). Reports the average time of a request per quarter of the run,
 * into a type without an aggregate and into one with an aggregate, and how many refreshes ran:
 * <ul>
 *   <li>{@link #oneOpenBucket()}: every request lands in one long (1h) bucket, so a refresh that recomputes the open bucket costs
 *   more with every request;</li>
 *   <li>{@link #oneOpenBucketAfterAHistory()}: the same, after a long history of closed buckets, which a refresh has no reason to
 *   read at all.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue10804IngestRefreshCostBenchmark extends TestHelper {
  private static final long HOUR          = 3_600_000L;
  private static final long H0            = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);
  private static final int  REQUESTS      = 20_000;
  private static final int  HISTORY_HOURS = 20_000;
  private static final int  HISTORY_TAGS  = 5;

  @Test
  void oneOpenBucket() {
    createTypes();
    report("one 1h bucket", REQUESTS);
  }

  @Test
  void oneOpenBucketAfterAHistory() throws Exception {
    createTypes();
    // HISTORY_HOURS closed buckets of HISTORY_TAGS tags each, ahead of H0, aggregated by one refresh
    for (final String type : new String[] { "Plain", "Aggregated" }) {
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
      final int n = HISTORY_HOURS * HISTORY_TAGS;
      final long[] timestamps = new long[n];
      final Object[] tags = new Object[n];
      final Object[] values = new Object[n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = H0 - (long) (HISTORY_HOURS - i / HISTORY_TAGS) * HOUR - 4 * HOUR;
        tags[i] = "t" + (i % HISTORY_TAGS);
        values[i] = 1.0;
      }
      database.transaction(() -> {
        try {
          engine.appendSamples(timestamps, tags, values);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    aggregate().refresh();
    report("one 1h bucket after " + (HISTORY_HOURS * HISTORY_TAGS) + " aggregated rows of history", REQUESTS / 4);
  }

  private void createTypes() {
    database.command("sql", "CREATE TIMESERIES TYPE Plain TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE Aggregated TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE AggregatedHourly AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total, count(*) AS n "
            + "FROM Aggregated GROUP BY hr, h");
  }

  private ContinuousAggregate aggregate() {
    return database.getSchema().getContinuousAggregate("AggregatedHourly");
  }

  private void report(final String scenario, final int requests) {
    // warm-up of the parser, the plan cache and the JIT, on another hour of each type
    run("Plain", H0 - 2 * HOUR, 500);
    run("Aggregated", H0 - 2 * HOUR, 500);

    final long[] plain = run("Plain", H0, requests);
    final long refreshesBefore = aggregate().getRefreshCount();
    final long[] aggregated = run("Aggregated", H0, requests);
    // the last write of the burst has to be aggregated: wait for a refresh still owed, if any
    final long waitStart = System.nanoTime();
    while (sumOfBucket() != requests && System.nanoTime() - waitStart < 30_000_000_000L)
      Thread.onSpinWait();
    final long refreshes = aggregate().getRefreshCount() - refreshesBefore;

    final StringBuilder report = new StringBuilder(
        "Issue #10804 - " + requests + " sequential autocommit INSERTs into " + scenario + "\n");
    for (int q = 0; q < 4; q++)
      report.append(String.format("  quarter %d: plain %,8d us/request | with aggregate %,8d us/request%n", q + 1, plain[q],
          aggregated[q]));
    report.append(String.format("  refreshes run: %,d for %,d requests, bucket total %,d", refreshes, requests, sumOfBucket()));
    // the report is the output of the benchmark, printed as TimeSeriesEmbeddedBenchmark prints its own: the test logging
    // configuration keeps LogManager to SEVERE, which would hide it
    System.out.println(report);
  }

  /** Average microseconds per request in each quarter of the run. */
  private long[] run(final String type, final long start, final int requests) {
    final long[] quarters = new long[4];
    final int perQuarter = requests / 4;
    for (int q = 0; q < 4; q++) {
      final long begin = System.nanoTime();
      for (int i = 0; i < perQuarter; i++) {
        // spread evenly over the hour that opens at start
        final long ts = start + (long) (q * perQuarter + i) * (HOUR / requests);
        database.command("sql", "INSERT INTO " + type + " SET ts = ?, h = 'a', v = 1.0", ts);
      }
      quarters[q] = (System.nanoTime() - begin) / 1_000 / perQuarter;
    }
    return quarters;
  }

  private long sumOfBucket() {
    try (final ResultSet rs = database.query("sql", "SELECT sum(n) AS n FROM AggregatedHourly WHERE hr = ?", new Date(H0))) {
      final Object n = rs.next().getProperty("n");
      return n == null ? 0L : ((Number) n).longValue();
    }
  }
}
