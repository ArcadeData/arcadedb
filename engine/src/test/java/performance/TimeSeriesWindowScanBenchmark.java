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
package performance;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The query shapes of issue #9612 over a compacted TIMESERIES type: a count over a time window, the same count with a
 * field predicate, the rows the field predicate keeps, and a plain {@code double[]} loop over the same values as the
 * floor. Prints the median and the nanoseconds per candidate sample of each.
 * <p>
 * Run explicitly with {@code ./mvnw -pl engine -Dtest=TimeSeriesWindowScanBenchmark -Dgroups=benchmark test}. Override
 * the series count with {@code -Darcadedb.windowScanBenchmark.hosts=1000} for the size quoted on the issue (4,320,000
 * samples).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class TimeSeriesWindowScanBenchmark extends TestHelper {
  private static final long T0       = 1_767_225_600L;
  private static final int  PER_HOST = 4_320;
  private static final int  RUNS     = 9;

  @Test
  void windowQueries() throws Exception {
    final int hosts = Integer.getInteger("arcadedb.windowScanBenchmark.hosts", 200);
    database.command("sql",
        "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (uu DOUBLE, us DOUBLE, ui DOUBLE) COMPACTION_INTERVAL 1 HOURS");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

    final Random random = new Random(11);
    final int samples = hosts * PER_HOST;
    final double[] uuAll = new double[samples];
    final int chunk = 100_000;
    long[] ts = new long[chunk];
    Object[] host = new Object[chunk], uu = new Object[chunk], us = new Object[chunk], ui = new Object[chunk];
    int fill = 0;
    int n = 0;
    for (int h = 0; h < hosts; h++)
      for (int i = 0; i < PER_HOST; i++) {
        ts[fill] = (T0 + i * 10L) * 1000;
        host[fill] = "host_" + h;
        final double v = random.nextInt(101);
        uu[fill] = v;
        us[fill] = (double) random.nextInt(101);
        ui[fill] = (double) random.nextInt(101);
        uuAll[n++] = v;
        if (++fill == chunk || n == samples) {
          final int size = fill;
          final long[] t = Arrays.copyOf(ts, size);
          final Object[] a = Arrays.copyOf(host, size), b = Arrays.copyOf(uu, size), c = Arrays.copyOf(us, size), d = Arrays.copyOf(ui, size);
          database.transaction(() -> {
            try {
              engine.appendSamples(t, a, b, c, d);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          });
          fill = 0;
        }
      }
    engine.compactAll();

    final String window = "ts >= " + (T0 * 1000) + " AND ts < " + ((T0 + 43_200) * 1000);
    long expected = 0;
    for (final double v : uuAll)
      if (v > 90)
        expected++;

    final String[] names = { "count, window + uu > 90", "rows (host, ts, uu), window + uu > 90", "count, window only",
        "count, avg, max over window" };
    final String[] sql = { "SELECT count(*) AS n FROM Point WHERE " + window + " AND uu > 90",
        "SELECT host, ts, uu FROM Point WHERE " + window + " AND uu > 90", "SELECT count(*) AS n FROM Point WHERE " + window,
        "SELECT count(*) AS n, avg(uu) AS a, max(us) AS m FROM Point WHERE " + window };
    final long[] expectedRows = { expected, expected, samples, samples };
    for (int q = 0; q < sql.length; q++) {
      final double[] times = new double[RUNS];
      long rows = 0;
      for (int i = -2; i < RUNS; i++) {
        final long start = System.nanoTime();
        rows = 0;
        try (final ResultSet rs = database.query("sql", sql[q])) {
          while (rs.hasNext()) {
            final var row = rs.next();
            rows += q == 1 ? 1 : ((Number) row.getProperty("n")).longValue();
          }
        }
        if (i >= 0)
          times[i] = (System.nanoTime() - start) / 1e6;
      }
      Arrays.sort(times);
      assertThat(rows).as(names[q]).isEqualTo(expectedRows[q]);
      report(names[q], times[RUNS / 2], samples);
    }

    final double[] times = new double[RUNS];
    for (int i = -2; i < RUNS; i++) {
      final long start = System.nanoTime();
      long count = 0;
      for (final double v : uuAll)
        if (v > 90)
          count++;
      if (i >= 0)
        times[i] = (System.nanoTime() - start) / 1e6;
      assertThat(count).isEqualTo(expected);
    }
    Arrays.sort(times);
    report("floor: double[] loop", times[RUNS / 2], samples);
  }

  private void report(final String name, final double medianMs, final int samples) {
    LogManager.instance().log(this, Level.INFO, String.format(Locale.ROOT, "RESULT %-40s median %8.1f ms | %6.1f ns per candidate sample", name,
        medianMs, medianMs * 1e6 / samples));
  }
}
