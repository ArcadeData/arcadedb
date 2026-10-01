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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8813: after COMPACT TIMESERIES TYPE, a series whose samples arrived out of order answered
 * {@code ORDER BY ts ASC LIMIT 1} with a sample that is not the oldest.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8813CompactedOldestSampleTest extends TestHelper {
  private static final int  N      = 200_000;
  private static final int  COMMIT = 1_000;
  private static final int  COMPACT_EVERY = 68_000;
  private static final long T0     = 1_700_000_000_000L;

  private long ts(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Object o = rs.next().getProperty("ts");
      return switch (o) {
        case Date d -> d.getTime();
        case Instant i -> i.toEpochMilli();
        case LocalDateTime l -> l.toInstant(ZoneOffset.UTC).toEpochMilli();
        default -> ((Number) o).longValue();
      };
    }
  }

  @Test
  void oldestSampleAfterCompactOfOutOfOrderSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE R TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");
    final Random rnd = new Random(43);
    long min = Long.MAX_VALUE;
    final long[] all = new long[N];
    long window = 0;
    database.begin();
    for (int i = 0; i < N; i++) {
      if (i % COMMIT == 0)
        window = (long) (rnd.nextDouble() * 47 * 3600_000);
      final long t = T0 + window + (long) (rnd.nextDouble() * 3600_000);
      min = Math.min(min, t);
      all[i] = t;
      database.command("sql", "INSERT INTO R SET ts = ?, host = 'h0', v = ?", new Date(t), rnd.nextDouble());
      if ((i + 1) % COMMIT == 0) {
        database.commit();
        // the reporter's server compacted in the background while the load was running: the sealed store already holds
        // earlier, overlapping blocks when the final COMPACT runs
        if ((i + 1) % COMPACT_EVERY == 0)
          database.command("sql", "COMPACT TIMESERIES TYPE R").close();
        database.begin();
      }
    }
    database.commit();

    assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 1")).as("before COMPACT").isEqualTo(min);
    database.command("sql", "COMPACT TIMESERIES TYPE R").close();
    assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 1")).as("ORDER BY ts ASC LIMIT 1").isEqualTo(min);
    assertThat(ts("SELECT ts FROM R LIMIT 1")).as("bare LIMIT 1").isEqualTo(min);
    assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 2")).as("first of LIMIT 2").isEqualTo(min);
    assertThat(ts("SELECT min(ts) AS ts FROM R")).as("min(ts)").isEqualTo(min);
    assertRangesAndOldest(all, min, "compacted");

    // the directory is rebuilt from the file on open, where the blocks sit in the order they were written
    reopenDatabase();
    assertRangesAndOldest(all, min, "reopened");
  }

  private void assertRangesAndOldest(final long[] all, final long min, final String when) {
    assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 1")).as(when + " ORDER BY ts ASC LIMIT 1").isEqualTo(min);
    final long span = 47L * 3600_000;
    for (int w = 0; w < 12; w++) {
      final long from = T0 + w * span / 12;
      final long to = from + span / 24;
      long expected = 0;
      for (final long t : all)
        if (t >= from && t <= to)
          ++expected;
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM R WHERE ts >= ? AND ts <= ?", new Date(from), new Date(to))) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as(when + " count in window " + w).isEqualTo(expected);
      }
    }
  }
}
