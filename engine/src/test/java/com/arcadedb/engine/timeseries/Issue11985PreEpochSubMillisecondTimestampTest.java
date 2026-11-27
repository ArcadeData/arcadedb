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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser.Precision;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11985: a nanosecond or microsecond timestamp was narrowed to milliseconds with {@code /}, which
 * truncates toward zero, so an instant before 1970 with a sub-millisecond part moved FORWARD - {@code -1} ns (one nanosecond
 * before the epoch) became {@code 0} ms and landed in the next second, hour and day bucket. The millisecond that contains an
 * instant is its floor, as {@code Instant.toEpochMilli} and {@code ts.timeBucket} (issue #6824) count it.
 */
class Issue11985PreEpochSubMillisecondTimestampTest extends TestHelper {

  @Test
  void aSubMillisecondUnitIsFlooredToTheMillisecondThatContainsTheInstant() {
    assertThat(Precision.NANOSECONDS.toMillis(-1L)).isEqualTo(-1L);
    assertThat(Precision.NANOSECONDS.toMillis(-500_000L)).isEqualTo(-1L);
    assertThat(Precision.NANOSECONDS.toMillis(-1_000_000L)).isEqualTo(-1L);
    assertThat(Precision.NANOSECONDS.toMillis(-1_500_000L)).isEqualTo(-2L);
    assertThat(Precision.MICROSECONDS.toMillis(-1L)).isEqualTo(-1L);
    assertThat(Precision.MICROSECONDS.toMillis(-1_500L)).isEqualTo(-2L);

    // after the epoch nothing changes
    assertThat(Precision.NANOSECONDS.toMillis(1_500_000L)).isEqualTo(1L);
    assertThat(Precision.MICROSECONDS.toMillis(999L)).isEqualTo(0L);
    assertThat(Precision.SECONDS.toMillis(-2L)).isEqualTo(-2_000L);
  }

  @Test
  void aLineOneNanosecondBeforeTheEpochIsStoredInTheLastMillisecondOf1969() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");

    final List<Sample> samples = LineProtocolParser.parse("""
        M,host=a v=1.0 -1
        M,host=b v=2.0 -1500000
        """, Precision.NANOSECONDS);
    assertThat(samples).extracting(Sample::getTimestampMs).containsExactly(-1L, -2L);

    TimeSeriesGateway.write((DatabaseInternal) database, samples);

    try (final ResultSet rs = database.query("sql",
        "SELECT host, (ts).asLong() AS t, (ts.timeBucket('1d', ts)).asLong() AS day FROM M ORDER BY host")) {
      final Result a = rs.next();
      assertThat(a.<Long>getProperty("t")).isEqualTo(-1L);
      assertThat(a.<Long>getProperty("day")).isEqualTo(-86_400_000L);
      final Result b = rs.next();
      assertThat(b.<Long>getProperty("t")).isEqualTo(-2L);
      assertThat(b.<Long>getProperty("day")).isEqualTo(-86_400_000L);
    }
  }
}
