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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9801: Prometheus selects a window LEFT-OPEN, {@code (t - window, t]}. A sample exactly one lookback old is
 * stale for an instant selector, and {@code [5m]} excludes the sample at {@code t-5m}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9801PromQLLeftOpenWindowTest extends TestHelper {
  private static final long T        = 1_700_000_000_000L;
  private static final long LOOKBACK = 300_000L;

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE TIMESERIES TYPE m1 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO m1 SET ts = ?, host = 'h1', value = 42.0", T));
  }

  @Test
  void aSampleExactlyOneLookbackOldIsStale() {
    assertThat(instant("m1", T + LOOKBACK)).isEmpty();
    assertThat(instant("m1", T + LOOKBACK - 1)).hasSize(1);
    assertThat(instant("m1", T + LOOKBACK + 1)).isEmpty();
    assertThat(instant("m1", T)).hasSize(1);
  }

  @Test
  void theOffsetShiftsBothEndsOfTheWindow() {
    assertThat(instant("m1 offset 1m", T + LOOKBACK + 60_000L)).isEmpty();
    assertThat(instant("m1 offset 1m", T + LOOKBACK + 59_999L)).hasSize(1);
  }

  @Test
  void aRangeSelectorExcludesTheSampleOnItsLowerBound() {
    assertThat(instant("count_over_time(m1[5m])", T + LOOKBACK)).isEmpty();
    assertThat(instant("count_over_time(m1[5m])", T + LOOKBACK - 1)).hasSize(1);

    final RangeVector range = (RangeVector) new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(
        new PromQLParser("m1[5m]").parse(), T + LOOKBACK - 1);
    final RangeSeries series = range.series().getFirst();
    // The window itself still starts at t-5m: only which samples fall in it changed
    assertThat(series.rangeStartMs()).isEqualTo(T - 1);
    assertThat(series.rangeEndMs()).isEqualTo(T + LOOKBACK - 1);
  }

  @Test
  void aScrapeIntervalThatDividesTheRangeCountsItsSamplesOnce() {
    // A 1m feed over [5m]: 6 samples from T to T+5m, evaluated at T+5m, Prometheus 3.x counts the last 5
    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("sql", "INSERT INTO m1 SET ts = ?, host = 'h1', value = 42.0", T + i * 60_000L);
    });
    final List<VectorSample> samples = instant("count_over_time(m1[5m])", T + LOOKBACK);
    assertThat(samples).hasSize(1);
    assertThat(samples.getFirst().value()).isEqualTo(5.0);
  }

  @Test
  void theSqlFunctionAgrees() {
    try (final ResultSet rs = database.query("sql", "SELECT promql('m1', ?) AS r", T + LOOKBACK)) {
      assertThat((List<Map<String, Object>>) rs.next().getProperty("r")).isEmpty();
    }
    try (final ResultSet rs = database.query("sql", "SELECT promql('m1', ?) AS r", T + LOOKBACK - 1)) {
      assertThat((List<Map<String, Object>>) rs.next().getProperty("r")).hasSize(1);
    }
  }

  private List<VectorSample> instant(final String promql, final long evalTimeMs) {
    final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(
        new PromQLParser(promql).parse(), evalTimeMs);
    return ((InstantVector) result).samples();
  }
}
