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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for issue #11981: {@code rate()}/{@code increase()} applied the counter zero-point limit BEFORE the
 * extrapolation threshold, the order Prometheus changed in 2.51 (prometheus/prometheus#13725). A zero point that falls between half
 * a sample spacing and the window start then extrapolated past the half spacing. The expected values are what Prometheus v3.5.0
 * answers for the same samples.
 */
class Issue11981RateExtrapolationOrderTest extends TestHelper {

  @Test
  void theZeroPointNoLongerExtrapolatesPastHalfASampleSpacing() {
    database.command("sql", "CREATE TIMESERIES TYPE cnt TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO cnt SET ts = 1700000200000, host = 'c', value = 4.11");
      database.command("sql", "INSERT INTO cnt SET ts = 1700000201492, host = 'c', value = 8.106");
    });

    // sampled interval 1.492 s, increase 3.996, zero point 1.535 s before the first sample, window start 200 s before it:
    // (1.492 + 0.746 + 0.746) / 1.492 * 3.996 = 7.992, where the old order answered (1.492 + 1.535 + 0.746) / 1.492 * 3.996 = 10.104
    assertThat(value("increase(cnt[10m])", 1_700_000_600_000L)).isCloseTo(7.992, within(1e-9));
    assertThat(value("rate(cnt[10m])", 1_700_000_600_000L)).isCloseTo(7.992 / 600, within(1e-12));
  }

  @Test
  void aZeroPointInsideHalfASpacingStillLimitsTheExtrapolation() {
    // first value 1, increase 100 over 10 s: the zero point is 0.1 s back, under the 5 s half spacing, so it wins in both orders
    database.command("sql", "CREATE TIMESERIES TYPE cnt2 TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO cnt2 SET ts = 1700000100000, value = 1.0");
      database.command("sql", "INSERT INTO cnt2 SET ts = 1700000110000, value = 101.0");
    });

    // window (1700000060, 1700000120]: start 40 s before the first sample (past the 11 s threshold), end 10 s after the last (under it)
    // (10 + 0.1 + 10) / 10 * 100 = 201
    assertThat(value("increase(cnt2[1m])", 1_700_000_120_000L)).isCloseTo(201.0, within(1e-9));
  }

  private double value(final String query, final long atMs) {
    final InstantVector vector = (InstantVector) new PromQLEvaluator((DatabaseInternal) database)
        .evaluateInstant(new PromQLParser(query).parse(), atMs);
    assertThat(vector.samples()).hasSize(1);
    return vector.samples().getFirst().value();
  }
}
