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
package com.arcadedb.function.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * round(x, precision) handed the precision straight to {@link BigDecimal#setScale}, which materializes 10^|precision -
 * scale| as a BigInteger: precision 10,000,000 already took seconds, 100,000,000 minutes, and one near the int range
 * overflowed BigInteger, an internal error for what is a legal argument. The answer never needs a
 * scale past the value's own (more digits add zeros) nor below the one that rounds every finite double to 0 or to a
 * power of ten no double can hold, so the scale is clamped to that window and the result is the same as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RoundExtremePrecisionTest extends TestHelper {
  // every mode round() accepts (UNNECESSARY is not one of them)
  private static final RoundingMode[] MODES = { RoundingMode.UP, RoundingMode.DOWN, RoundingMode.CEILING, RoundingMode.FLOOR,
      RoundingMode.HALF_UP, RoundingMode.HALF_DOWN, RoundingMode.HALF_EVEN };

  // A hang detector, not a latency bound: before the clamp these calls ran for minutes
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aLargePrecisionAnswersAtOnce() {
    assertThat(round(1.5, 100_000_000, "HALF_UP")).isEqualTo(1.5);
    assertThat(round(1.5, -100_000_000, "UP")).isEqualTo(Double.POSITIVE_INFINITY);
  }

  // Before the clamp these overflowed BigInteger: an ArithmeticException, reported as an internal error
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anExtremePrecisionAnswersAtOnce() {
    assertThat(round(1.5, Integer.MAX_VALUE, "HALF_UP")).isEqualTo(1.5);
    assertThat(round(-1.5, Integer.MAX_VALUE, "FLOOR")).isEqualTo(-1.5);
    assertThat(round(Double.MIN_VALUE, Integer.MAX_VALUE, "UP")).isEqualTo(Double.MIN_VALUE);
    assertThat(round(1.5, Integer.MIN_VALUE, "HALF_UP")).isEqualTo(0.0);
    assertThat(round(Double.MAX_VALUE, Integer.MIN_VALUE, "HALF_EVEN")).isEqualTo(0.0);
    // the unit 10^2147483648 is not a double: rounding away from zero overflows as it did before the clamp
    assertThat(round(1.5, Integer.MIN_VALUE, "UP")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(round(-1.5, Integer.MIN_VALUE, "FLOOR")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(round(-1.5, Integer.MIN_VALUE, "CEILING")).isEqualTo(0.0);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anExtremePrecisionThroughCypherAnswersAtOnce() {
    assertThat(cypher("RETURN round(1.5, 2147483647) AS r")).isEqualTo(1.5);
    assertThat(cypher("RETURN round(1.5, -2147483648) AS r")).isEqualTo(0.0);
    assertThat(cypher("RETURN round(1.5, -2147483648, 'UP') AS r")).isEqualTo(Double.POSITIVE_INFINITY);
  }

  /**
   * The clamp is an equivalence, not an approximation: around both edges of the window every mode answers what the
   * unclamped BigDecimal does (cheap at these sizes).
   */
  @Test
  void theClampedScaleAnswersWhatTheUnclampedOneDoes() {
    final double[] values = { 1.5, -1.5, 2.5, -2.5, 0.0, -0.0, 0.1, 123456.789, -987.654321, Double.MIN_VALUE, -Double.MIN_VALUE,
        Double.MAX_VALUE, -Double.MAX_VALUE, 1.0e19, 4.9e-300, 6.02214076e23, 0x1p52 + 1 };
    for (final double value : values)
      for (final RoundingMode mode : MODES)
        for (final int precision : precisionsAroundTheEdges()) {
          final double expected = BigDecimal.valueOf(value).setScale(precision, mode).doubleValue();
          assertThat(round(value, precision, mode.name())).as("round(%s, %d, %s)", value, precision, mode).isEqualTo(expected);
        }
  }

  private static int[] precisionsAroundTheEdges() {
    final int[] precisions = new int[2 * 130 + 2 * 40];
    int i = 0;
    for (int p = -460; p < -330; ++p)
      precisions[i++] = p;
    for (int p = -330; p < -200; p += 1)
      precisions[i++] = p;
    for (int p = -20; p < 20; ++p)
      precisions[i++] = p;
    for (int p = 300; p < 380; p += 2)
      precisions[i++] = p;
    return precisions;
  }

  private static double round(final double value, final int precision, final String mode) {
    return ((Number) new RoundFunction().execute(new Object[] { value, precision, mode }, null)).doubleValue();
  }

  private double cypher(final String query) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of())) {
      return rs.next().<Number>getProperty("r").doubleValue();
    }
  }
}
