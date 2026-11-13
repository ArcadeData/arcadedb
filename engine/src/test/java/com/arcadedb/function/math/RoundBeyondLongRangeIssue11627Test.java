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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11627: round(x) went through Math.round(double), which saturates at the long range, so every
 * |x| >= 2^63 (Double.MAX_VALUE among them, an extreme value of Neo4j's ExtremeValuesLibrary) rounded to +-9.223372036854776E18.
 */
class RoundBeyondLongRangeIssue11627Test extends TestHelper {

  @Test
  void aDoubleBeyondTheLongRangeRoundsToItself() {
    for (final double value : new double[] { 1.0e19, -1.0e19, 9.3e18, Double.MAX_VALUE, -Double.MAX_VALUE, 0x1p63, 0x1p52 + 1 })
      assertThat(round(value)).as("round(%s)", value).isEqualTo(value);
    // below 2^52 a double may have a fraction, which rounds half up as before
    assertThat(round(0x1p51 + 0.5)).isEqualTo(0x1p51 + 1);
    assertThat(round(2.5)).isEqualTo(3.0);
    assertThat(round(-2.5)).isEqualTo(-2.0);
    assertThat(round(-0.4)).isEqualTo(0.0);
  }

  private double round(final double value) {
    try (final ResultSet rs = database.query("opencypher", "RETURN round($v) AS r", Map.of("v", value))) {
      return rs.next().<Number>getProperty("r").doubleValue();
    }
  }
}
