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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StringUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12837: {@code format('%f')} of a decimal with a huge exponent expanded it to its plain spelling: {@code 1E+99999999}
 * took seconds and a gigabyte, {@code 1E+999999999} was an OutOfMemoryError.
 */
class Issue12837FormatHugeDecimalTest extends TestHelper {
  @Test
  void aHugeDecimalIsRefusedAsAValidationError() {
    assertThatThrownBy(() -> run("SELECT '1e999999999'.asDecimal().format('%f') AS v")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1000000");
    assertThatThrownBy(() -> run("SELECT format('%.2f', '1e99999999'.asDecimal()) AS v")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> run("SELECT '1e-999999999'.asDecimal().format('%f') AS v")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%f", new BigDecimal("1E+2000000"))).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void theOtherConversionsAndOrdinaryDecimalsStillFormat() {
    assertThat(run("SELECT '1e999999999'.asDecimal().format('%s') AS v")).isEqualTo("1E+999999999");
    assertThat(run("SELECT '1e999999999'.asDecimal().format('%e') AS v")).isEqualTo("1.000000e+999999999");
    assertThat(run("SELECT '1234.5'.asDecimal().format('%.2f') AS v")).isEqualTo("1234.50");
    // 301 digits and ".0"
    assertThat(run("SELECT '1e300'.asDecimal().format('%.1f') AS v").toString()).hasSize(303);
    assertThat(StringUtils.format("format", "%f %s", new BigDecimal("1E+400"), "x")).endsWith(" x");
  }

  private Object run(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }
}
