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
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StringUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12837: {@code format('%f')} of a decimal with a huge exponent expanded it to its plain spelling: {@code 1E+99999999}
 * took seconds and a gigabyte, {@code 1E+999999999} was an OutOfMemoryError.
 * <p>
 * The refusals that go through the formatter when the guard is missing use exponents just over the limit: without the fix
 * they cost a couple of megabytes and fail an assertion, where the exponents of the report would take the test JVM down. The
 * exponents of the report are kept where a missing guard cannot reach the formatter.
 */
class Issue12837FormatHugeDecimalTest extends TestHelper {
  @Test
  void aHugeDecimalIsRefusedAsAValidationError() {
    assertThatThrownBy(() -> run("SELECT '1e2000000'.asDecimal().format('%f') AS v")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1000000");
    assertThatThrownBy(() -> run("SELECT format('%.2f', '1e2000000'.asDecimal()) AS v")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> run("SELECT '1e-2000000'.asDecimal().format('%f') AS v")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%f", new BigDecimal("1E+2000000"))).isInstanceOf(
        IllegalArgumentException.class);
    // A zero is padded like any other value: 0E+2000000 is two million zeros
    assertThatThrownBy(() -> StringUtils.format("format", "%f", new BigDecimal("0E+2000000"))).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("text.format", Locale.ENGLISH, "%,.2f", new BigDecimal("1E+2000000"))).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void theLimitIsOnThePlainDigitsAndNotOnTheExponentText() {
    // 1,000,000 integer digits is the last size printed, one more is refused
    assertThat(StringUtils.format("format", "%.0f", new BigDecimal("1E+999999"))).hasSize(1_000_000);
    assertThatThrownBy(() -> StringUtils.format("format", "%.0f", new BigDecimal("1E+1000000"))).isInstanceOf(
        IllegalArgumentException.class);
    // The scale spans the whole int range: the digit count must not overflow into an accepted value
    assertThatThrownBy(() -> StringUtils.format("format", "%f", new BigDecimal("1E+2147483647"))).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%f", new BigDecimal("1E-2147483647"))).isInstanceOf(
        IllegalArgumentException.class);
  }

  /**
   * Only the argument a fixed-point conversion consumes is measured. Every pattern is formatted for real, with a decimal one
   * digit over the limit in one argument position, and the guard must refuse exactly the patterns whose output spells it out:
   * a pattern refused although the formatter writes a short text is a valid query broken, one accepted although it writes
   * over a million characters is the hole of the issue, reopened through the argument index.
   */
  @Test
  void onlyTheArgumentOfAFixedPointConversionIsMeasured() {
    final BigDecimal huge = new BigDecimal("1E+1000000");
    final BigDecimal small = new BigDecimal("1.5");
    final String[] patterns = { "%f", "%s", "%e", "%g", "%s %f", "%f %s", "%2$f", "%1$f", "%2$s %1$f", "%2$f %1$s", "%s %<f", "%f %<s",
        "%2$s %<f", "%1$s %<f", "%s %s %1$f", "%s %s %2$f", "%2$s %f", "%1$s %s %f", "%n%f", "%%%f", "%% %s %f", "%n %2$s %f", "%-5% %f",
        "%s %1$<f", "%2$s %1$<f", "%.2f %s", "%s %,.2f", "%010.1f %s", "%2$e %1$g", "%tY %f", "%s %S %f", "%3$f", "%f %f %f", "%d %f",
        "%f %y", "%0$f" };

    int refused = 0;
    int spelledOut = 0;
    for (final String pattern : patterns)
      for (int position = 0; position < 2; position++) {
        final Object[] args = position == 0 ? new Object[] { huge, small } : new Object[] { small, huge };

        // What the formatter itself does with the pattern: an output over the limit, a short one, or an error of its own
        boolean overTheLimit = false;
        boolean formats = true;
        try {
          overTheLimit = String.format(Locale.ROOT, pattern, args).length() > StringUtils.MAX_FORMAT_WIDTH;
        } catch (final IllegalArgumentException e) {
          formats = false;
        }

        final String description = "pattern '" + pattern + "' with the huge decimal as argument " + (position + 1);
        if (overTheLimit) {
          spelledOut++;
          assertThatThrownBy(() -> StringUtils.format("format", pattern, args)).as(description).isInstanceOf(
              IllegalArgumentException.class).hasMessageContaining("would spell a decimal");
        } else if (formats)
          assertThat(StringUtils.format("format", pattern, args)).as(description).isEqualTo(String.format(Locale.ROOT, pattern, args));
        else {
          refused++;
          assertThatThrownBy(() -> StringUtils.format("format", pattern, args)).as(description).isInstanceOf(
              IllegalArgumentException.class);
        }
      }

    // The grid holds all three outcomes, or it compares nothing
    assertThat(spelledOut).isGreaterThan(10);
    assertThat(refused).isGreaterThan(3);
  }

  @Test
  void aHugeDecimalBesideAnUnrelatedFixedPointConversionStillFormats() {
    assertThat(run("SELECT format('%s is not %f', '1e999999999'.asDecimal(), 1.5) AS v")).isEqualTo("1E+999999999 is not 1.500000");
    assertThat(run("SELECT format('%2$f is not %1$e', '1e999999999'.asDecimal(), 1.5) AS v")).isEqualTo(
        "1.500000 is not 1.000000e+999999999");
  }

  /**
   * util.validate formats its message with the params through the same formatter, and never reached the guard. It keeps its
   * own answer for a message it cannot format: the text and the params, as given.
   */
  @Test
  void utilValidateDoesNotSpellAHugeDecimalOut() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.validate");
    assertThatThrownBy(() -> fn.execute(new Object[] { true, "too big: %f", List.of(new BigDecimal("1E+2000000")) }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessage("too big: %f [1E+2000000]");
    assertThatThrownBy(() -> fn.execute(new Object[] { true, "fine: %.1f and %s", List.of(new BigDecimal("2.5"), "x") }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessage("fine: " + String.format("%.1f", 2.5) + " and x");
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
