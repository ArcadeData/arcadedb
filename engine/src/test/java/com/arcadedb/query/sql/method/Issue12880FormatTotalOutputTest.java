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
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12880: the width limit of {@code format()} bounded each specifier and not the pattern, so a pattern repeating an
 * allowed specifier still built a gigabyte: a repeated wide field, or one long argument printed again and again.
 * <p>
 * Every refusal here uses the smallest pattern over the ceiling: without the fix it formats some 17MB and fails an
 * assertion, where the patterns of the report would take the test JVM down.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12880FormatTotalOutputTest extends TestHelper {
  private static final int WIDTH = StringUtils.MAX_FORMAT_WIDTH;
  private static final int FIELDS = StringUtils.MAX_FORMAT_OUTPUT / WIDTH;

  @Test
  void aRepeatedWideFieldIsRefused() {
    final String field = "%" + WIDTH + "s";
    final String arguments = ", 'x'".repeat(FIELDS + 1);

    assertThatThrownBy(() -> run("SELECT format('" + field.repeat(FIELDS + 1) + "'" + arguments + ") AS v")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThatThrownBy(() -> run("SELECT 'x'.format('" + ("%1$" + WIDTH + "s").repeat(FIELDS + 1) + "') AS v")).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", ("%-" + WIDTH + "d").repeat(FIELDS + 1), ordinals(FIELDS + 1))).isInstanceOf(
        IllegalArgumentException.class);
    // The precision of a number is as many digits as the width is of padding
    assertThatThrownBy(() -> StringUtils.format("format", "%." + WIDTH + "e" + ("%<." + WIDTH + "e").repeat(FIELDS), 1.5d)).isInstanceOf(
        IllegalArgumentException.class);
    // So is the width of a literal percent sign
    assertThatThrownBy(() -> StringUtils.format("format", ("%-" + WIDTH + "%").repeat(FIELDS + 1))).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void aFewWideFieldsStillFormat() {
    assertThat(run("SELECT format('%" + WIDTH + "s', 'x') AS v").toString()).hasSize(WIDTH);
    assertThat(run("SELECT format('%" + WIDTH + "s|%-" + WIDTH + "s|%" + WIDTH + "d', 'x', 'y', 3) AS v").toString()).hasSize(
        3 * WIDTH + 2);
    // The padding of a field is what counts, and sixteen full fields are the last size printed
    final String sixteen = StringUtils.format("format", ("%" + WIDTH + "s").repeat(FIELDS), (Object[]) "x".repeat(FIELDS).split(""));
    assertThat(sixteen).hasSize(FIELDS * WIDTH);
  }

  @Test
  void aDecimalSpelledOutAgainAndAgainIsRefused() {
    final String pattern = "%.0f" + "%<.0f".repeat(FIELDS);
    assertThatThrownBy(() -> run("SELECT format('" + pattern + "', '1e999999'.asDecimal()) AS v")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThatThrownBy(() -> run("SELECT '1e999999'.asDecimal().format('" + pattern + "') AS v")).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$.0f".repeat(FIELDS + 1), new BigDecimal("1E+999999"))).isInstanceOf(
        IllegalArgumentException.class);

    // Printed twice it is two million digits, as before
    assertThat(StringUtils.format("format", "%.0f%<.0f", new BigDecimal("1E+999999"))).hasSize(2 * WIDTH);
  }

  @Test
  void aDoubleSpelledOutAgainAndAgainIsRefused() {
    // 1e308 is 309 digits under %f: 3 characters of pattern each
    assertThatThrownBy(() -> StringUtils.format("format", "%f" + "%<f".repeat(60_000), 1e308d)).isInstanceOf(
        IllegalArgumentException.class);

    final String pattern = "%f" + "%<f".repeat(1_000);
    assertThat(StringUtils.format("format", pattern, 1e308d)).isEqualTo(String.format(Locale.ROOT, pattern, 1e308d));
  }

  @Test
  void aLongStringPrintedAgainAndAgainIsRefused() {
    final String text = "a".repeat(WIDTH);

    // The first print is the argument itself, each further one is a copy the pattern asks for
    assertThatThrownBy(() -> run("SELECT format('" + "%1$s".repeat(FIELDS + 2) + "', :text) AS v", text)).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThatThrownBy(() -> run("SELECT :text.format('" + "%s" + "%<s".repeat(FIELDS + 1) + "') AS v", text)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%2$s" + "%2$S".repeat(FIELDS + 1), 1, text)).isInstanceOf(
        IllegalArgumentException.class);

    // One copy fewer is the last size printed
    assertThat(run("SELECT format('" + "%1$s".repeat(FIELDS + 1) + "', :text) AS v", text).toString()).hasSize((FIELDS + 1) * WIDTH);
  }

  @Test
  void anArgumentPastTheSixtyFourthIsCountedLikeTheOthers() {
    final Object[] args = ordinals(70);
    args[68] = "a".repeat(WIDTH);

    assertThatThrownBy(() -> StringUtils.format("format", "%69$s".repeat(FIELDS + 2), args)).isInstanceOf(
        IllegalArgumentException.class);
    assertThat(StringUtils.format("format", "%69$s".repeat(FIELDS + 1), args)).hasSize((FIELDS + 1) * WIDTH);
  }

  @Test
  void anObjectPrintedAgainAndAgainIsMeasuredByItsText() {
    final List<String> list = List.of("a".repeat(WIDTH));
    assertThatThrownBy(() -> StringUtils.format("format", "%1$s".repeat(FIELDS + 2), list)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%s" + "%<s".repeat(FIELDS + 1), list)).isInstanceOf(
        IllegalArgumentException.class);
    assertThat(StringUtils.format("format", "%1$s %1$s", list)).hasSize(2 * (WIDTH + 2) + 1);

    // A million digits and more, never spelled out by the refusal
    final BigInteger integer = BigInteger.ONE.shiftLeft(3_400_000);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$d".repeat(FIELDS + 2), integer)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$s".repeat(FIELDS + 2), integer)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$x".repeat(FIELDS + 6), integer)).isInstanceOf(
        IllegalArgumentException.class);
    final BigInteger small = BigInteger.TEN.pow(40);
    assertThat(StringUtils.format("format", "%1$d %1$s %1$x %1$,d", small)).isEqualTo(
        String.format(Locale.ROOT, "%1$d %1$s %1$x %1$,d", small));
  }

  @Test
  void anUpperCasedArgumentCountsForWhatItGrowsTo() {
    // 'ß' is printed as 'SS' by %S: two million characters each time, of which the argument holds one million once
    final String text = "ß".repeat(WIDTH);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$S".repeat(FIELDS / 2 + 1), text)).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$s" + "%1$S".repeat(FIELDS / 2 + 1), text)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$S".repeat(FIELDS / 2 + 1), List.of(text))).isInstanceOf(
        IllegalArgumentException.class);
    // A short one is printed: the formatter itself takes minutes to upper-case a million of them
    final String brief = "ß".repeat(1_000);
    assertThat(StringUtils.format("format", "%1$S%1$S|%1$s", brief)).isEqualTo("SS".repeat(2_000) + "|" + brief);

    // The precision truncates before the case is changed
    assertThat(StringUtils.format("format", "%1$.5S".repeat(100_000), text)).hasSize(1_000_000);

    // Text that is no longer in upper case is counted as it was
    final String plain = "a".repeat(WIDTH);
    assertThat(StringUtils.format("format", "%1$S".repeat(FIELDS + 1), plain)).hasSize((FIELDS + 1) * WIDTH);
    assertThatThrownBy(() -> StringUtils.format("format", "%1$S".repeat(FIELDS + 2), plain)).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void aNullArgumentArrayIsCountedAsNullArguments() {
    // The formatter prints 'null' for every specifier of a null array, padded to the width
    assertThatThrownBy(() -> StringUtils.format("format", ("%" + WIDTH + "s").repeat(FIELDS + 1), (Object[]) null)).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThat(StringUtils.format("format", "%6s|%s|%b", (Object[]) null)).isEqualTo(
        String.format(Locale.ROOT, "%6s|%s|%b", (Object[]) null));
  }

  @Test
  void theGroupingOfAGeneralScientificFieldCountsItsPrecision() {
    // %g prints as many integer digits as its precision, and one separator every three of them
    assertThatThrownBy(
        () -> StringUtils.format("format", ("%1$,." + WIDTH + "g").repeat(FIELDS - 1), new BigDecimal("1E+999999"))).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThat(StringUtils.format("format", "%1$,.10g %1$,g", 1234567.5d)).isEqualTo(
        String.format(Locale.ROOT, "%1$,.10g %1$,g", 1234567.5d));
  }

  // Formats strings of 17MB and 34MB for real
  @Test
  @Tag("slow")
  void whatTheCallerAlreadyHoldsIsNotCounted() {
    final int large = StringUtils.MAX_FORMAT_OUTPUT + WIDTH;
    final String text = "a".repeat(large);

    // One long string, printed once: by the function, by the method, and beside others
    assertThat(run("SELECT format('%s', :text) AS v", text)).isEqualTo(text);
    assertThat(run("SELECT :text.format('[%s]') AS v", text).toString()).hasSize(large + 2);
    assertThat(StringUtils.format("format", "%s%s", text, text)).hasSize(2 * large);
    assertThat(StringUtils.format("format", "%S", new StringBuilder(text))).hasSize(large);

    // The literal text of the pattern
    assertThat(StringUtils.format("format", text + "%d%%%n", 7)).hasSize(large + 2 + System.lineSeparator().length());
  }

  @Test
  void aTruncatedArgumentCountsForWhatIsPrintedOfIt() {
    final String text = "a".repeat(WIDTH);
    final String pattern = "%1$.5s".repeat(100_000);
    assertThat(StringUtils.format("format", pattern, text)).hasSize(500_000);
  }

  @Test
  void aLongPatternOfSmallSpecifiersStillFormats() {
    final String pattern = "%1$d %2$s %3$.2f %4$b %5$c %6$e|".repeat(20_000);
    final Object[] args = { 42, "x", 1.5d, true, 'c', 1234.5d };
    assertThat(StringUtils.format("format", pattern, args)).isEqualTo(String.format(Locale.ROOT, pattern, args));
  }

  /**
   * The estimate is what the ceiling is taken on, so it must not fall short of what the formatter writes: each pattern
   * is formatted for real and repeated up to just under the ceiling of the estimate, which the output may not exceed
   * by more than the arguments it prints. Each refusal comes before anything is formatted, but the patterns alone are
   * up to 32MB each.
   */
  @Test
  @Tag("slow")
  void theOutputStaysUnderTheCeilingPlusTheArguments() {
    final BigDecimal decimal = new BigDecimal("123456789012345678901234567890.123456789");
    final BigInteger integer = BigInteger.TEN.pow(60).negate();
    final String text = "some text";
    final Object[] args = { decimal, integer, text, 1e308d, -Float.MAX_VALUE, Long.MIN_VALUE, true, 'c', new Date(0),
        List.of("a", "b"), null, Double.NaN, "straße ﬃ" };
    final String[] specifiers = { "%1$f", "%1$,.20f", "%1$s", "%1$e", "%1$g", "%1$,.40g", "%1$,.3f", "%2$d", "%2$,d", "%2$o", "%2$x", "%2$s",
        "%2$(,d", "%3$s", "%3$S", "%13$S", "%13$.2S", "%3$20s", "%3$.3s", "%3$b", "%3$h", "%4$f", "%4$,f", "%4$.0f", "%4$e", "%4$a", "%4$g", "%4$s",
        "%5$f", "%5$,.10f", "%6$d", "%6$,d", "%6$o", "%6$x", "%6$s", "%6$+,20d", "%7$b", "%7$s", "%8$c", "%8$s", "%9$tc", "%9$tF",
        "%9$tA %9$tB", "%9$s", "%10$s", "%10$30s", "%11$s", "%11$b", "%11$d", "%11$f", "%12$f", "%12$e", "%12$s", "%-5%" };

    int checked = 0;
    int argumentLength = 0;
    for (final Object arg : args)
      argumentLength += String.valueOf(arg).length();

    for (final String specifier : specifiers) {
      final String once = String.format(Locale.ROOT, specifier, args);
      assertThat(StringUtils.format("format", specifier, args)).as(specifier).isEqualTo(once);

      // A specifier that writes no more than its own text cannot take the output over the pattern
      if (once.length() <= specifier.length())
        continue;
      checked++;

      // As many repetitions as the formatter needs to write twice the ceiling
      final int repetitions = 2 * StringUtils.MAX_FORMAT_OUTPUT / Math.max(once.length(), 1) + 1;
      final String pattern = specifier.repeat(repetitions);
      String formatted = null;
      try {
        formatted = StringUtils.format("format", pattern, args);
      } catch (final IllegalArgumentException e) {
        assertThat(e).as(specifier).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
      }
      if (formatted != null)
        assertThat(formatted.length()).as(specifier).isLessThanOrEqualTo(
            StringUtils.MAX_FORMAT_OUTPUT + pattern.length() + argumentLength);
    }
    assertThat(checked).isGreaterThan(30);
  }

  @Test
  void theOtherCallersShareTheCeiling() {
    final String text = "a".repeat(WIDTH);
    final String pattern = "%1$s".repeat(FIELDS + 2);

    final StatelessFunction textFormat = CypherFunctionRegistry.get("text.format");
    assertThatThrownBy(() -> textFormat.execute(new Object[] { pattern, List.of(text) }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("more than " + StringUtils.MAX_FORMAT_OUTPUT);
    assertThat(textFormat.execute(new Object[] { "%1$s and %1$s", List.of("x") }, null)).isEqualTo("x and x");

    // util.validate keeps its own answer for a message it cannot format: the text and the params, as given
    final StatelessFunction validate = CypherFunctionRegistry.get("util.validate");
    final String wide = ("%1$" + WIDTH + "s").repeat(FIELDS + 1);
    assertThatThrownBy(() -> validate.execute(new Object[] { true, wide, List.of("x") }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessage(wide + " [x]");
    // That one is turned away by util.validate's own width check: a pattern with no width reaches the shared ceiling
    assertThatThrownBy(() -> validate.execute(new Object[] { true, pattern, List.of("x".repeat(WIDTH)) }, null)).isInstanceOf(
        IllegalArgumentException.class).hasMessageStartingWith(pattern + " [xxx");
  }

  @Test
  void theRefusalDoesNotRepeatALongPattern() {
    final String pattern = "long literal text ".repeat(1_000) + ("%1$" + WIDTH + "s").repeat(FIELDS + 1);
    assertThatThrownBy(() -> StringUtils.format("format", pattern, "x")).isInstanceOf(IllegalArgumentException.class)
        .satisfies(e -> assertThat(e.getMessage().length()).isLessThan(400));
  }

  private static Object[] ordinals(final int count) {
    final Object[] args = new Object[count];
    for (int i = 0; i < count; i++)
      args[i] = i;
    return args;
  }

  private Object run(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }

  private Object run(final String sql, final String text) {
    try (final ResultSet rs = database.query("sql", sql, Map.of("text", text))) {
      return rs.next().getProperty("v");
    }
  }
}
