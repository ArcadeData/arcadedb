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
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.TimeZone;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9833: only the {@code =} arm of a PromQL label matcher read the matcher value in the tag's declared type. The other three
 * compared the stored value's {@code toString()} with the matcher text, so on any tag whose canonical text differs from what the
 * user wrote, {@code {t="X"}} and {@code {t!="X"}} both selected the series and {@code {t=~"X"}} selected none.
 * <p>
 * The tests pin the claim rather than the cells: for every declared tag type, {@code =} and {@code !=} on the same value select
 * complementary sets, {@code =~} with the value as a literal pattern agrees with {@code =} and {@code !~} with {@code !=}. Each
 * case runs against the instant-vector selector and the range-vector selector, the two scans of the evaluator, and against the
 * mutable bucket and the sealed store, the two layers a row can come from.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9833PromQLTypedTagMatchersTest extends TestHelper {

  private static final long EVAL_TIME_MS = 6_000L;

  private static int typeCounter;

  static Stream<Arguments> typedTags() {
    return Stream.of(
        Arguments.of("LONG", "7", "007"),
        Arguments.of("LONG", "7", "+7"),
        Arguments.of("LONG", "7", "7"),
        Arguments.of("INTEGER", "7", "007"),
        Arguments.of("SHORT", "7", "007"),
        Arguments.of("BYTE", "7", "007"),
        Arguments.of("DOUBLE", "5", "5"),
        Arguments.of("DOUBLE", "5", "5.00"),
        Arguments.of("FLOAT", "5", "5"),
        Arguments.of("DATETIME", "'2024-01-01 00:00:00'", "2024-01-01 00:00:00"),
        Arguments.of("DATE", "'2024-01-01'", "2024-01-01"),
        Arguments.of("STRING", "'007'", "007"),
        Arguments.of("BOOLEAN", "true", "true"));
  }

  @ParameterizedTest(name = "{0} {1} matched by \"{2}\"")
  @MethodSource("typedTags")
  void theFourOperatorsAgreeOnAValueOfTheDeclaredType(final String declaredType, final String stored, final String matcherValue) {
    final String type = createType(declaredType, stored);
    assertOperatorsAgree(type, matcherValue, true);

    compact(type);
    assertOperatorsAgree(type, matcherValue, true);
  }

  @ParameterizedTest(name = "{0} {1} matched by \"{2}\"")
  @MethodSource("typedTags")
  void theFourOperatorsAgreeOnADifferentValue(final String declaredType, final String stored, final String matcherValue) {
    // a second series whose tag is NOT the matcher's value: = must not select it, != must, the patterns likewise
    final String type = createType(declaredType, stored);
    final String other = switch (declaredType) {
      case "DATETIME" -> "2025-06-01 00:00:00";
      case "DATE" -> "2025-06-01";
      case "BOOLEAN" -> "false";
      case "STRING" -> "7";
      default -> "8";
    };
    assertOperatorsAgree(type, other, false);

    compact(type);
    assertOperatorsAgree(type, other, false);
  }

  @Test
  void aRealPatternStillMatchesTheTagAsItIsReported() {
    // the label a typed tag is reported under is its canonical text: a pattern is matched against that text, as before
    final String type = createType("LONG", "7");
    assertThat(count(type + "{code=~\"7|8\"}")).isEqualTo(1);
    assertThat(count(type + "{code=~\"[0-9]\"}")).isEqualTo(1);
    assertThat(count(type + "{code!~\"7|8\"}")).isZero();
    assertThat(count(type + "{code=~\"8|9\"}")).isZero();
    assertThat(count(type + "{code!~\"8|9\"}")).isEqualTo(1);
    // a real pattern is not read as a value: "0.7" matches neither the text "7" nor names the LONG 7
    assertThat(count(type + "{code=~\"0.7\"}")).isZero();
    assertThat(count(type + "{code!~\"0.7\"}")).isEqualTo(1);
    assertThat(count(type + "{code=~\"0+7\"}")).isZero();

    // an unescaped pattern that reads as a value of the declared type names that value too, as = does
    final String doubleType = createType("DOUBLE", "5");
    assertThat(count(doubleType + "{code=~\"5.00\"}")).isEqualTo(1);
    assertThat(count(doubleType + "{code!~\"5.00\"}")).isZero();
  }

  @Test
  void textTheDeclaredTypeCannotReadIsNoValueOfTheTag() {
    final String type = createType("BOOLEAN", "false");
    // "yes" is no boolean: it must not be read as false, on any arm
    assertThat(count(type + "{code=\"yes\"}")).isZero();
    assertThat(count(type + "{code!=\"yes\"}")).isEqualTo(1);
    assertThat(count(type + "{code=~\"yes\"}")).isZero();
    assertThat(count(type + "{code!~\"yes\"}")).isEqualTo(1);
  }

  @Test
  void aDateTimeMatcherIsReadAsTheInsertReadItWhateverTheJvmTimeZone() {
    // The INSERT reads the text through the database; the matcher used to be read with no database, in the JVM's default
    // zone, and so named another instant whenever that zone is not the database's. CI runs in UTC, where the two coincide:
    // a zone with an offset on the stored date is forced for the duration of the test so the difference shows anywhere.
    final TimeZone jvmZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));
      final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
      assertOperatorsAgree(type, "2024-01-01 00:00:00", true);
    } finally {
      TimeZone.setDefault(jvmZone);
    }
  }

  @Test
  void aPatternIsReadAsALiteralOnlyWhenItHasNoUnescapedMetacharacter() {
    assertThat(PromQLEvaluator.literalOfPattern("007")).isEqualTo("007");
    assertThat(PromQLEvaluator.literalOfPattern("5\\.00")).isEqualTo("5.00");
    assertThat(PromQLEvaluator.literalOfPattern("\\+7")).isEqualTo("+7");
    assertThat(PromQLEvaluator.literalOfPattern("a\\\\b")).as("an escaped backslash").isEqualTo("a\\b");
    assertThat(PromQLEvaluator.literalOfPattern("2024-01-01 00:00:00")).as("- is no metacharacter").isEqualTo("2024-01-01 00:00:00");
    assertThat(PromQLEvaluator.literalOfPattern("")).isEmpty();

    assertThat(PromQLEvaluator.literalOfPattern("5.00")).as("an unescaped dot").isNull();
    assertThat(PromQLEvaluator.literalOfPattern("7|8")).isNull();
    assertThat(PromQLEvaluator.literalOfPattern("0+7")).isNull();
    assertThat(PromQLEvaluator.literalOfPattern("\\d")).as("an escaped letter is a character class").isNull();
    assertThat(PromQLEvaluator.literalOfPattern("\\1")).as("an escaped digit is a back reference").isNull();
    assertThat(PromQLEvaluator.literalOfPattern("7\\")).as("a trailing backslash").isNull();
  }

  private void assertOperatorsAgree(final String type, final String value, final boolean expectedEquality) {
    for (final boolean range : new boolean[] { false, true }) {
      final int eq = count(selector(type, "=", value), range);
      final int neq = count(selector(type, "!=", value), range);
      final int re = count(selector(type, "=~", value), range);
      final int nre = count(selector(type, "!~", value), range);
      final String scan = range ? "range selector" : "instant selector";

      assertThat(eq).as("%s: = on %s", scan, value).isEqualTo(expectedEquality ? 1 : 0);
      assertThat(eq + neq).as("%s: = and != on %s must be complements", scan, value).isEqualTo(1);
      assertThat(re).as("%s: =~ with the literal %s must agree with =", scan, value).isEqualTo(eq);
      assertThat(nre).as("%s: !~ with the literal %s must agree with !=", scan, value).isEqualTo(neq);
    }
  }

  private static String selector(final String type, final String op, final String value) {
    // a literal pattern: escape what PromQL's string syntax and the regex engine would read as syntax
    final String pattern = op.contains("~") ? value.replace(".", "\\\\.").replace("+", "\\\\+") : value;
    return type + "{code" + op + "\"" + pattern + "\"}";
  }

  private String createType(final String declaredType, final String stored) {
    final String type = "promql9833_" + declaredType.toLowerCase() + "_" + (++typeCounter);
    database.command("sql", "CREATE TIMESERIES TYPE " + type + " TIMESTAMP ts TAGS (code " + declaredType + ") FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET ts = 1000, code = " + stored + ", value = 1.0"));
    return type;
  }

  private void compact(final String type) {
    try {
      ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine().compactAll();
    } catch (final Exception e) {
      throw new AssertionError("compaction of " + type + " failed", e);
    }
  }

  private int count(final String selector) {
    return count(selector, false);
  }

  /** The series {@code selector} selects: as an instant vector, or through {@code count_over_time} over a range vector. */
  private int count(final String selector, final boolean range) {
    final PromQLExpr expr = new PromQLParser(range ? "count_over_time(" + selector + "[10s])" : selector).parse();
    final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(expr, EVAL_TIME_MS);
    assertThat(result).isInstanceOf(InstantVector.class);
    int samples = 0;
    for (final VectorSample ignored : ((InstantVector) result).samples())
      samples++;
    return samples;
  }
}
