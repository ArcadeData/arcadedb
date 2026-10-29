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
package com.arcadedb.function;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.agg.AggNth;
import com.arcadedb.function.agg.AggSlice;
import com.arcadedb.function.coll.CollFlatten;
import com.arcadedb.function.coll.CollInsert;
import com.arcadedb.function.coll.CollRemove;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.function.date.DateAdd;
import com.arcadedb.function.date.DateConvert;
import com.arcadedb.function.temporal.DateTimeFromEpochFunction;
import com.arcadedb.function.temporal.DateTimeFromEpochMillisFunction;
import com.arcadedb.function.text.SubstringFunction;
import com.arcadedb.function.text.TextCharAt;
import com.arcadedb.function.text.TextHexValue;
import com.arcadedb.function.text.TextIndexOf;
import com.arcadedb.function.text.TextLpad;
import com.arcadedb.function.text.TextRandom;
import com.arcadedb.function.text.TextRpad;
import com.arcadedb.function.util.UtilSleep;
import com.arcadedb.function.vector.VectorCreateFunction;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10850: the Cypher-style functions outside {@code function/sql/} read their INTEGER arguments through
 * {@code Number.intValue()} / {@code longValue()}, so a fraction truncated ({@code left('abc', 2.9)} read 2) and a value
 * past the int range wrapped to a different one ({@code 4294967297} read 1). A whole number is now read as written - kept
 * as a long where the function can answer for it, refused where it has to fit an int - and anything else is a client
 * error naming the function and the argument.
 */
class Issue10850WholeNumberArgumentsTest extends TestHelper {

  /** 2^32 + 1: {@code intValue()} wrapped it to 1, an in-range value the caller never wrote. */
  private static final long WRAPS_TO_ONE = 4_294_967_297L;

  // ---------------------------------------------------------------- left() / right() / substring(), through Cypher

  @Test
  void leftAndRightReadAWholeLengthAndRefuseAFraction() {
    assertThat(cypher("RETURN left('abcdef', 2.0) AS v")).isEqualTo("ab");
    assertThat(cypher("RETURN right('abcdef', 2.0) AS v")).isEqualTo("ef");

    assertThatThrownBy(() -> cypher("RETURN left('abc', 2.9) AS v")).hasStackTraceContaining("left() requires a whole number <length>");
    assertThatThrownBy(() -> cypher("RETURN right('abc', 2.9) AS v")).hasStackTraceContaining("right() requires a whole number <length>");
  }

  @Test
  void leftAndRightReadALengthPastTheIntRangeAsTheWholeString() {
    assertThat(cypher("RETURN left('abc', " + WRAPS_TO_ONE + ") AS v")).isEqualTo("abc");
    assertThat(cypher("RETURN right('abc', " + WRAPS_TO_ONE + ") AS v")).isEqualTo("abc");
    // 3000000000 used to wrap to a negative length and be refused as one
    assertThat(cypher("RETURN left('abc', 3000000000) AS v")).isEqualTo("abc");
  }

  @Test
  void substringReadsWholeBoundsAndRefusesFractions() {
    assertThat(cypher("RETURN substring('abcdef', 1.0, 2.0) AS v")).isEqualTo("bc");
    assertThat(cypher("RETURN substring('abcdef', 1, " + (WRAPS_TO_ONE + 1) + ") AS v")).isEqualTo("bcdef");
    assertThat(cypher("RETURN substring('abcdef', " + WRAPS_TO_ONE + ") AS v")).isEqualTo("");

    assertThatThrownBy(() -> cypher("RETURN substring('abcdef', 1.9) AS v"))
        .hasStackTraceContaining("substring() requires a whole number <start>");
    assertThatThrownBy(() -> cypher("RETURN substring('abcdef', 1, 2.5) AS v"))
        .hasStackTraceContaining("substring() requires a whole number <length>");
  }

  /** {@link SubstringFunction} is not what Cypher resolves substring() to, but it is the same reading. */
  @Test
  void theFunctionTreeSubstringReadsTheSameWay() {
    final SubstringFunction substring = new SubstringFunction();
    assertThat(substring.execute(new Object[] { "abcdef", 1L, WRAPS_TO_ONE + 1 }, null)).isEqualTo("bcdef");
    assertThat(substring.execute(new Object[] { "abcdef", WRAPS_TO_ONE }, null)).isEqualTo("");
    assertThatThrownBy(() -> substring.execute(new Object[] { "abcdef", 1.9 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<start>");
  }

  @Test
  void aNonNumberIsStillTheSameTypeError() {
    assertThatThrownBy(() -> cypher("RETURN left('abc', [1]) AS v")).hasStackTraceContaining("expects an INTEGER or a FLOAT");
  }

  // ---------------------------------------------------------------- round() precision

  @Test
  void roundReadsAWholePrecisionInTheIntRange() {
    assertThat(((Number) cypher("RETURN round(3.14159, 2.0) AS v")).doubleValue()).isEqualTo(3.14);

    assertThatThrownBy(() -> cypher("RETURN round(3.14159, 2.9) AS v")).hasStackTraceContaining("round() requires a whole number <precision>");
    // 4294967298 used to wrap to the scale 2
    assertThatThrownBy(() -> cypher("RETURN round(3.14159, 4294967298) AS v"))
        .hasStackTraceContaining("round() requires a whole number <precision> in the int range");
    assertThat(cypher("RETURN round(null, 2.9) AS v")).isNull();
  }

  // ---------------------------------------------------------------- vector create dimension

  @Test
  void vectorCreateComparesTheDimensionAsWritten() {
    final VectorCreateFunction vector = new VectorCreateFunction();
    assertThat((float[]) vector.execute(new Object[] { List.of(1, 2, 3), 3.0 }, null)).hasSize(3);

    assertThatThrownBy(() -> vector.execute(new Object[] { List.of(1, 2, 3), 3.9 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<dimension>");
    // 4294967299 used to wrap to 3 and match the vector
    assertThatThrownBy(() -> vector.execute(new Object[] { List.of(1, 2, 3), 4_294_967_299L }, null))
        .hasMessageContaining("dimension mismatch");
  }

  // ---------------------------------------------------------------- coll.insert / coll.remove / coll.flatten

  @Test
  void collInsertRefusesAFractionalOrWrappingIndex() {
    final CollInsert insert = new CollInsert();
    assertThat(insert.execute(new Object[] { List.of("a", "b"), 1.0, "x" }, null)).isEqualTo(List.of("a", "x", "b"));

    assertThatThrownBy(() -> insert.execute(new Object[] { List.of("a", "b"), 1.5, "x" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<index>");
    assertThatThrownBy(() -> insert.execute(new Object[] { List.of("a", "b"), WRAPS_TO_ONE, "x" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("is out of range");
  }

  @Test
  void collRemoveReadsWholeIndexAndCount() {
    final CollRemove remove = new CollRemove();
    assertThat(remove.execute(new Object[] { List.of("a", "b", "c"), 1.0 }, null)).isEqualTo(List.of("a", "c"));
    // a count past the int range removes the rest of the list
    assertThat(remove.execute(new Object[] { List.of("a", "b", "c"), 1L, 3_000_000_000L }, null)).isEqualTo(List.of("a"));

    assertThatThrownBy(() -> remove.execute(new Object[] { List.of("a", "b", "c"), 0.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<index>");
    // 4294967296 used to wrap to 0
    assertThatThrownBy(() -> remove.execute(new Object[] { List.of("a", "b", "c"), 4_294_967_296L }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("is out of range");
    assertThatThrownBy(() -> remove.execute(new Object[] { List.of("a", "b", "c"), 0L, 1.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<count>");
  }

  @Test
  void collFlattenReadsAWholeDepth() {
    final CollFlatten flatten = new CollFlatten();
    final List<Object> nested = List.of(1, List.of(2, List.of(3)));
    // depth 4294967297 used to wrap to 1 and flatten one level only
    assertThat(flatten.execute(new Object[] { nested, WRAPS_TO_ONE }, null)).isEqualTo(List.of(1, 2, 3));

    assertThatThrownBy(() -> flatten.execute(new Object[] { nested, 1.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<depth>");
  }

  // ---------------------------------------------------------------- agg.nth / agg.slice

  @Test
  void aggNthReadsAWholeIndex() {
    final AggNth nth = new AggNth();
    assertThat(nth.execute(new Object[] { List.of("a", "b"), 1.0 }, null)).isEqualTo("b");
    // used to wrap to 1 and answer "b"
    assertThat(nth.execute(new Object[] { List.of("a", "b"), WRAPS_TO_ONE }, null)).isNull();

    assertThatThrownBy(() -> nth.execute(new Object[] { List.of("a", "b"), 1.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<index>");
  }

  @Test
  void aggSliceReadsWholeBounds() {
    final AggSlice slice = new AggSlice();
    // to 4294967297 used to wrap to 1 and answer ["a"]
    assertThat(slice.execute(new Object[] { List.of("a", "b", "c"), 0L, WRAPS_TO_ONE }, null)).isEqualTo(List.of("a", "b", "c"));

    assertThatThrownBy(() -> slice.execute(new Object[] { List.of("a", "b", "c"), 0.5, 2L }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<from>");
    assertThatThrownBy(() -> slice.execute(new Object[] { List.of("a", "b", "c"), 0L, 2.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<to>");
  }

  // ---------------------------------------------------------------- path.slice, through Cypher

  @Test
  void pathSliceReadsWholeIndexes() {
    database.transaction(() -> database.command("opencypher", "CREATE (:N10850 {k: 0})-[:R]->(:N10850 {k: 1})-[:R]->(:N10850 {k: 2})"));
    final String match = "MATCH p = (a:N10850 {k: 0})-[:R*2]->(b:N10850) ";

    // to 4294967297 used to wrap to 1 and answer one relationship
    assertThat(cypher(match + "RETURN length(path.slice(p, 0, " + WRAPS_TO_ONE + ")) AS v")).isEqualTo(2L);
    assertThat(cypher(match + "RETURN length(path.slice(p, 1.0)) AS v")).isEqualTo(1L);

    assertThatThrownBy(() -> cypher(match + "RETURN path.slice(p, 0.5) AS v")).hasStackTraceContaining("path.slice() requires a whole number <index>");
  }

  // ---------------------------------------------------------------- text.* helpers

  @Test
  void textFunctionsReadAWholeIntArgument() {
    assertThat(new TextLpad().execute(new Object[] { "x", 3.0, "-" }, null)).isEqualTo("--x");

    assertThatThrownBy(() -> new TextLpad().execute(new Object[] { "x", 2.5, "-" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("text.lpad() requires a whole number <length>");
    assertThatThrownBy(() -> new TextRpad().execute(new Object[] { "x", 2.5, "-" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<length>");
    assertThatThrownBy(() -> new TextCharAt().execute(new Object[] { "abc", 1.9 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<index>");
    assertThatThrownBy(() -> new TextIndexOf().execute(new Object[] { "abcabc", "a", 1.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<start>");
    assertThatThrownBy(() -> new TextRandom().execute(new Object[] { 2.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<length>");
    // 4294967297 used to wrap to the index 1
    assertThatThrownBy(() -> new TextCharAt().execute(new Object[] { "abc", WRAPS_TO_ONE }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("in the int range");
  }

  @Test
  void textHexValueRefusesAFraction() {
    assertThat(new TextHexValue().execute(new Object[] { 255.0 }, null)).isEqualTo("ff");
    assertThatThrownBy(() -> new TextHexValue().execute(new Object[] { 255.9 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<value>");
  }

  // ---------------------------------------------------------------- long-typed arguments of the Cypher-style family

  @Test
  void epochConstructorsReadWholeNumbersAndPropagateNull() {
    final DateTimeFromEpochMillisFunction fromMillis = new DateTimeFromEpochMillisFunction();
    assertThat(fromMillis.execute(new Object[] { 1000.0 }, null)).isEqualTo(fromMillis.execute(new Object[] { 1000L }, null));
    assertThat(fromMillis.execute(new Object[] { null }, null)).isNull();
    assertThatThrownBy(() -> fromMillis.execute(new Object[] { 1.9 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<millis>");

    final DateTimeFromEpochFunction fromEpoch = new DateTimeFromEpochFunction();
    assertThat(fromEpoch.execute(new Object[] { null, 0L }, null)).isNull();
    assertThatThrownBy(() -> fromEpoch.execute(new Object[] { 1.5, 0L }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<seconds>");
    assertThatThrownBy(() -> fromEpoch.execute(new Object[] { 1L, 0.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<nanoseconds>");
  }

  @Test
  void dateFunctionsReadWholeAmounts() {
    // date.convert(1.5, 's', 'ms') used to answer 1000
    assertThat(new DateConvert().execute(new Object[] { 2.0, "s", "ms" }, null)).isEqualTo(2000L);
    assertThatThrownBy(() -> new DateConvert().execute(new Object[] { 1.5, "s", "ms" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<value>");

    assertThatThrownBy(() -> new DateAdd().execute(new Object[] { 0L, 1.5, "h" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<value>");
    // the timestamp goes through AbstractDateFunction.toMillis, shared by the whole date.* family
    assertThatThrownBy(() -> new DateAdd().execute(new Object[] { 1.5, 1L, "ms" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<timestamp>");
  }

  @Test
  void utilSleepRefusesAFraction() {
    assertThatThrownBy(() -> new UtilSleep().execute(new Object[] { 1.5 }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<milliseconds>");
  }

  @Test
  void rangeRefusesAFractionalBigDecimal() {
    // FLOAT was already refused by type; a BigDecimal parameter is not a FLOAT and longValue() truncated it
    assertThat(cypher("RETURN size(range($start, 3)) AS v", Map.of("start", new BigDecimal("1.0")))).isEqualTo(3L);
    assertThatThrownBy(() -> cypher("RETURN range($start, 3) AS v", Map.of("start", new BigDecimal("1.5"))))
        .hasStackTraceContaining("range() requires a whole number <start>");
  }

  @Test
  void dateMapDayOfWeekRefusesAFraction() {
    final LocalDate monday = LocalDate.of(2024, 1, 1);
    assertThat(CypherFunctionHelper.applyDateMap(monday, Map.of("dayOfWeek", 3.0))).isEqualTo(LocalDate.of(2024, 1, 3));
    assertThatThrownBy(() -> CypherFunctionHelper.applyDateMap(monday, Map.of("dayOfWeek", 2.5)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dayOfWeek");
  }

  // ---------------------------------------------------------------- SQL long-typed arguments named by the issue

  @Test
  void sqlDurationReadsAWholeAmount() {
    assertThat(sql("SELECT duration(2.0, 'hours') AS v").toString()).isEqualTo("PT2H");
    // duration(2.5, 'hours') used to be 2 hours
    assertThatThrownBy(() -> sql("SELECT duration(2.5, 'hours') AS v")).hasStackTraceContaining("duration() requires a whole number <amount>");
  }

  @Test
  void sqlDateReadsWholeMillis() {
    assertThat(sql("SELECT date(1000.0) AS v")).isEqualTo(sql("SELECT date(1000) AS v"));
    assertThatThrownBy(() -> sql("SELECT date(1.9) AS v")).hasStackTraceContaining("date() requires a whole number <millis>");
  }

  @Test
  void sqlPromQLReadsAWholeEvaluationTime() {
    assertThatThrownBy(() -> sql("SELECT promql('up', 1.5) AS v")).hasStackTraceContaining("promql() requires a whole number <evalTimeMs>");
  }

  @Test
  void sqlTimeBucketReadsAWholeOrigin() {
    assertThatThrownBy(() -> sql("SELECT ts.timeBucket('1h', 1000, 1.5) AS v"))
        .hasStackTraceContaining("ts.timeBucket() requires a whole number <origin>");
    assertThatThrownBy(() -> sql("SELECT ts.timeBucket('1h', 1000, {offset: 1.5}) AS v"))
        .hasStackTraceContaining("ts.timeBucket() requires a whole number <offset>");
  }

  @Test
  void sqlCypherRidReadsAWholeId() {
    assertThatThrownBy(() -> sql("SELECT cypherRID(1.9) AS v")).hasStackTraceContaining("cypherrid() requires a whole number <cypherId>");
  }

  private Object cypher(final String query) {
    return cypher(query, Map.of());
  }

  private Object cypher(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      final Result row = rs.next();
      return row.getProperty("v");
    }
  }

  private Object sql(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().getProperty("v");
    }
  }
}
