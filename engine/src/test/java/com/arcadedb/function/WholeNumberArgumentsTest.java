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
import com.arcadedb.utility.NumberUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the whole-number arguments of the functions: a fraction or an out of range value must be refused, never
 * silently truncated or wrapped.
 */
class WholeNumberArgumentsTest {

  /**
   * Issue #10850: the Cypher-style functions outside {@code function/sql/} read their INTEGER arguments through
   * {@code Number.intValue()} / {@code longValue()}, so a fraction truncated ({@code left('abc', 2.9)} read 2) and a value
   * past the int range wrapped to a different one ({@code 4294967297} read 1). A whole number is now read as written - kept
   * as a long where the function can answer for it, refused where it has to fit an int - and anything else is a client
   * error naming the function and the argument.
   */
  @Nested
  class WholeNumberArguments extends TestHelper {

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

    /** The null check moved from the number read to the callers: a null argument still answers as it did. */
    @Test
    void aNullArgumentStillPropagatesOrDefaultsAsBefore() {
      final List<Object> list = List.of("a", "b", "c");
      assertThat(new CollInsert().execute(new Object[] { list, null, "x" }, null)).isNull();
      assertThat(new CollRemove().execute(new Object[] { list, null }, null)).isNull();
      assertThat(new CollRemove().execute(new Object[] { list, 0L, null }, null)).isNull();
      assertThat(new CollFlatten().execute(new Object[] { list, null }, null)).isNull();
      assertThat(new AggNth().execute(new Object[] { list, null }, null)).isNull();
      // agg.slice: a null bound is the default bound, not a null answer
      assertThat(new AggSlice().execute(new Object[] { list, null, 2L }, null)).isEqualTo(List.of("a", "b"));
      assertThat(new AggSlice().execute(new Object[] { list, 1L, null }, null)).isEqualTo(List.of("b", "c"));
      assertThat(new VectorCreateFunction().execute(new Object[] { List.of(1, 2), null }, null)).isNull();
      assertThat(new TextLpad().execute(new Object[] { "x", null, "-" }, null)).isEqualTo("x");
      assertThat(cypher("RETURN left('abc', null) AS v")).isNull();
      assertThat(cypher("RETURN substring('abc', null) AS v")).isNull();
      assertThat(cypher("RETURN substring('abc', 1, null) AS v")).isNull();
      assertThat(cypher("RETURN round(1.5, null) AS v")).isNull();
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

  /**
   * Issue #10954: the TEXT branch of the integer arguments of date.add / date.convert / util.sleep / text.* went through
   * Long.parseLong / Integer.parseInt, so a text that is not a whole number answered a raw NumberFormatException (an internal
   * error) instead of the client error naming the function and the argument that the number branch gives.
   */
  @Nested
  class TextWholeNumberArguments {

    @Test
    void dateConvertRefusesANonIntegerText() {
      final DateConvert convert = new DateConvert();
      assertThat(convert.execute(new Object[] { "2", "s", "ms" }, null)).isEqualTo(2000L);
      assertThatThrownBy(() -> convert.execute(new Object[] { "1.5", "s", "ms" }, null))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessage("date.convert() requires a whole number <value>, but received '1.5'");
      assertThatThrownBy(() -> convert.execute(new Object[] { "abc", "s", "ms" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<value>");
    }

    @Test
    void dateAddRefusesANonIntegerText() {
      final DateAdd add = new DateAdd();
      assertThat(add.execute(new Object[] { 1000L, "2", "ms" }, null)).isEqualTo(1002L);
      assertThatThrownBy(() -> add.execute(new Object[] { 1000L, "1.5", "h" }, null))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessage("date.add() requires a whole number <value>, but received '1.5'");
    }

    @Test
    void utilSleepRefusesANonIntegerText() {
      assertThat(new UtilSleep().execute(new Object[] { "0" }, null)).isNull();
      assertThatThrownBy(() -> new UtilSleep().execute(new Object[] { "1.5" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<milliseconds>");
    }

    @Test
    void textFunctionsRefuseANonIntegerText() {
      assertThat(new TextLpad().execute(new Object[] { "x", "3", "-" }, null)).isEqualTo("--x");
      assertThatThrownBy(() -> new TextLpad().execute(new Object[] { "x", "2.5", "-" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("text.lpad() requires a whole number <length>");
      assertThatThrownBy(() -> new TextRpad().execute(new Object[] { "x", "abc", "-" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<length>");
      assertThatThrownBy(() -> new TextCharAt().execute(new Object[] { "abc", "1.5" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<index>");
      assertThatThrownBy(() -> new TextIndexOf().execute(new Object[] { "abc", "a", "1.5" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<start>");
      assertThatThrownBy(() -> new TextRandom().execute(new Object[] { "1.5" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<length>");
      assertThatThrownBy(() -> new TextCharAt().execute(new Object[] { "abc", "4294967297" }, null))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("in the int range");
    }
  }

  /**
   * The census of the narrowing conversions on the path a function argument takes to become an int or a long: a fraction that
   * truncates ({@code 2.5} read as 2) and a value that wraps ({@code 3000000000} read as a negative int) are a wrong argument
   * answered as a plausible one.
   * <ul>
   * <li>Issue #10699: #10611 converted the integer arguments of the five vector functions it enumerated, and its test enumerated the
   * same five, so it could not find {@code vector.sparseCreate}, {@code randomInt} and {@code if}. An enumeration of the converted
   * sites cannot find the unconverted ones; a scan can.</li>
   * <li>Issue #10850: the first scan covered {@code function/sql/**} only, so the Cypher-style functions next to it ({@code left},
   * {@code substring}, {@code coll.*}, {@code duration(2.5, 'hours')}) kept reading their arguments through
   * {@code intValue()} / {@code longValue()}.</li>
   * <li>Issue #10931: a scan of source SPELLINGS ({@code .intValue()}, {@code Integer.parseInt(}) is passed by
   * {@code (int) ((Number) arg).longValue()}, a compound assignment, a method reference or a static import. The set of spellings is
   * open; the set of things the JVM can do to narrow a value is closed. So the scan reads the COMPILED classes and records every
   * narrowing instruction ({@code l2i}, {@code d2i}, {@code f2i}, {@code d2l}, {@code f2l}, {@code i2s}, {@code i2b},
   * {@code i2c}) and every call or method reference of the JDK methods that narrow, whatever the source looked like.</li>
   * <li>Issue #11022: the two scans ranged over two directory trees, and neither over the code that performs the check:
   * {@code NumberUtils.toLongWhole} / {@code toIntWhole}, the authority every strict helper delegates to. A narrowing put back
   * there left both green. Narrowing is a property of a data path, not of a directory, so the census now ranges over the path:
   * every function (SQL and Cypher, {@code function/**}), the Cypher function implementations kept in the executor, and
   * {@code NumberUtils}; and it asserts that every strict helper still reaches {@code NumberUtils}. The two scans were one
   * invariant over overlapping trees, with two reviewed lists to drift apart, and are now this one.</li>
   * </ul>
   * Every occurrence is keyed by its class, its method and the narrowing it performs, and is an offender unless {@link #REVIEWED}
   * lists that key with the number of times it occurs and the reason none is a narrowing read of an argument. A count, rather than a
   * key, cannot see one narrowing replaced by another in the same method ({@code Math.toIntExact} by {@code Number.intValue} in
   * {@code toIntWhole} is one occurrence either way), which is the shape of the #11022 mutation. What a census cannot see at all is a
   * check that is deleted around a narrowing it keeps (the whole-number test of {@code toLongWhole}): {@code NumberUtilsTest} pins
   * the behaviour of the authority itself.
   */
  @Nested
  class NoNarrowingReads {
    private static final String FUNCTIONS        = "com/arcadedb/function";
    private static final String CYPHER_FUNCTIONS = "com/arcadedb/query/opencypher/executor";
    private static final String NUMBER_UTILS     = "com/arcadedb/utility/NumberUtils";

    private static final Set<String> NARROWING_INT_METHODS = Set.of("intValue", "shortValue", "byteValue");
    /** Unboxing loses nothing: {@code Integer.intValue()}, {@code Short.shortValue()}, {@code Byte.byteValue()}. */
    private static final Set<String> LOSSLESS_INT          = Set.of("java/lang/Integer.intValue", "java/lang/Short.shortValue",
        "java/lang/Byte.byteValue", "java/util/concurrent/atomic/AtomicInteger.intValue");
    /** The owners whose {@code longValue()} widens or unboxes: every other owner ({@code Number}, {@code Double}, ...) narrows. */
    private static final Set<String> LOSSLESS_LONG_OWNERS  = Set.of("java/lang/Long", "java/lang/Integer", "java/lang/Short",
        "java/lang/Byte", "java/util/concurrent/atomic/AtomicLong", "java/util/concurrent/atomic/AtomicInteger");
    private static final Set<String> PARSING_OWNERS        = Set.of("java/lang/Integer", "java/lang/Short", "java/lang/Byte");
    private static final Set<String> PARSING_METHODS       = Set.of("parseInt", "parseShort", "parseByte", "valueOf", "decode",
        "parseUnsignedInt");

    /**
     * The strict helpers an argument is read through, each of which must reach {@code NumberUtils.toLongWhole} or
     * {@code toIntWhole}: a helper that stops delegating stops being strict whatever its own instructions are.
     */
    private static final Map<String, List<String>> STRICT_HELPERS = Map.of(//
        "com/arcadedb/function/sql/SQLFunctionAbstract", List.of("requireWholeIntArgument", "requireWholeLongArgument"),
        "com/arcadedb/function/cypher/CypherFunctionHelper", List.of("requireWholeNumberArgument", "requireWholeIntArgument",
            "requireWholeNumberOrTextArgument", "requireWholeIntOrTextArgument"));

    /**
     * Relative to {@code com/arcadedb}, as {@code class#method: narrowing}, lambdas and nested classes counted for their outer class
     * and method: the reviewed occurrences, and why none of them is a narrowing read of an argument.
     */
    private static final Map<String, Integer> REVIEWED = new TreeMap<>(Map.ofEntries(//
        // the strict helper itself: parses a TEXT argument, and Integer.parseInt refuses what does not fit
        Map.entry("function/sql/SQLFunctionAbstract#requireWholeIntArgument: java/lang/Integer.parseInt", 1),
        // the vector family's strict helpers: parseInteger (#10611, same shape) and toInt8Code, a (byte) after a range check
        Map.entry("function/sql/vector/SQLFunctionVectorAbstract#parseInteger: java/lang/Integer.parseInt", 1),
        Map.entry("function/sql/vector/SQLFunctionVectorAbstract#toInt8Code: i2b", 1),
        Map.entry("function/sql/vector/SQLFunctionVectorAbstract#toInt8Code: l2i", 1),
        // the shared option-map reader: a text option parsed by Integer.parseInt, which refuses rather than truncates, and the sign check of a value getInt/getLong already read strictly
        Map.entry("function/sql/FunctionOptions#getInt: java/lang/Integer.parseInt", 1),
        Map.entry("function/sql/FunctionOptions#requireNonNegative: java/lang/Number.longValue", 1),
        // the Cypher strict helpers: (int) of a long tested to fit first; and a StringBuilder capacity capped by Math.min
        Map.entry("function/cypher/CypherFunctionHelper#replaceLiteral: l2i", 1),
        Map.entry("function/cypher/CypherFunctionHelper#requireWholeIntArgument: l2i", 2),
        Map.entry("function/cypher/CypherFunctionHelper#requireWholeIntOrTextArgument: l2i", 2),
        // the authority (issue #11022): every narrowing is behind a range or a whole-number check, or is the saturation the method promises; NumberUtilsTest pins what each one answers
        Map.entry("utility/NumberUtils#parsePositiveInteger: java/lang/Integer.parseInt", 1),
        Map.entry("utility/NumberUtils#saturateToInt: java/lang/Number.longValue", 1),
        Map.entry("utility/NumberUtils#saturateToInt: java/math/BigDecimal.intValue", 1),
        Map.entry("utility/NumberUtils#saturateToInt: java/math/BigInteger.intValue", 1),
        Map.entry("utility/NumberUtils#saturateToInt: l2i", 1),
        Map.entry("utility/NumberUtils#saturateToIntOrNull: java/lang/Integer.parseInt", 1),
        Map.entry("utility/NumberUtils#toIntExact: java/lang/Math.toIntExact", 1),
        Map.entry("utility/NumberUtils#toIntExact: java/lang/Number.longValue", 1),
        Map.entry("utility/NumberUtils#toIntExact: java/math/BigDecimal.intValue", 1),
        Map.entry("utility/NumberUtils#toIntExact: java/math/BigInteger.intValue", 1),
        Map.entry("utility/NumberUtils#toIntWhole: java/lang/Math.toIntExact", 1),
        Map.entry("utility/NumberUtils#toLongWhole: d2l", 1),
        Map.entry("utility/NumberUtils#toLongWhole: java/lang/Number.longValue", 1),
        // narrow a COMPUTED result back to the type of the input (abs(short) after absExact), not an argument
        Map.entry("function/sql/math/SQLFunctionAbsoluteValue#execute: i2b", 1),
        Map.entry("function/sql/math/SQLFunctionAbsoluteValue#execute: i2s", 1),
        Map.entry("function/sql/math/SQLFunctionAbsoluteValue#execute: l2i", 3),
        // the exact power of an integral base answered as an INTEGER once it is known to fit (issue #11047)
        Map.entry("function/sql/math/SQLFunctionPow#integralPow: java/lang/Math.toIntExact", 1),
        // sum(): none reads an argument. addToInt: the overflow check of the running long ((int) longSum). get(): the INT total
        // narrowed back while it is known to fit (not widened), and a one-row total narrowed back to the Short or Byte that row added
        // (l2i+i2s, l2i+i2b: the total IS that value). addSmall: the short or byte parameter of addShort / addByte, widened to an int to
        // share the code, boxed back to its own type for Type.increment (i2s, i2b)
        Map.entry("function/sql/math/NumericSum#addToInt: l2i", 1),
        Map.entry("function/sql/math/NumericSum#addSmall: i2b", 1),
        Map.entry("function/sql/math/NumericSum#addSmall: i2s", 1),
        Map.entry("function/sql/math/NumericSum#get: l2i", 3),
        Map.entry("function/sql/math/NumericSum#get: i2b", 1),
        Map.entry("function/sql/math/NumericSum#get: i2s", 1),
        // round(value) IS the rounding the function is asked for
        Map.entry("function/math/RoundFunction#execute: java/lang/Math.round", 1),
        // a list index computed from the size of the collected values, never from an argument
        Map.entry("function/sql/math/SQLFunctionPercentile#evaluate: d2i", 2),
        Map.entry("function/sql/time/SQLFunctionTsPercentile#getResult: d2i", 2),
        Map.entry("function/sql/time/SQLFunctionLead#getResult: l2i", 1),
        Map.entry("function/agg/AggPercentiles#execute: d2i", 2),
        Map.entry("function/agg/PercentileContFunction#getAggregatedResult: d2i", 2),
        Map.entry("function/agg/AggStatistics#execute: l2i", 3),
        // a list index computed from the size of the collected values and clamped to it; and a value known to be Long/Integer
        Map.entry("function/agg/PercentileDiscFunction#getAggregatedResult: d2i", 1),
        Map.entry("function/agg/PercentileDiscFunction#getAggregatedResult: java/lang/Number.longValue", 1),
        // an index or a bound read strictly as a long, tested against or clamped to the size of the list before the cast
        Map.entry("function/agg/AggNth#execute: l2i", 1),
        Map.entry("function/agg/AggSlice#execute: l2i", 2),
        Map.entry("function/coll/CollInsert#execute: l2i", 1),
        Map.entry("function/coll/CollRemove#execute: l2i", 1),
        Map.entry("function/path/PathSlice#execute: l2i", 2),
        // the cardinality, refused above Integer.MAX_VALUE before the cast
        Map.entry("function/coll/RangeFunction#execute: l2i", 1),
        // a length or a start read strictly as a long, clamped to the int range a string fits in (past it is past the end)
        Map.entry("function/text/LeftFunction#execute: l2i", 1),
        Map.entry("function/text/RightFunction#execute: l2i", 1),
        Map.entry("function/text/SubstringFunction#startCodePoint: l2i", 1),
        Map.entry("query/opencypher/executor/CypherSubstringFunction#startCodePoint: l2i", 1),
        // a capacity or a bound already capped below Integer.MAX_VALUE by Math.min
        Map.entry("function/sql/time/SQLFunctionMovingAvg#execute: l2i", 1),
        Map.entry("function/sql/graph/SQLFunctionAstar#internalExecute: l2i", 1),
        Map.entry("function/sql/vector/SQLFunctionVectorDiscover#execute: l2i", 1),
        // a float score of the index turned into the int score of the cursor entry
        Map.entry("function/sql/text/SQLFunctionSearchFieldsMore#execute: f2i", 1),
        Map.entry("function/sql/text/SQLFunctionSearchIndexMore#execute: f2i", 1),
        // produce quantized codes from values the function computed itself (bits packed into bytes, round() in [0, 255])
        Map.entry("function/sql/vector/SQLFunctionVectorQuantizeBinary#execute: i2b", 1),
        Map.entry("function/sql/vector/SQLFunctionVectorQuantizeInt8#execute: i2b", 1),
        Map.entry("function/sql/vector/SQLFunctionVectorQuantizeInt8#execute: java/lang/Math.round", 1),
        Map.entry("function/sql/vector/SQLFunctionVectorQuantizeInt8#execute: l2i", 1),
        // a coordinate tested to be a whole number inside the range of its type before the cast
        Map.entry("function/vector/VectorCreateFunction#toBytes: i2b", 1),
        Map.entry("function/vector/VectorCreateFunction#toBytes: l2i", 1),
        Map.entry("function/vector/VectorCreateFunction#toInts: l2i", 1),
        Map.entry("function/vector/VectorCreateFunction#toShorts: i2s", 1),
        Map.entry("function/vector/VectorCreateFunction#toShorts: l2i", 1),
        Map.entry("function/vector/VectorCreateFunction#wholeCoordinate: d2l", 1),
        // the srid, tested to be a whole number in [0, Integer.MAX_VALUE] before the cast
        Map.entry("function/geo/CypherPointFunction#execute: d2i", 1),
        // hashCode() of a double pair
        Map.entry("function/sql/geo/LightweightPoint#hashCode: l2i", 2),
        // the bucket id unpacked from the high bits of an encoded id, not a read of a number
        Map.entry("function/graph/IdFunction#decodeLongToRid: l2i", 1),
        // the canonical key of a value: a double tested to be an exact integer, and a value known to be Long/Integer/Short/Byte
        Map.entry("function/DistinctNumericKey#canonicalDouble: d2l", 1),
        Map.entry("function/DistinctNumericKey#canonicalize: java/lang/Number.longValue", 1),
        // a value already known to be Long/Integer/Short/Byte: widening, nothing to lose
        Map.entry("function/math/AbsFunction#execute: java/lang/Number.longValue", 1),
        Map.entry("function/number/NumberFormat#execute: java/lang/Number.longValue", 1),
        // toInteger()/convert.toInteger(): truncating a FLOAT IS the conversion (toInteger(2.9) is 2 in Neo4j too)
        Map.entry("function/convert/ToIntegerFunction#execute: d2l", 1),
        Map.entry("function/convert/ToIntegerFunction#execute: java/lang/Number.longValue", 1),
        Map.entry("function/convert/ConvertToInteger#execute: d2l", 1),
        Map.entry("function/convert/ConvertToInteger#execute: java/lang/Number.longValue", 1),
        // toBoolean(): FLOAT and DOUBLE are refused before it, the integral value is tested against zero
        Map.entry("function/convert/ToBooleanFunction#execute: java/lang/Number.longValue", 1),
        // facet.searchValues(): the sign check of the limit only, which is then read through requireIntArgument
        Map.entry("function/sql/facet/SQLFunctionFacetSearchValues#execute: java/lang/Number.longValue", 1),
        // year/month/day of a temporal value the engine built: components a LocalDate already holds as ints
        Map.entry("function/temporal/DateConstructorFunction#execute: java/lang/Number.intValue", 3)));

    @Test
    void noFunctionReadsAnArgumentThroughANarrowingConversion() throws Exception {
      final Path classes = classesRoot();
      final List<Path> files = new ArrayList<>(classFiles(classes.resolve(FUNCTIONS)));
      // only the function implementations of the Cypher executor, not the whole executor
      files.addAll(classFiles(classes.resolve(CYPHER_FUNCTIONS)).stream()
          .filter(p -> outerClassOf(classes.relativize(p).toString()).matches(".*/Cypher\\w+Function")).toList());
      // the authority: the strict helpers are all built on it (issue #11022)
      files.addAll(classFiles(classes.resolve(NUMBER_UTILS).getParent()).stream()
          .filter(p -> outerClassOf(classes.relativize(p).toString()).equals(NUMBER_UTILS)).toList());

      final Map<String, Integer> found = new TreeMap<>();
      for (final Path file : files) {
        final String outerClass = outerClassOf(classes.relativize(file).toString()).substring("com/arcadedb/".length());
        for (final String occurrence : narrowingsIn(file))
          found.merge(outerClass + "#" + occurrence, 1, Integer::sum);
      }

      assertThat(files.size()).as("no compiled function found under " + classes.toAbsolutePath()).isGreaterThan(400);
      assertThat(files).as("the scan no longer reaches CypherSubstringFunction, what Cypher resolves substring() to")
          .anyMatch(p -> p.endsWith("CypherSubstringFunction.class"));
      assertThat(files).as("the scan no longer reaches NumberUtils, the authority of the strict helpers")
          .anyMatch(p -> p.endsWith("NumberUtils.class"));

      final List<String> offenders = new ArrayList<>();
      for (final Map.Entry<String, Integer> entry : found.entrySet())
        if (!REVIEWED.containsKey(entry.getKey()))
          offenders.add(entry.getKey() + " x" + entry.getValue());
      assertThat(offenders)
          .as("narrowing conversions that may read a function argument, which truncates 2.5 to 2 and wraps 3000000000 to a negative "
              + "int (issues #10699, #10850, #10931, #11022): read the argument through SQLFunctionAbstract.requireWholeIntArgument / "
              + "requireWholeLongArgument / requireIntArgument or CypherFunctionHelper.requireWholeNumberArgument / "
              + "requireWholeIntArgument instead, or review the occurrence and add it to REVIEWED")
          .isEmpty();

      final List<String> changed = new ArrayList<>();
      for (final Map.Entry<String, Integer> entry : REVIEWED.entrySet())
        if (!entry.getValue().equals(found.get(entry.getKey())))
          changed.add(entry.getKey() + " (reviewed " + entry.getValue() + ", found " + found.get(entry.getKey()) + ")");
      assertThat(changed)
          .as("the reviewed occurrences changed: a new narrowing in a reviewed method, or one removed (review it, then update REVIEWED)")
          .isEmpty();
    }

    /** Each strict helper reaches the authority, directly or through another method of its class (issue #11022). */
    @Test
    void everyStrictHelperDelegatesToNumberUtils() throws Exception {
      final Path classes = classesRoot();
      final String authority = NumberUtils.class.getName().replace('.', '/');
      for (final Map.Entry<String, List<String>> helper : STRICT_HELPERS.entrySet()) {
        final Map<String, Set<String>> calls = callsByMethod(classes.resolve(helper.getKey() + ".class"));
        for (final String method : helper.getValue()) {
          assertThat(calls).as(helper.getKey() + " no longer has " + method).containsKey(method);
          assertThat(reaches(calls, helper.getKey(), method, authority, new HashSet<>()))
              .as(helper.getKey() + "." + method + " no longer reaches NumberUtils.toLongWhole/toIntWhole: it reads its argument some "
                  + "other way, which nothing proves strict")
              .isTrue();
        }
      }
    }

    private static boolean reaches(final Map<String, Set<String>> calls, final String owner, final String method, final String authority,
        final Set<String> visited) {
      if (!visited.add(method))
        return false;
      for (final String call : calls.getOrDefault(method, Set.of())) {
        if (call.equals(authority + ".toLongWhole") || call.equals(authority + ".toIntWhole"))
          return true;
        if (call.startsWith(owner + ".") && reaches(calls, owner, call.substring(owner.length() + 1), authority, visited))
          return true;
      }
      return false;
    }

    /** {@code a/b/C$1.class} -> {@code a/b/C}: lambdas and nested classes count for their file. */
    private static String outerClassOf(final String relative) {
      final String name = relative.replace('\\', '/');
      final String withoutExtension = name.endsWith(".class") ? name.substring(0, name.length() - ".class".length()) : name;
      final int nested = withoutExtension.indexOf('$');
      return nested < 0 ? withoutExtension : withoutExtension.substring(0, nested);
    }

    /** {@code lambda$execute$3} -> {@code execute}: a lambda counts for the method it is written in, whatever its number. */
    private static String methodOf(final String methodName) {
      if (methodName.startsWith("lambda$")) {
        final int end = methodName.lastIndexOf('$');
        return end > "lambda$".length() ? methodName.substring("lambda$".length(), end) : methodName;
      }
      return methodName;
    }

    private static Path classesRoot() throws URISyntaxException {
      return Path.of(NumberUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    private static List<Path> classFiles(final Path root) throws IOException {
      assertThat(root).as("compiled classes").isDirectory();
      try (final Stream<Path> files = Files.walk(root)) {
        return files.filter(p -> p.toString().endsWith(".class")).sorted().toList();
      }
    }

    /** Every narrowing instruction and narrowing JDK call of a class file, as {@code method: what}. */
    private static List<String> narrowingsIn(final Path classFile) throws IOException {
      final List<String> result = new ArrayList<>();
      try (final InputStream in = Files.newInputStream(classFile)) {
        new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9) {
          @Override
          public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
              final String[] exceptions) {
            final String method = methodOf(methodName);
            return new MethodVisitor(Opcodes.ASM9) {
              @Override
              public void visitInsn(final int opcode) {
                switch (opcode) {
                case Opcodes.L2I -> result.add(method + ": l2i");
                case Opcodes.D2I -> result.add(method + ": d2i");
                case Opcodes.F2I -> result.add(method + ": f2i");
                case Opcodes.D2L -> result.add(method + ": d2l");
                case Opcodes.F2L -> result.add(method + ": f2l");
                case Opcodes.I2S -> result.add(method + ": i2s");
                case Opcodes.I2B -> result.add(method + ": i2b");
                case Opcodes.I2C -> result.add(method + ": i2c");
                default -> {
                }
                }
              }

              @Override
              public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                  final boolean isInterface) {
                if (isNarrowingCall(owner, name, desc))
                  result.add(method + ": " + owner + "." + name);
              }

              @Override
              public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
                  final Object... bootstrapArguments) {
                // A METHOD REFERENCE (Number::intValue) IS A HANDLE PASSED TO THE LAMBDA BOOTSTRAP, NOT A CALL
                for (final Object argument : bootstrapArguments)
                  if (argument instanceof Handle handle && isNarrowingCall(handle.getOwner(), handle.getName(), handle.getDesc()))
                    result.add(method + ": " + handle.getOwner() + "." + handle.getName());
              }
            };
          }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      }
      return result;
    }

    /** The calls of each method of a class file, as {@code owner.name}. */
    private static Map<String, Set<String>> callsByMethod(final Path classFile) throws IOException {
      final Map<String, Set<String>> result = new HashMap<>();
      try (final InputStream in = Files.newInputStream(classFile)) {
        new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9) {
          @Override
          public MethodVisitor visitMethod(final int access, final String methodName, final String descriptor, final String signature,
              final String[] exceptions) {
            final Set<String> calls = result.computeIfAbsent(methodName, k -> new HashSet<>());
            return new MethodVisitor(Opcodes.ASM9) {
              @Override
              public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
                  final boolean isInterface) {
                calls.add(owner + "." + name);
              }
            };
          }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      }
      return result;
    }

    private static boolean isNarrowingCall(final String owner, final String name, final String desc) {
      if (NARROWING_INT_METHODS.contains(name) && desc.startsWith("()"))
        return !LOSSLESS_INT.contains(owner + "." + name);
      if ("longValue".equals(name) && "()J".equals(desc))
        return !LOSSLESS_LONG_OWNERS.contains(owner);
      if (PARSING_OWNERS.contains(owner) && PARSING_METHODS.contains(name) && desc.startsWith("(Ljava/lang/String;"))
        return true;
      return "java/lang/Math".equals(owner) && ("toIntExact".equals(name) || "round".equals(name));
    }
  }
}
