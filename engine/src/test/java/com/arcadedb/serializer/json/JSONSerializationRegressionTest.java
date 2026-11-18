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
package com.arcadedb.serializer.json;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.utility.DateUtils;
import com.google.gson.internal.LazilyParsedNumber;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the JSON serialization classes, one nested class per reported issue.
 */
class JSONSerializationRegressionTest {

  /**
   * Regression test for issue #4148: HTTP parameter binding loses int64 precision for ARRAY_OF_LONGS.
   * <p>
   * Before the fix, {@link JSONArray#toPrimitiveNumericArrayOrNull()} returned a {@code float[]} for
   * every homogeneous numeric JSON array. For int64 values larger than 2^24, that narrowing collapsed
   * distinct longs onto identical float bits (e.g. both {@code 1000000000000} and
   * {@code 1000000000001} mapped to the same float, then back to {@code 999999995904}).
   * After the fix, integer-only arrays are returned as {@code long[]} so the round trip preserves
   * every bit, while fractional / scientific arrays continue to take the {@code double[]} fast path
   * used by vector embeddings.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ArrayOfLongsParamPrecision {

    @Test
    void integerOnlyArrayPreservesInt64Precision() {
      // Two consecutive int64 values that differ by 1 must come back distinct.
      final JSONObject obj = new JSONObject("{\"arr\":[1000000000000, 1000000000001, 9999999999999]}");
      final Map<String, Object> map = obj.toMap(true);

      assertThat(map.get("arr")).isInstanceOf(long[].class);
      final long[] arr = (long[]) map.get("arr");
      assertThat(arr).containsExactly(1000000000000L, 1000000000001L, 9999999999999L);
    }

    @Test
    void boundaryInt64ValuesPreserved() {
      final JSONObject obj = new JSONObject("{\"arr\":[" + Long.MAX_VALUE + "," + Long.MIN_VALUE + ",0,-1,1]}");
      final Map<String, Object> map = obj.toMap(true);

      assertThat(map.get("arr")).isInstanceOf(long[].class);
      assertThat((long[]) map.get("arr")).containsExactly(Long.MAX_VALUE, Long.MIN_VALUE, 0L, -1L, 1L);
    }

    @Test
    void fractionalArrayKeepsDoubleFastPath() {
      // Vector embeddings: fractional values must continue to take the double[] fast path so we
      // do not regress the issue #3864 follow-up optimization.
      final JSONObject obj = new JSONObject("{\"vector\":[1.5, 2.5, 3.5]}");
      final Map<String, Object> map = obj.toMap(true);

      assertThat(map.get("vector")).isInstanceOf(double[].class);
      assertThat((double[]) map.get("vector")).containsExactly(1.5, 2.5, 3.5);
    }

    @Test
    void scientificNotationKeepsDoubleFastPath() {
      // 1e3 is integer-valued numerically but the textual form has 'e', so we treat it as float.
      // This matches what JSON callers expect: an explicit decimal/exponent means floating point.
      final JSONObject obj = new JSONObject("{\"v\":[1e3, 2e-2]}");
      final Map<String, Object> map = obj.toMap(true);

      assertThat(map.get("v")).isInstanceOf(double[].class);
      assertThat((double[]) map.get("v")).containsExactly(1000.0, 0.02);
    }

    @Test
    void mixedDecimalAndIntegerInSameArrayStaysAsList() {
      // A primitive array would turn the integers into floating point values (issue #9003): the list keeps each element as written
      final JSONObject obj = new JSONObject("{\"v\":[1, 2.5, 3]}");
      final Map<String, Object> map = obj.toMap(true);

      assertThat(map.get("v")).isInstanceOf(List.class);
      assertThat(map.get("v")).isEqualTo(List.of(1, 2.5, 3));
    }

    @Test
    void smallIntegerArrayPreservedAsLongArray() {
      // Ordinary integer arrays (e.g. ids) should also come back as long[] now.
      final JSONObject obj = new JSONObject("{\"ids\":[1, 2, 3, 4]}");
      final Map<String, Object> map = obj.toMap(true);

      assertThat(map.get("ids")).isInstanceOf(long[].class);
      assertThat((long[]) map.get("ids")).containsExactly(1L, 2L, 3L, 4L);
    }
  }

  /**
   * Regression test for issues #9004 (an integer beyond the long range wrapped around to its low 64 bits, a decimal with more digits
   * than a double holds was narrowed to a double), #9003 (a numeric array was narrowed to float32) and #9002 (a primitive array written
   * to a LIST property became its one element).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class JsonNumbersBeyondLongAndDouble extends TestHelper {

    @Test
    void integerBeyondLongIsKeptExact() {
      final Map<String, Object> map = new JSONObject("{\"a\":18446744073709551617,\"b\":123456789012345678901234567890,\"c\":-9223372036854775809}").toMap();
      assertThat(map.get("a")).isEqualTo(new BigDecimal("18446744073709551617"));
      assertThat(map.get("b")).isEqualTo(new BigDecimal("123456789012345678901234567890"));
      assertThat(map.get("c")).isEqualTo(new BigDecimal("-9223372036854775809"));
    }

    @Test
    void integersInsideTheLongRangeKeepTheirType() {
      final Map<String, Object> map = new JSONObject("{\"i\":7,\"l\":9223372036854775807,\"m\":-9223372036854775808}").toMap();
      assertThat(map.get("i")).isEqualTo(7);
      assertThat(map.get("l")).isEqualTo(Long.MAX_VALUE);
      assertThat(map.get("m")).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void decimalWithMoreDigitsThanADoubleIsKeptExact() {
      final Map<String, Object> map = new JSONObject("{\"x\":1.23456789012345678901234567890}").toMap();
      assertThat(map.get("x")).isEqualTo(new BigDecimal("1.23456789012345678901234567890"));
    }

    @Test
    void decimalAKnownDoubleWroteStaysADouble() {
      final Map<String, Object> map = new JSONObject("{\"a\":3.141592653589793,\"b\":0.30000000000000004,\"c\":1e300,\"d\":0.10000000000000000}").toMap();
      assertThat(map.get("a")).isEqualTo(3.141592653589793);
      assertThat(map.get("b")).isEqualTo(0.30000000000000004);
      assertThat(map.get("c")).isEqualTo(1e300);
      assertThat(map.get("d")).isEqualTo(0.1);
    }

    @Test
    void longDoubleTokensThatRoundTripStayPrimitive() {
      // 17 significant digits, as Double.toString writes them: no BigDecimal, the array keeps the primitive path
      final double a = 0.1234567890123456;
      final double b = 1.0 / 3;
      final Map<String, Object> map = new JSONObject("{\"v\":[" + a + "," + b + "]}").toMap(true);
      assertThat(map.get("v")).isInstanceOf(double[].class);
      assertThat((double[]) map.get("v")).containsExactly(a, b);
    }

    @Test
    void seventeenDigitProducersKeepThePrimitiveVectorPath() {
      // what printf("%.17g") writes: not the shortest rendering, still one double
      final Map<String, Object> map = new JSONObject("{\"v\":[0.10000000000000001,0.20000000000000001,0.30000000000000004]}").toMap(true);
      assertThat(map.get("v")).isInstanceOf(double[].class);
      assertThat((double[]) map.get("v")).containsExactly(0.1, 0.2, 0.30000000000000004);
      assertThat(new JSONObject("{\"x\":0.10000000000000001}").toMap().get("x")).isEqualTo(0.1);
    }

    @Test
    void tokenLengthBoundaryAndExponentOverflow() {
      // 15 characters: always a double. 16 characters that a double does not hold exactly: exact. 1e400 overflows a double: exact
      final Map<String, Object> map = new JSONObject("{\"a\":0.1234567890123,\"b\":0.12345678901234567,\"c\":0.1234567890123456789,\"d\":1e400}").toMap();
      assertThat(map.get("a")).isEqualTo(0.1234567890123);
      // 17 significant digits identify one double: no BigDecimal even when it is not the shortest rendering
      assertThat(map.get("b")).isEqualTo(0.12345678901234567);
      assertThat(map.get("c")).isEqualTo(new BigDecimal("0.1234567890123456789"));
      assertThat(map.get("d")).isEqualTo(new BigDecimal("1e400"));
      assertThat(new JSONObject("{\"v\":[1e400,1.5e0]}").toMap(true).get("v")).isEqualTo(List.of(new BigDecimal("1e400"), 1.5));
    }

    @Test
    void underflowingTokenIsKeptExact() {
      final Map<String, Object> map = new JSONObject("{\"u\":1e-400,\"z\":0.0,\"s\":1.5e-3}").toMap();
      assertThat(map.get("u")).isEqualTo(new BigDecimal("1e-400"));
      assertThat(map.get("z")).isEqualTo(0.0);
      assertThat(map.get("s")).isEqualTo(0.0015);
    }

    @Test
    void primitiveArraysConvertToEveryCollectionTarget() {
      assertThat(Type.convert(database, new long[] { 1, 2 }, Set.class)).isEqualTo(Set.of(1L, 2L));
      assertThat(Type.convert(database, new int[] { 1, 2 }, Collection.class)).isEqualTo(List.of(1, 2));
      assertThat(Type.convert(database, new float[] { 1.5f }, List.class)).isEqualTo(List.of(1.5f));
      assertThat(Type.convert(database, new short[] { 3 }, List.class)).isEqualTo(List.of((short) 3));
      assertThat(Type.isPrimitiveNumberArray(new int[0])).isTrue();
      assertThat(Type.isPrimitiveNumberArray(new byte[0])).isFalse();
      assertThat(Type.isPrimitiveNumberArray(new boolean[0])).isFalse();
      assertThat(Type.isPrimitiveNumberArray(new char[0])).isFalse();
    }

    @Test
    void listsOfDoublesAndBigDecimalsConvertToAVectorProperty() {
      assertThat((float[]) Type.convert(database, List.of(0.5, 1.5), float[].class)).containsExactly(0.5f, 1.5f);
      assertThat((float[]) Type.convert(database, List.of(new BigDecimal("0.1234567890123456789"), 2), float[].class)).hasSize(2);
    }

    @Test
    void hostileExponentAndLengthAreRefused() {
      for (final String token : new String[] { "1e999999999", "-1e999999999", "1e-999999999", "1e18446744073709551617", "1e-18446744073709551617",
          "9".repeat(1001) }) {
        assertThatThrownBy(() -> new JSONObject("{\"x\":" + token + "}").toMap()).as(token.length() > 30 ? "long token" : token)
            .isInstanceOf(JSONException.class);
        assertThatThrownBy(() -> new JSONObject("{\"a\":[1.5," + token + "]}").toMap(true)).isInstanceOf(JSONException.class);
      }
      // within the limits the number is still kept exact
      assertThat(new JSONObject("{\"x\":1e900}").toMap().get("x")).isEqualTo(new BigDecimal("1e900"));
      assertThat(new JSONObject("{\"z\":0.000000000000000000000}").toMap().get("z")).isEqualTo(0.0);
    }

    @Test
    void everyPrimitiveNumberArrayExpandsToEveryCollectionTarget() {
      final Object[] arrays = { new long[] { 1, 2 }, new double[] { 1, 2 }, new int[] { 1, 2 }, new float[] { 1, 2 }, new short[] { 1, 2 } };
      for (final Object array : arrays) {
        assertThat((List<?>) Type.convert(database, array, List.class)).hasSize(2);
        assertThat((Set<?>) Type.convert(database, array, Set.class)).hasSize(2);
        assertThat((Collection<?>) Type.convert(database, array, Collection.class)).hasSize(2);
      }
    }

    @Test
    void binaryAndNonNumericArraysAreNotNumberArrays() {
      assertThat(Type.isPrimitiveNumberArray(new byte[] { 1, 2 })).isFalse();
      assertThat(Type.isPrimitiveNumberArray(new char[] { 'a' })).isFalse();
    }

    @Test
    void numericArraysAreExact() {
      final Map<String, Object> map = new JSONObject(
          "{\"d\":[0.1,3.141592653589793],\"big\":[1,18446744073709551617],\"mixed\":[2,3.5],\"wide\":[1.5,1.23456789012345678901234567890]}").toMap(true);
      assertThat(map.get("d")).isInstanceOf(double[].class);
      assertThat((double[]) map.get("d")).containsExactly(0.1, 3.141592653589793);
      assertThat(map.get("big")).isEqualTo(List.of(1, new BigDecimal("18446744073709551617")));
      assertThat(map.get("mixed")).isEqualTo(List.of(2, 3.5));
      assertThat(map.get("wide")).isEqualTo(List.of(1.5, new BigDecimal("1.23456789012345678901234567890")));
    }

    @Test
    void fromJsonKeepsBigNumbers() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Big");
        type.createProperty("dec", Type.DECIMAL);
        final MutableDocument doc = database.newDocument("Big").fromJSON(new JSONObject("{\"dec\":123456789012345678901234567890}"));
        doc.save();
        assertThat(doc.get("dec")).isEqualTo(new BigDecimal("123456789012345678901234567890"));
      });
    }

    @Test
    void primitiveArrayWrittenToAListPropertyBecomesItsElements() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Lst");
        type.createProperty("lst", Type.LIST);
        assertThat(Type.convert(database, new long[] { 1, 2, 3 }, List.class)).isEqualTo(List.of(1L, 2L, 3L));
        final MutableDocument doc = database.newDocument("Lst").set("lst", new double[] { 0.5, 1.5 });
        doc.save();
        assertThat(doc.get("lst")).isEqualTo(List.of(0.5, 1.5));
        final MutableDocument ints = database.newDocument("Lst").set("lst", new long[] { 1, 2, 3 });
        ints.save();
        assertThat((List<?>) ints.get("lst")).hasSize(3);
      });
    }
  }

  /**
   * Regression test for #9037: a {@link LocalDate} is a calendar day, so its JSON number must not depend on the JVM default time zone.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class JSONObjectLocalDateTimeZone {

    private static final long UTC_MIDNIGHT = LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

    @Test
    @ResourceLock(Resources.TIME_ZONE)
    void localDateIsTheSameNumberInEveryZone() {
      final TimeZone previous = TimeZone.getDefault();
      try {
        for (final String zone : new String[] { "UTC", "Asia/Seoul", "America/Los_Angeles" }) {
          TimeZone.setDefault(TimeZone.getTimeZone(zone));
          final JSONObject direct = new JSONObject().put("day", (Object) LocalDate.of(2024, 1, 2));
          assertThat(direct.getLong("day")).as(zone).isEqualTo(UTC_MIDNIGHT);
          assertThat(DateUtils.millisToLocalDate(direct.getLong("day"))).as(zone).isEqualTo(LocalDate.of(2024, 1, 2));

          final JSONObject nested = new JSONObject().put("days", (Object) List.of(LocalDate.of(2024, 1, 2)));
          assertThat(nested.getJSONArray("days").getLong(0)).as(zone).isEqualTo(UTC_MIDNIGHT);
        }
      } finally {
        TimeZone.setDefault(previous);
      }
    }
  }

  /**
   * Regression guard for #9114 (#9043, #9044): the JSON classes wrote a non-finite number as {@code 0} (JSONArray.put(Number)), as the
   * text {@code NaN} (JSONArray.put(Object), the Collection constructor), or as {@code null} (JSONObject.put(String, Number)), and a
   * {@code byte[]} inside a list as its identity hash ({@code "[B@33a10788"}) while a top-level one was an array of integers.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class JsonArrayNonFiniteAndByteArray {

    @Test
    void nonFiniteIsNullInEveryPosition() {
      final List<Object> list = new ArrayList<>(List.of(1.0, Double.NaN, Double.POSITIVE_INFINITY, 4.0));
      final String expected = "[1.0,null,null,4.0]";

      assertThat(new JSONArray().put((Number) Double.NaN).toString()).isEqualTo("[null]");
      assertThat(new JSONArray().put((Object) Double.NaN).toString()).isEqualTo("[null]");
      assertThat(new JSONArray().put((Object) Float.NEGATIVE_INFINITY).toString()).isEqualTo("[null]");
      assertThat(new JSONArray(list).toString()).isEqualTo(expected);
      assertThat(new JSONArray(list.toArray()).toString()).isEqualTo(expected);
      assertThat(new JSONObject().put("v", (Object) list).getJSONArray("v").toString()).isEqualTo(expected);
      assertThat(new JSONObject(Map.of("v", list)).getJSONArray("v").toString()).isEqualTo(expected);
      assertThat(new JSONObject().put("v", new float[] { 1F, Float.NaN, Float.POSITIVE_INFINITY }).getJSONArray("v").toString())
          .isEqualTo("[1.0,null,null]");
      assertThat(new JSONObject().put("v", Double.NaN).isNull("v")).isTrue();
    }

    @Test
    void negativeInfinityDoubleArrayAndNullNumber() {
      assertThat(new JSONArray().put((Number) Double.NEGATIVE_INFINITY).toString()).isEqualTo("[null]");
      assertThat(new JSONObject().put("v", new double[] { Double.NaN, 2D }).getJSONArray("v").toString()).isEqualTo("[null,2.0]");
      assertThat(new JSONArray().put((Number) null).toString()).isEqualTo("[null]");
    }

    @Test
    void nonFiniteThroughIndexedPut() {
      final JSONArray array = new JSONArray().put(1).put(2);
      array.put(1, Double.NaN);
      assertThat(array.toString()).isEqualTo("[1,null]");
    }

    @Test
    void finiteNumbersKeepTheirValue() {
      assertThat(new JSONArray(List.of(1, 2L, 3.5d, new BigDecimal("1e400"))).toString()).isEqualTo("[1,2,3.5,1E+400]");
    }

    @Test
    void primitiveArrayInsideAListIsAnArray() {
      final byte[] bytes = { 1, 2 };
      assertThat(new JSONArray(List.of(bytes)).toString()).isEqualTo("[[1,2]]");
      assertThat(new JSONArray().put((Object) bytes).toString()).isEqualTo("[[1,2]]");
      assertThat(new JSONArray().put((Object) new int[] { 1, 2 }).toString()).isEqualTo("[[1,2]]");
      assertThat(new JSONObject().put("v", List.of(bytes)).getJSONArray("v").toString()).isEqualTo("[[1,2]]");
      assertThat(new JSONObject().put("v", Map.of("k", bytes)).getJSONObject("v").toString()).isEqualTo("{\"k\":[1,2]}");
      assertThat(new JSONObject().put("v", bytes).getJSONArray("v").toString()).isEqualTo("[1,2]");
    }

    @Test
    void otherPrimitiveArraysAndSets() {
      assertThat(new JSONArray(List.of(new short[] { 1, 2 }, new boolean[] { true, false }, new char[] { 'a' })).toString())
          .isEqualTo("[[1,2],[true,false],[\"a\"]]");
      assertThat(new JSONArray().put((Object) new int[][] { { 1 }, { 2, 3 } }).toString()).isEqualTo("[[[1],[2,3]]]");
      assertThat(new JSONObject().put("v", new LinkedHashSet<>(List.of(1.0, Double.NaN))).getJSONArray("v").toString()).isEqualTo("[1.0,null]");
    }

    @Test
    void finitePrimitiveArraysRoundTripAndFloatNaNInCollections() {
      assertThat(new JSONObject().put("v", new float[] { 1.5F, -2F }).getJSONArray("v").toString()).isEqualTo("[1.5,-2.0]");
      assertThat(new JSONObject().put("v", new double[] { 0.25D, 3D }).getJSONArray("v").toString()).isEqualTo("[0.25,3.0]");
      assertThat(new JSONObject().put("v", new long[] { 1L, Long.MAX_VALUE }).getJSONArray("v").toString()).isEqualTo("[1," + Long.MAX_VALUE + "]");
      assertThat(new JSONArray(List.of(1F, Float.NaN)).toString()).isEqualTo("[1.0,null]");
      assertThat(new JSONArray(new Object[] { Float.NaN, 2 }).toString()).isEqualTo("[null,2]");
    }

    @Test
    void lazilyParsedNonFiniteTokenIsNullToo() {
      final Number parsed = new LazilyParsedNumber("NaN");
      assertThat(new JSONObject().put("v", parsed).isNull("v")).isTrue();
      assertThat(new JSONArray().put(parsed).toString()).isEqualTo("[null]");
      assertThat(new JSONArray().put(new LazilyParsedNumber("12")).toString()).isEqualTo("[12]");
    }

    @Test
    void smallIntegralNumbersAreLeftAlone() {
      assertThat(new JSONArray().put((Number) (short) 3).put((Number) (byte) 4).put(new AtomicLong(5)).toString())
          .isEqualTo("[3,4,5]");
    }

    @Test
    void validateKeepsAHugeBigDecimalAndRepairsNonFinite() {
      final JSONObject json = new JSONObject().put("big", new BigDecimal("1e400")).put("parsed", (Object) new LazilyParsedNumber("NaN"));
      json.put("arr", new JSONArray().put(new BigDecimal("1e400")).put(new LazilyParsedNumber("Infinity")));
      json.validate();
      assertThat(json.get("big")).isEqualTo(new BigDecimal("1e400"));
      assertThat(json.isNull("parsed")).isTrue();
      assertThat(json.getJSONArray("arr").toString()).isEqualTo("[1E+400,null]");
    }
  }

  /**
   * Regression guard for #9114 (#9044): a stored record whose list holds a {@code byte[]} serialized it as its identity hash.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StoredByteArrayInList extends TestHelper {

    @Test
    void storedRecordWithByteArrayInList() {
      database.command("sql", "CREATE DOCUMENT TYPE Blob");
      database.transaction(() -> {
        final Map<String, Object> map = new HashMap<>();
        map.put("k", new byte[] { 1, 2 });
        database.newDocument("Blob").set("in_list", new ArrayList<>(List.of(new byte[] { 1, 2 }))).set("in_map", map).save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT FROM Blob")) {
        final Result row = rs.next();
        final JSONObject json = row.toJSON();
        assertThat(json.getJSONArray("in_list").toString()).doesNotContain("[B@");
        assertThat(json.getJSONArray("in_list").getJSONArray(0).toString()).isEqualTo("[1,2]");
        assertThat(json.getJSONObject("in_map").getJSONArray("k").toString()).isEqualTo("[1,2]");
      }
    }
  }

  /**
   * Regression test for #9630: {@code arcadedb.dateTimeImplementation=java.util.Calendar} and
   * {@code arcadedb.dateImplementation=java.util.Calendar} are supported settings, so a {@link Calendar} is a value a record
   * can carry. {@link JSONObject} wrote one as its {@code toString()} - a 600-character dump of the calendar's fields - where
   * a {@link Date} holding the same instant was written as its epoch milliseconds.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class JSONObjectCalendar {

    private static final long MILLIS = Instant.parse("2024-03-15T14:30:45.123Z").toEpochMilli();

    private static Calendar calendar(final String zone) {
      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(zone));
      calendar.setTimeInMillis(MILLIS);
      return calendar;
    }

    @Test
    void calendarIsWrittenAsItsEpochMillisLikeADate() {
      for (final String zone : new String[] { "UTC", "Asia/Tokyo", "America/New_York" }) {
        final JSONObject json = new JSONObject().put("ts", (Object) calendar(zone)).put("date", (Object) new Date(MILLIS));
        assertThat(json.getLong("ts")).as(zone).isEqualTo(MILLIS);
        assertThat(json.getLong("ts")).as(zone).isEqualTo(json.getLong("date"));
      }
    }

    @Test
    void calendarInsideACollectionOrMapIsWrittenAsItsEpochMillis() {
      final JSONObject json = new JSONObject().put("list", (Object) List.of(calendar("Asia/Tokyo")))
          .put("map", (Object) Map.of("ts", calendar("America/New_York")));
      assertThat(json.getJSONArray("list").getLong(0)).isEqualTo(MILLIS);
      assertThat(json.getJSONObject("map").getLong("ts")).isEqualTo(MILLIS);
    }

    /**
     * With a date format set, a Calendar is written as the Date it holds would be, whatever zone the calendar carries: the
     * same instant must not become two different strings depending on which of the two configured classes holds it.
     */
    @Test
    void calendarFollowsTheDateFormatExactlyAsADate() {
      for (final String zone : new String[] { "UTC", "Asia/Tokyo", "America/New_York" }) {
        final JSONObject json = new JSONObject().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").put("ts", (Object) calendar(zone))
            .put("date", (Object) new Date(MILLIS));
        assertThat(json.getString("ts")).as(zone).isEqualTo(json.getString("date"));
        assertThat(json.getString("ts")).as(zone).matches("2024-03-1[45] \\d\\d:\\d\\d:45\\.123");
      }
    }
  }

  /**
   * Regression for #9805: a JSON object's member name is DERIVED from a map key, it is not the key. {@code JsonSerializer.serializeMap}
   * held that rule, the shared {@link JSONObject} constructor did not: it cast every key to {@code String}, so a stored MAP with an
   * Integer key made {@code Result.toJSON()}, {@code convert.toJson()}, {@code asJSON()} and the Postgres wire throw
   * {@code ClassCastException}, and a {@code null} key an {@code IllegalArgumentException} that names neither the property nor the
   * key. The claim is pinned, not the case: a map whose keys are an Integer, a Long, a {@code null}, a Date and an arbitrary object
   * is rendered by every entry point to the same member names.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class JsonObjectNonStringMapKey extends TestHelper {

    /** An arbitrary key: only its text can name a JSON member. */
    private record Handle(String id) {
      @Override
      public String toString() {
        return "handle-" + id;
      }
    }

    private static final Date DATE_KEY = new Date(1_700_000_000_000L);

    private static Map<Object, Object> mixedKeys() {
      final Map<Object, Object> map = new LinkedHashMap<>();
      map.put(1, "int");
      map.put(2L, "long");
      map.put(null, "null");
      map.put(DATE_KEY, "date");
      map.put(new Handle("a"), "object");
      return map;
    }

    private static Set<String> mixedKeyNames() {
      return new TreeSet<>(List.of("1", "2", "null", DATE_KEY.toString(), "handle-a"));
    }

    /** {@code asJSON()} answers a JSON object and {@code convert.toJson()} its text: both must name the same members. */
    private static JSONObject asJson(final Object value) {
      return value instanceof JSONObject json ? json : new JSONObject(value.toString());
    }

    private static Set<String> names(final JSONObject json) {
      return new TreeSet<>(json.keySet());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void constructorNamesEveryKey() {
      // a raw Map is how every caller reaches the constructor: the Map<String, ?> of its signature is a promise none can keep
      final JSONObject json = new JSONObject((Map) mixedKeys());
      assertThat(names(json)).isEqualTo(mixedKeyNames());
      assertThat(json.getString("1")).isEqualTo("int");
      assertThat(json.getString("null")).isEqualTo("null");
      assertThat(json.getString("handle-a")).isEqualTo("object");
    }

    @Test
    void putOfANestedMapNamesEveryKey() {
      final JSONObject json = new JSONObject().put("m", mixedKeys());
      assertThat(names(json.getJSONObject("m"))).isEqualTo(mixedKeyNames());
    }

    @Test
    void arrayElementThatIsAMapNamesEveryKey() {
      final JSONArray array = new JSONArray();
      array.put((Object) mixedKeys());
      assertThat(names(array.getJSONObject(0))).isEqualTo(mixedKeyNames());

      final JSONArray fromCollection = new JSONArray(List.of(mixedKeys()));
      assertThat(names(fromCollection.getJSONObject(0))).isEqualTo(mixedKeyNames());

      final List<JSONObject> objects = array.toListOfObjects();
      assertThat(names(objects.getFirst())).isEqualTo(mixedKeyNames());
    }

    @Test
    void keysWithTheSameTextNameOneMemberAndTheLastEntryWinsOnEverySerializer() {
      final Map<Object, Object> map = new LinkedHashMap<>();
      map.put(null, "first");
      map.put("null", "second");
      map.put(1, "int");
      map.put("1", "string");

      final JSONObject constructed = new JSONObject(map);
      assertThat(names(constructed)).containsExactly("1", "null");
      assertThat(constructed.getString("null")).isEqualTo("second");
      assertThat(constructed.getString("1")).isEqualTo("string");

      final ResultInternal row = new ResultInternal(database);
      row.setProperty("m", map);
      final JSONObject viaResult = row.toJSON().getJSONObject("m");
      assertThat(viaResult.getString("null")).isEqualTo("second");
      assertThat(viaResult.getString("1")).isEqualTo("string");

      final JSONObject viaSerializer = JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("m");
      assertThat(viaSerializer.getString("null")).isEqualTo("second");
      assertThat(viaSerializer.getString("1")).isEqualTo("string");
    }

    @Test
    void anEnumKeyIsNamedByItsText() {
      // the shape Gremlin's elementMap() produces: its keys are the T enum (T.id, T.label)
      final Map<Object, Object> map = new LinkedHashMap<>();
      map.put(TimeUnit.SECONDS, 1);
      map.put("name", "Jay");
      assertThat(names(new JSONObject(map))).containsExactly("SECONDS", "name");
      assertThat(names(new JSONObject().put("m", map).getJSONObject("m"))).containsExactly("SECONDS", "name");
    }

    @Test
    void aMapOfMapsNamesEveryKeyAtEveryLevel() {
      final Map<Object, Object> outer = new HashMap<>();
      outer.put(7, mixedKeys());
      final JSONObject json = new JSONObject().put("m", outer);
      assertThat(names(json.getJSONObject("m").getJSONObject("7"))).isEqualTo(mixedKeyNames());
    }

    @Test
    void nonStringKeysOfAStoredMapRenderThroughEveryEntryPoint() {
      store();
      for (int round = 0; round < 2; round++) {
        // round 1 runs after a close and reopen: the keys come back as Integer/Long, not as the Strings they were written as
        if (round == 1)
          reopenDatabase();

        try (final ResultSet rs = database.query("sql", "SELECT name, m FROM D WHERE name = 'intKeys'")) {
          final Result row = rs.next();
          assertThat(row.<Map<?, ?>>getProperty("m").keySet()).as("keys come back as numbers").allMatch(k -> k instanceof Number);

          final Set<String> expected = Set.of("1", "2");
          assertThat(names(row.toJSON().getJSONObject("m"))).as("Result.toJSON()").isEqualTo(expected);
          assertThat(names(JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("m"))).as(
              "JsonSerializer.serializeResult").isEqualTo(expected);
        }

        try (final ResultSet rs = database.query("sql", "SELECT FROM D WHERE name = 'intKeys'")) {
          assertThat(names(rs.next().toJSON().getJSONObject("m"))).as("element Result.toJSON()").isEqualTo(Set.of("1", "2"));
        }

        try (final ResultSet rs = database.query("sql", "SELECT m.asJSON() AS j, convert.toJson(m) AS c FROM D WHERE name = 'intKeys'")) {
          final Result row = rs.next();
          assertThat(names(asJson(row.getProperty("j")))).as("asJSON()").isEqualTo(Set.of("1", "2"));
          assertThat(names(asJson(row.getProperty("c")))).as("convert.toJson()").isEqualTo(Set.of("1", "2"));
        }
      }
    }

    @Test
    void aNullKeyOfAStoredMapIsNamedNullThroughEveryEntryPoint() {
      store();
      try (final ResultSet rs = database.query("sql", "SELECT name, m FROM D WHERE name = 'nullKey'")) {
        final Result row = rs.next();
        assertThat(names(row.toJSON().getJSONObject("m"))).as("Result.toJSON()").isEqualTo(Set.of("null"));
        assertThat(names(JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("m"))).as(
            "JsonSerializer.serializeResult").isEqualTo(Set.of("null"));
      }

      try (final ResultSet rs = database.query("sql", "SELECT m.asJSON() AS j, convert.toJson(m) AS c FROM D WHERE name = 'nullKey'")) {
        final Result row = rs.next();
        assertThat(names(asJson(row.getProperty("j")))).as("asJSON()").isEqualTo(Set.of("null"));
        assertThat(names(asJson(row.getProperty("c")))).as("convert.toJson()").isEqualTo(Set.of("null"));
      }
    }

    private void store() {
      final DocumentType type = database.getSchema().createDocumentType("D");
      type.createProperty("name", Type.STRING);
      type.createProperty("m", Type.MAP);

      final Map<Object, Object> intKeys = new LinkedHashMap<>();
      intKeys.put(1, "one");
      intKeys.put(2L, "two");
      final Map<Object, Object> nullKey = new HashMap<>();
      nullKey.put(null, "nothing");

      database.transaction(() -> {
        database.newDocument("D").set("name", "intKeys").set("m", intKeys).save();
        database.newDocument("D").set("name", "nullKey").set("m", nullKey).save();
      });
    }
  }
}
