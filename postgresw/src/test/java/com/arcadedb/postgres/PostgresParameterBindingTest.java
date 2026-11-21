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
package com.arcadedb.postgres;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.arcadedb.postgres.PostgresNetworkExecutor.highestParameterPlaceholder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the values of a Bind are read and how a Parse counts the parameters they fill: the text and binary array formats, the
 * temporal and jsonb types, the precomputed answer a portal binds from, and the {@code $N} placeholder count.
 */
@SuppressWarnings("unchecked")
class PostgresParameterBindingTest {

  /*
   * Regression test for issue #9007: an array column requested in binary format was sent as the text literal {@code {1,2,3}}
   * instead of PostgreSQL's binary array layout, so pgjdbc failed with BufferUnderflowException from the 6th execution on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static byte[] encode(final PostgresType arrayType, final Object value) {
    final Binary buffer = new Binary();
    arrayType.serializeAsBinary(arrayType, buffer, value);
    buffer.flip();
    final int length = buffer.getInt();
    final byte[] data = new byte[length];
    buffer.getByteArray(data);
    assertThat(buffer.size() - buffer.position()).as("the declared length covers the whole value").isZero();
    return data;
  }

  // Regression: #9007
  @Test
  void intArrayUsesTheBinaryArrayHeader() {
    final ByteBuffer data = ByteBuffer.wrap(encode(PostgresType.ARRAY_INT, List.of(1, 2, 3)));
    assertThat(data.getInt()).as("dimensions").isEqualTo(1);
    assertThat(data.getInt()).as("has null").isZero();
    assertThat(data.getInt()).as("element OID").isEqualTo(PostgresType.INTEGER.code);
    assertThat(data.getInt()).as("dimension length").isEqualTo(3);
    assertThat(data.getInt()).as("lower bound").isEqualTo(1);
    for (int i = 1; i <= 3; i++) {
      assertThat(data.getInt()).as("element length").isEqualTo(4);
      assertThat(data.getInt()).isEqualTo(i);
    }
    assertThat(data.remaining()).isZero();
  }

  // Regression: #9007
  @Test
  void everyArrayTypeRoundTripsThroughTheBinaryDecoder() {
    assertRoundTrip(PostgresType.ARRAY_INT, List.of(1, -2, 3), List.of(1, -2, 3));
    assertRoundTrip(PostgresType.ARRAY_LONG, List.of(1L, Long.MAX_VALUE), List.of(1L, Long.MAX_VALUE));
    assertRoundTrip(PostgresType.ARRAY_REAL, List.of(1.5f, -2.25f), List.of(1.5f, -2.25f));
    assertRoundTrip(PostgresType.ARRAY_DOUBLE, List.of(1.5d, 1e300), List.of(1.5d, 1e300));
    assertRoundTrip(PostgresType.ARRAY_BOOLEAN, List.of(true, false), List.of(true, false));
    assertRoundTrip(PostgresType.ARRAY_TEXT, List.of("a", "b,\"c\"", "é😀"), List.of("a", "b,\"c\"", "é😀"));
    assertRoundTrip(PostgresType.ARRAY_NUMERIC, List.of(new BigDecimal("1.50"), new BigDecimal("-3")),
        List.of(new BigDecimal("1.50"), new BigDecimal("-3")));
  }

  // Regression: #9007
  @Test
  void nullElementsAreFlaggedAndRoundTrip() {
    final List<Object> value = new ArrayList<>(Arrays.asList(1, null, 3));
    final byte[] data = encode(PostgresType.ARRAY_INT, value);
    assertThat(ByteBuffer.wrap(data).getInt(4)).as("has null").isEqualTo(1);
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data)).isEqualTo(value);
  }

  // Regression: #9007
  @Test
  void emptyArrayHasNoDimensions() {
    final byte[] data = encode(PostgresType.ARRAY_INT, List.of());
    assertThat(data).hasSize(12);
    assertThat(ByteBuffer.wrap(data).getInt()).isZero();
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data)).isEqualTo(List.of());
  }

  // Regression: #9007
  @Test
  void primitiveArrayIsEncodedToo() {
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, encode(PostgresType.ARRAY_INT, new int[] { 7, 8 })))
        .isEqualTo(List.of(7, 8));
  }

  // Regression: #9007
  @Test
  void textArrayHoldingNonStringsEncodesTheirText() {
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, encode(PostgresType.ARRAY_TEXT, List.of(1, "x", List.of(1, 2)))))
        .isEqualTo(List.of("1", "x", "[1,2]"));
  }

  // Regression: #9007
  @Test
  void mixedNumericListsAreConvertedToTheAnnouncedElementType() {
    assertRoundTrip(PostgresType.ARRAY_LONG, List.of(1L, 2, (short) 3), List.of(1L, 2L, 3L));
    assertRoundTrip(PostgresType.ARRAY_INT, List.of(1, 2L, (short) 3), List.of(1, 2, 3));
    assertRoundTrip(PostgresType.ARRAY_DOUBLE, List.of(1.5d, 2, 3L), List.of(1.5d, 2.0d, 3.0d));
  }

  // Regression: #9007
  @Test
  void anElementOfAnotherTypeFailsWithAClearError() {
    assertThatThrownBy(() -> encode(PostgresType.ARRAY_LONG, List.of(1L, "x"))).isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("String element").hasMessageContaining("_int8");
  }

  // Regression: #9007
  @Test
  void heterogeneousListsAreTypedFromAllTheirElementsSoNothingIsNarrowed() {
    final List<Object> mixedNumbers = List.of(1, 2.5);
    final PostgresType numbersType = PostgresType.getTypeForValue(mixedNumbers);
    assertThat(PostgresType.deserialize(numbersType.code, 1, encode(numbersType, mixedNumbers)))
        .asList().extracting(e -> ((Number) e).doubleValue()).containsExactly(1.0d, 2.5d);

    final List<Object> numberAndText = List.of(1, "a");
    final PostgresType textType = PostgresType.getTypeForValue(numberAndText);
    assertThat(textType).isEqualTo(PostgresType.ARRAY_TEXT);
    assertThat(PostgresType.deserialize(textType.code, 1, encode(textType, numberAndText))).isEqualTo(List.of("1", "a"));
  }

  // Regression: #9007
  @Test
  void aNonArrayValueIsRefusedForAnArrayColumn() {
    assertThatThrownBy(() -> encode(PostgresType.ARRAY_TEXT, "not a list")).isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("_text");
  }

  // Regression: #9007
  @Test
  void charAndNestedListArraysEncode() {
    // a "char"[] element reads back as the Character the scalar "char" and the text format read (issue #10870)
    assertRoundTrip(PostgresType.ARRAY_CHAR, List.of('a', 'b'), List.of('a', 'b'));
    assertRoundTrip(PostgresType.ARRAY_JSON, List.of(List.of(1, 2)), List.of("[1,2]"));
  }

  private static void assertRoundTrip(final PostgresType arrayType, final List<?> value, final List<?> expected) {
    final Object decoded = PostgresType.deserialize(arrayType.code, 1, encode(arrayType, value));
    assertThat(decoded).as(arrayType.name()).isEqualTo(expected);
  }

  /*
   * Regression test for issue #10870: the text-format array parser dropped an empty-string element, read the unquoted SQL NULL as
   * the four-character string "NULL" (and failed an int4[] holding one), and read any untyped parameter starting and ending with a
   * brace - a JSON document included - as a list. The server could not read back the array literal it writes itself.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static Object text(final PostgresType type, final String literal) {
    return PostgresType.deserialize(type.code, 0, literal.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  private static Object untyped(final String literal) {
    return PostgresType.deserialize(0, 0, literal.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  // Regression: #10870
  @Test
  void emptyStringAndNullElementsKeepTheirPlace() {
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"a\",\"\",\"b\",NULL,\"NULL\"}")).containsExactly("a", "", "b", null,
        "NULL");
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"\"}")).containsExactly("");
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{}")).isEmpty();
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{a,null,Null}")).containsExactly("a", null, null);
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"NULL\",NULL}")).containsExactly("NULL", null);
    // an escaped character makes the token data, not the NULL keyword
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\\NULL}")).containsExactly("NULL");
  }

  // Regression: #10870
  @Test
  void aNullElementOfANumericArrayIsNullNotAParseFailure() {
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "{1,NULL,3}")).containsExactly(1, null, 3);
    assertThat((List<Object>) text(PostgresType.ARRAY_LONG, "{ 1 , NULL , 3 }")).containsExactly(1L, null, 3L);
    assertThat((List<Object>) text(PostgresType.ARRAY_DOUBLE, "{1.5,NULL}")).containsExactly(1.5d, null);
  }

  // Regression: #10870
  @Test
  void whitespaceQuotingAndEscapesFollowPostgres() {
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{ a b , \"  c  \" ,d\\ }")).containsExactly("a b", "  c  ", "d ");
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"a,b\",c}")).containsExactly("a,b", "c");
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"x\\\"y\",\"C:\\\\temp\",\"{}\"}")).containsExactly("x\"y", "C:\\temp",
        "{}");
  }

  // Regression: #10870
  @Test
  void booleanElementsAcceptPostgresSpellings() {
    // PostgreSQL writes a bool[] as {t,f}: Boolean.parseBoolean read "t" as false
    assertThat((List<Object>) text(PostgresType.ARRAY_BOOLEAN, "{t,f,true,false,NULL}")).containsExactly(true, false, true, false, null);
  }

  // Regression: #10870
  @Test
  void multidimensionalLiteralsAreReadFlatAsTheBinaryFormatReadsThem() {
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "{{1,2},{3,4}}")).containsExactly(1, 2, 3, 4);
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "[1:2][1:1]={{1},{2}}")).containsExactly(1, 2);
  }

  // Regression: #10870
  @Test
  void aLiteralNestedDeeperThanPostgresAllowsIsRefusedNotRecursedInto() {
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "{{{{{{1}}}}}}")).containsExactly(1);
    assertThatThrownBy(() -> text(PostgresType.ARRAY_INT, "{{{{{{{1}}}}}}}")).isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("dimensions exceeds");
    // a hostile literal of nothing but opening braces fails on the 7th, not with a StackOverflowError
    assertThatThrownBy(() -> text(PostgresType.ARRAY_TEXT, "{".repeat(100_000))).isInstanceOf(PostgresProtocolException.class)
        // and the message quotes the start of the literal, not all of it
        .satisfies(e -> assertThat(e.getMessage().length()).isLessThan(300));
  }

  // Regression: #10870
  @Test
  void theLeniencyTheProtocolAlwaysHadIsKept() {
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "")).isEmpty();
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "1,NULL,3")).containsExactly(1, null, 3);
  }

  // Regression: #10870
  @Test
  void malformedLiteralsAreRefusedNotReinterpreted() {
    for (final String malformed : List.of("{a,,b}", "{a", "a,,b", "{\"a\"b}", "{a}x", "{{1,2},{3}}", "{{1},2}", "{1,{2}}",
        "{a\"b}"))
      assertThatThrownBy(() -> text(PostgresType.ARRAY_TEXT, malformed)).as(malformed).isInstanceOf(PostgresProtocolException.class)
          .hasMessageContaining("malformed array literal");
  }

  // Regression: #10870
  @Test
  void anUntypedParameterIsAListOnlyWhenItIsAnArrayLiteral() {
    // psycopg sends a Python list as an untyped array literal (issue #2559)
    assertThat((List<Object>) untyped("{\"#1:0\",\"#1:1\"}")).containsExactly("#1:0", "#1:1");
    assertThat((List<Object>) untyped("{a,NULL,\"\"}")).containsExactly("a", null, "");
    // a JSON document merely starts and ends with a brace: it stays the string the client sent
    assertThat(untyped("{\"k\": 1}")).isEqualTo("{\"k\": 1}");
    assertThat(untyped("{\"k\":{\"j\":[1,2]}}")).isEqualTo("{\"k\":{\"j\":[1,2]}}");
    assertThat(untyped("plain")).isEqualTo("plain");
  }

  // Regression: #10870
  /**
   * The round-trip property, for every array type the enum declares: what the server writes as text, it reads back as the same
   * list, and the text and binary formats agree on it - including a NULL element, an empty string and the characters the literal
   * has to escape.
   */
  @Test
  void everyArrayTypeReadsBackItsOwnTextAndAgreesWithBinary() {
    final Map<PostgresType, List<Object>> values = new EnumMap<>(PostgresType.class);
    values.put(PostgresType.ARRAY_INT, Arrays.asList(1, null, -3));
    values.put(PostgresType.ARRAY_SMALLINT, Arrays.asList((short) 1, null, (short) -3));
    values.put(PostgresType.ARRAY_LONG, Arrays.asList(1L, null, Long.MAX_VALUE));
    values.put(PostgresType.ARRAY_REAL, Arrays.asList(1.5f, null, -2.25f));
    values.put(PostgresType.ARRAY_DOUBLE, Arrays.asList(1.5d, null, 1e300));
    values.put(PostgresType.ARRAY_NUMERIC, Arrays.asList(new BigDecimal("1.50"), null, new BigDecimal("-3")));
    values.put(PostgresType.ARRAY_BOOLEAN, Arrays.asList(true, null, false));
    // 'é' is past 0x7F: the binary decoder read it sign-extended, as '￩'
    values.put(PostgresType.ARRAY_CHAR, Arrays.asList('a', null, ',', '"', '\\', '{', 'é'));
    final List<Object> strings = Arrays.asList("a", "", null, "NULL", "b,c", "{d}", "e\"f", "g\\h", " i ");
    values.put(PostgresType.ARRAY_TEXT, strings);
    values.put(PostgresType.ARRAY_VARCHAR, strings);
    values.put(PostgresType.ARRAY_BPCHAR, strings);
    values.put(PostgresType.ARRAY_JSON, Arrays.asList("{\"k\":1}", null, "[1,\"a,b\"]"));
    values.put(PostgresType.ARRAY_BYTEA, Arrays.asList(new byte[] { 1, 2, (byte) 0xff }, null, new byte[0]));
    // a date element decodes to the LocalDate it names (issue #11956)
    values.put(PostgresType.ARRAY_DATE, Arrays.asList(LocalDate.of(2024, 2, 29), null));
    values.put(PostgresType.ARRAY_TIMESTAMP, Arrays.asList(LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123456000), null));
    // the array types of #12449
    values.put(PostgresType.ARRAY_TIME, Arrays.asList(LocalTime.of(13, 14, 15, 123456000), null, LocalTime.MIDNIGHT));
    values.put(PostgresType.ARRAY_TIMETZ, Arrays.asList(OffsetTime.of(13, 14, 15, 500000000, ZoneOffset.ofHoursMinutes(5, 30)), null,
        OffsetTime.of(1, 0, 0, 0, ZoneOffset.ofHours(-2))));
    values.put(PostgresType.ARRAY_TIMESTAMPTZ, Arrays.asList(ZonedDateTime.of(2024, 2, 29, 13, 14, 15, 123456000, ZoneOffset.UTC), null));
    values.put(PostgresType.ARRAY_INTERVAL, Arrays.asList(new CypherDuration(14, 3, 4 * 3600 + 5 * 60 + 6, 500000000L), null,
        new CypherDuration(-14, -3, -1, 0), new CypherDuration(0, 0, 0, 0)));
    values.put(PostgresType.ARRAY_JSONB, Arrays.asList("{\"k\":1}", null, "[1,\"a,b\"]"));
    values.put(PostgresType.ARRAY_NAME, Arrays.asList("pg_class", null, "a b"));
    values.put(PostgresType.ARRAY_OID, Arrays.asList(1L, null, 4294967295L));
    values.put(PostgresType.ARRAY_INT2VECTOR, Arrays.asList("1 3", null, "2"));

    for (final PostgresType type : PostgresType.values()) {
      if (!type.isArrayType())
        continue;
      final List<Object> value = values.get(type);
      assertThat(value).as("a round-trip value for %s", type).isNotNull();

      final Object fromText = text(type, PostgresType.VARCHAR.toText(type, value));
      final Object fromBinary = PostgresType.deserialize(type.code, 1, encodeBinary(type, value));
      assertThat(normalize(fromText)).as("%s text round trip", type).isEqualTo(normalize(value));
      assertThat(normalize(fromBinary)).as("%s binary round trip", type).isEqualTo(normalize(value));
    }
  }

  // Regression: #10870
  @Test
  void aMultidimensionalArrayIsTheSameListInBothFormats() {
    final ByteBuffer buffer = ByteBuffer.allocate(12 + 16 + 4 * 8);
    buffer.putInt(2).putInt(0).putInt(PostgresType.INTEGER.code);
    buffer.putInt(2).putInt(1).putInt(2).putInt(1);
    for (int i = 1; i <= 4; i++)
      buffer.putInt(4).putInt(i);
    final Object fromBinary = PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, buffer.array());
    assertThat(fromBinary).isEqualTo(text(PostgresType.ARRAY_INT, "{{1,2},{3,4}}"));
  }

  private static byte[] encodeBinary(final PostgresType type, final Object value) {
    final Binary buffer = new Binary();
    type.serializeAsBinary(type, buffer, value);
    buffer.flip();
    final byte[] data = new byte[buffer.getInt()];
    buffer.getByteArray(data);
    return data;
  }

  /** Byte arrays compare by content, so the comparison is on the value and not on the Java identity. */
  private static List<Object> normalize(final Object list) {
    final List<Object> normalized = new ArrayList<>();
    for (final Object element : (List<?>) list)
      normalized.add(switch (element) {
        case byte[] bytes -> Arrays.toString(bytes);
        case null -> null;
        default -> element;
      });
    return normalized;
  }

  /*
   * Issue #11960: a timestamptz, time, timetz, interval or jsonb parameter failed the Bind with "Type with code N not supported for
   * deserializing" (XX000). Each OID now decodes, in text and binary format, to the engine value of the matching type.
   */
  private static Object text(final int oid, final String value) {
    return PostgresType.deserialize(oid, 0, value.getBytes(StandardCharsets.UTF_8));
  }

  private static Object binary(final int oid, final byte[] value) {
    return PostgresType.deserialize(oid, 1, value);
  }

  private static CypherDuration duration(final long months, final long days, final long seconds, final long nanos) {
    return new CypherDuration(months, days, seconds, nanos);
  }

  // Regression: #11960
  @Test
  void timestamptzKeepsTheOffsetItWasSentWith() {
    final ZonedDateTime expected = LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000).atZone(ZoneOffset.ofHours(2));
    assertThat(text(1184, "2024-02-29 13:14:15.123456+02")).isEqualTo(expected);
    assertThat(text(1184, "2024-02-29 13:14:15.123456+02:00")).isEqualTo(expected);
    assertThat(text(1184, "2024-02-29T13:14:15.123456+0200")).isEqualTo(expected);
    assertThat(text(1184, "2024-02-29 13:14:15Z")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 14, 15).atZone(ZoneOffset.UTC));
    assertThat(text(1184, "2024-02-29 13:14:15")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 14, 15).atZone(ZoneOffset.UTC));

    // binary: microseconds since 2000-01-01 UTC, the same instant read at UTC
    final long micros = (expected.toEpochSecond() - 946684800L) * 1_000_000L + 123_456L;
    final Object decoded = binary(1184, ByteBuffer.allocate(8).putLong(micros).array());
    assertThat(((ZonedDateTime) decoded).toInstant()).isEqualTo(expected.toInstant());
  }

  // Regression: #11960
  @Test
  void timeAndTimetz() {
    assertThat(text(1083, "10:15:30")).isEqualTo(LocalTime.of(10, 15, 30));
    assertThat(text(1083, "10:15:30.000004")).isEqualTo(LocalTime.of(10, 15, 30, 4_000));
    assertThat(text(1266, "10:15:30+02")).isEqualTo(OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHours(2)));
    assertThat(text(1266, "10:15:30-05:30")).isEqualTo(OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHoursMinutes(-5, -30)));

    final long micros = LocalTime.of(10, 15, 30).toNanoOfDay() / 1_000L;
    assertThat(binary(1083, ByteBuffer.allocate(8).putLong(micros).array())).isEqualTo(LocalTime.of(10, 15, 30));
    // timetz stores the offset in seconds WEST of UTC: +02:00 is -7200
    assertThat(binary(1266, ByteBuffer.allocate(12).putLong(micros).putInt(-7200).array()))
        .isEqualTo(OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHours(2)));
  }

  // Regression: #11960
  @Test
  void intervalInEveryStyleAClientSends() {
    // psycopg (postgres style, str(timedelta))
    assertThat(text(1186, "1 day 0:00:05")).isEqualTo(duration(0, 1, 5, 0));
    assertThat(text(1186, "-1 day 23:59:55")).isEqualTo(duration(0, -1, 86395, 0));
    assertThat(text(1186, "0:00:00.000004")).isEqualTo(duration(0, 0, 0, 4_000));
    // psycopg sql_standard
    assertThat(text(1186, "+1 day +5 second +0 microsecond")).isEqualTo(duration(0, 1, 5, 0));
    // pgjdbc PGInterval
    assertThat(text(1186, "0 years 2 mons 1 days 3 hours 0 mins 5.5 secs")).isEqualTo(duration(2, 1, 3 * 3600 + 5, 500_000_000));
    // postgres_verbose and ISO 8601
    assertThat(text(1186, "@ 1 year 2 days ago")).isEqualTo(duration(-12, -2, 0, 0));
    assertThat(text(1186, "P1DT5S")).isEqualTo(duration(0, 1, 5, 0));
    // a fraction of a month spills into days of 30, a fraction of a day into hours
    assertThat(text(1186, "1.5 months")).isEqualTo(duration(1, 15, 0, 0));
    assertThat(text(1186, "1.5 days")).isEqualTo(duration(0, 1, 12 * 3600, 0));

    assertThat(binary(1186, ByteBuffer.allocate(16).putLong(5_000_004L).putInt(1).putInt(2).array()))
        .isEqualTo(duration(2, 1, 5, 4_000));

    assertThatThrownBy(() -> text(1186, "5 fortnights")).isInstanceOf(IllegalArgumentException.class);
  }

  // Regression: #11960
  @Test
  void intervalSqlStandardFields() {
    assertThat(text(1186, "1-2")).isEqualTo(duration(14, 0, 0, 0));
    assertThat(text(1186, "-1-2")).isEqualTo(duration(-14, 0, 0, 0));
    assertThat(text(1186, "3 4:05:06")).isEqualTo(duration(0, 3, 4 * 3600 + 5 * 60 + 6, 0));
    assertThat(text(1186, "1-2 3 4:05:06")).isEqualTo(duration(14, 3, 4 * 3600 + 5 * 60 + 6, 0));
    // a lone number is seconds
    assertThat(text(1186, "5")).isEqualTo(duration(0, 0, 5, 0));
  }

  // Regression: #11960
  @Test
  void isoFractionalMonthSpillsIntoDaysOfThirty() {
    // as "1.5 months": no hours from an average month length
    assertThat(text(1186, "P1.5M")).isEqualTo(duration(1, 15, 0, 0));
    assertThat(text(1186, "P1.5D")).isEqualTo(duration(0, 1, 12 * 3600, 0));
    assertThat(text(1186, "-P1Y2M")).isEqualTo(duration(-14, 0, 0, 0));
    assertThat(text(1186, "PT1M30S")).isEqualTo(duration(0, 0, 90, 0));
    // the alternative format
    assertThat(text(1186, "P0001-02-03T04:05:06")).isEqualTo(duration(14, 3, 4 * 3600 + 5 * 60 + 6, 0));
  }

  // Regression: #11960
  @Test
  void intervalAmountsOfUnboundedMagnitudeAreRefusedBeforeAnyArithmetic() {
    // each of these used to make setScale build a number of about a hundred million digits
    for (final String huge : new String[] { "1e100000000 months", "1e100000000 days", "1e-100000000 seconds", "1e100000000:00" })
      assertThatThrownBy(() -> text(1186, huge)).isInstanceOf(IllegalArgumentException.class);
  }

  // Regression: #11960
  @Test
  void timestamptzRefusesADateTheCalendarDoesNotHave() {
    assertThatThrownBy(() -> text(1184, "2024-02-30 12:00:00+00")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> text(1184, "2023-02-29 12:00:00+00")).isInstanceOf(IllegalArgumentException.class);
  }

  // Regression: #11960
  @Test
  void scalarJsonDocuments() {
    for (final int oid : new int[] { 114, 3802 }) {
      assertThat(text(oid, "42")).isEqualTo(42);
      assertThat(text(oid, "\"s\"")).isEqualTo("s");
      assertThat(text(oid, "true")).isEqualTo(true);
      assertThat(text(oid, "null")).isNull();
    }
    final byte[] versioned = ByteBuffer.allocate(3).put((byte) 1).put("42".getBytes(StandardCharsets.UTF_8)).array();
    assertThat(binary(3802, versioned)).isEqualTo(42);
  }

  // Regression: #11960
  @Test
  void malformedScalarJsonIsRefused() {
    for (final int oid : new int[] { 114, 3802 })
      for (final String malformed : new String[] { "not_json", "'s'", "1 2", "tru", "\"a\" x", "", "   ", "\n\t" })
        assertThatThrownBy(() -> text(oid, malformed)).as("%d '%s'", oid, malformed).isInstanceOf(IllegalArgumentException.class);
    final byte[] versioned = ByteBuffer.allocate(9).put((byte) 1).put("not_json".getBytes(StandardCharsets.UTF_8)).array();
    assertThatThrownBy(() -> binary(3802, versioned)).isInstanceOf(IllegalArgumentException.class);
    // a binary jsonb of the version byte alone, or of the version byte and blanks, holds no document
    assertThatThrownBy(() -> binary(3802, new byte[] { 1 })).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> binary(3802, new byte[] { 1, ' ', ' ' })).isInstanceOf(IllegalArgumentException.class);
    // binary json (114) is the text itself
    assertThatThrownBy(() -> binary(114, new byte[] { ' ' })).isInstanceOf(IllegalArgumentException.class);
  }

  // Regression: #11960
  @Test
  void aFractionOfAYearIsRoundedToWholeMonths() {
    // PostgreSQL: 1.1 years is 13 months (13.2 rounded), with no days spilled from the fraction
    assertThat(text(1186, "1.1 years")).isEqualTo(duration(13, 0, 0, 0));
    assertThat(text(1186, "P1.1Y")).isEqualTo(duration(13, 0, 0, 0));
    assertThat(text(1186, "1.5 years")).isEqualTo(duration(18, 0, 0, 0));
    // a fraction of a month still spills into days of 30
    assertThat(text(1186, "1.5 months")).isEqualTo(duration(1, 15, 0, 0));
  }

  // Regression: #11960
  @Test
  void jsonbReadsLikeJson() {
    assertThat(((JSONObject) text(3802, "{\"a\": 1}")).getInt("a")).isEqualTo(1);
    final byte[] json = "{\"a\": 1}".getBytes(StandardCharsets.UTF_8);
    final byte[] versioned = ByteBuffer.allocate(json.length + 1).put((byte) 1).put(json).array();
    assertThat(((JSONObject) binary(3802, versioned)).getInt("a")).isEqualTo(1);
  }

  /*
   * Issue #10934: {@link PostgresPortal#bindFrom} was documented as leaving {@code fullResultSet} at its fresh default, while it
   * hands every portal bound from a statement the answer PARSE precomputed (SHOW, system and catalog queries): one list, shared
   * by every portal of the statement. The sharing is what makes those answers work, so it stays, documented, and the shared
   * lists are now handed out read-only: a portal that wrote into one in place would change the answer of all the others.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10934
  @Test
  void portalsBoundFromOneStatementShareItsPrecomputedAnswerReadOnly() {
    final PostgresPortal template = new PostgresPortal("SHOW TRANSACTION ISOLATION LEVEL", "sql");
    template.executed = true;
    template.fullResultSet = new ArrayList<>(List.of(new ResultInternal(Map.of("transaction_isolation", "read committed"))));
    template.cachedResultSet = new ArrayList<>(template.fullResultSet);

    final PostgresPortal first = PostgresPortal.bindFrom(template);
    final PostgresPortal second = PostgresPortal.bindFrom(template);

    // THE PRECOMPUTED ANSWER IS INHERITED, NOT LEFT AT ITS DEFAULT: AN EXECUTE OF A BOUND PORTAL SERVES IT
    assertThat(first.executed).isTrue();
    assertThat(first.fullResultSet).containsExactlyElementsOf(template.fullResultSet);
    assertThat(second.fullResultSet).containsExactlyElementsOf(template.fullResultSet);
    assertThat(first.cachedResultSet).containsExactlyElementsOf(template.cachedResultSet);
    assertThat(first.statement).isSameAs(template);

    // ...AND READ-ONLY: NO PORTAL CAN CHANGE WHAT THE OTHERS OF THE STATEMENT ANSWER
    final Result extra = new ResultInternal(Map.of("transaction_isolation", "serializable"));
    assertThatThrownBy(() -> first.fullResultSet.add(extra)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> first.fullResultSet.clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> first.cachedResultSet.set(0, extra)).isInstanceOf(UnsupportedOperationException.class);
    assertThat(second.fullResultSet).hasSize(1);
    assertThat(template.fullResultSet).hasSize(1);

    // a portal bound from a portal bound from the template keeps the same view, not a view of a view
    assertThat(PostgresPortal.bindFrom(first).fullResultSet).isSameAs(first.fullResultSet);
  }

  // Regression: #10934
  @Test
  void theReplacementOfAnInheritedAnswerIsThePortalsOwn() {
    final PostgresPortal template = new PostgresPortal("SELECT 1", "sql");
    template.fullResultSet = new ArrayList<>(List.of(new ResultInternal(Map.of("a", 1))));

    final PostgresPortal first = PostgresPortal.bindFrom(template);
    final PostgresPortal second = PostgresPortal.bindFrom(template);
    // AN EXECUTE THAT RE-RUNS THE STATEMENT REPLACES THE WHOLE FIELD (PostgresNetworkExecutor.executeCommand)
    first.fullResultSet = List.of(new ResultInternal(Map.of("a", 2)));

    assertThat(second.fullResultSet).singleElement().satisfies(r -> assertThat((Integer) r.getProperty("a")).isEqualTo(1));
    assertThat(template.fullResultSet).singleElement().satisfies(r -> assertThat((Integer) r.getProperty("a")).isEqualTo(1));
  }

  // Regression: #10934
  @Test
  void aStatementWithoutAPrecomputedAnswerBindsPortalsWithoutOne() {
    final PostgresPortal portal = PostgresPortal.bindFrom(new PostgresPortal("SELECT FROM V", "sql"));
    assertThat(portal.fullResultSet).isNull();
    assertThat(portal.cachedResultSet).isNull();
    assertThat(portal.executed).isFalse();
  }

  /*
   * The count of {@code $N} placeholders a Parse derives from its text (issue #11018). Since a Bind with the wrong number of values
   * is refused, a placeholder counted where the SQL lexer sees none would refuse a correct Bind.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11018
  @Test
  void countsTheHighestPlaceholderNotHowManyThereAre() {
    assertThat(highestParameterPlaceholder("SELECT 1")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT $3, $1, $3")).isEqualTo(3);
    assertThat(highestParameterPlaceholder("SELECT * FROM t WHERE a=$1 AND b IN ($2,$12)")).isEqualTo(12);
  }

  // Regression: #11018
  @Test
  void ignoresPlaceholdersInsideQuotedTextAndComments() {
    assertThat(highestParameterPlaceholder("SELECT '$5' FROM t WHERE a = $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT \"$5\" FROM t WHERE a = $2")).isEqualTo(2);
    assertThat(highestParameterPlaceholder("SELECT `$5` FROM t WHERE a = $2")).isEqualTo(2);
    assertThat(highestParameterPlaceholder("SELECT 'it\\'s $9' FROM t WHERE a = $1")).as("an escaped quote does not end the string")
        .isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT 'a''$9' FROM t WHERE a = $1")).as("a doubled quote does not end it either")
        .isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT a /* $9 */ FROM t WHERE a = $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT a -- $9\nFROM t WHERE a = $1")).isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT a /* $9 unterminated")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT '$9 unterminated")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $$ $9 $$ FROM t WHERE a = $1")).as("a dollar-quoted literal").isEqualTo(1);
    assertThat(highestParameterPlaceholder("SELECT $tag$ $9 $tag$ FROM t WHERE a = $2")).isEqualTo(2);
    assertThat(highestParameterPlaceholder("SELECT $$ $9 unterminated")).isEqualTo(9);
  }

  // Regression: #11018
  @Test
  void aDollarNameIsNotAPlaceholder() {
    assertThat(highestParameterPlaceholder("SELECT $parent, $current FROM t")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $1abc FROM t")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT $1_x FROM t")).isEqualTo(0);
    assertThat(highestParameterPlaceholder("SELECT 5 - $2")).isEqualTo(2);
  }

  // Regression: #11018
  @Test
  void aPositionPastAnyBindIsReportedNotOverflowed() {
    assertThat(highestParameterPlaceholder("SELECT $70000")).isGreaterThan(PostgresNetworkExecutor.MAX_STATEMENT_PARAMETERS);
    assertThat(highestParameterPlaceholder("SELECT $99999999999999999999")).isEqualTo(Integer.MAX_VALUE);
  }
}
