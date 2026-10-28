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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10870: the text-format array parser dropped an empty-string element, read the unquoted SQL NULL as
 * the four-character string "NULL" (and failed an int4[] holding one), and read any untyped parameter starting and ending with a
 * brace - a JSON document included - as a list. The server could not read back the array literal it writes itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class Issue10870ArrayTextLiteralTest {

  private static Object text(final PostgresType type, final String literal) {
    return PostgresType.deserialize(type.code, 0, literal.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  private static Object untyped(final String literal) {
    return PostgresType.deserialize(0, 0, literal.getBytes(DatabaseFactory.getDefaultCharset()));
  }

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

  @Test
  void aNullElementOfANumericArrayIsNullNotAParseFailure() {
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "{1,NULL,3}")).containsExactly(1, null, 3);
    assertThat((List<Object>) text(PostgresType.ARRAY_LONG, "{ 1 , NULL , 3 }")).containsExactly(1L, null, 3L);
    assertThat((List<Object>) text(PostgresType.ARRAY_DOUBLE, "{1.5,NULL}")).containsExactly(1.5d, null);
  }

  @Test
  void whitespaceQuotingAndEscapesFollowPostgres() {
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{ a b , \"  c  \" ,d\\ }")).containsExactly("a b", "  c  ", "d ");
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"a,b\",c}")).containsExactly("a,b", "c");
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "{\"x\\\"y\",\"C:\\\\temp\",\"{}\"}")).containsExactly("x\"y", "C:\\temp",
        "{}");
  }

  @Test
  void booleanElementsAcceptPostgresSpellings() {
    // PostgreSQL writes a bool[] as {t,f}: Boolean.parseBoolean read "t" as false
    assertThat((List<Object>) text(PostgresType.ARRAY_BOOLEAN, "{t,f,true,false,NULL}")).containsExactly(true, false, true, false, null);
  }

  @Test
  void multidimensionalLiteralsAreReadFlatAsTheBinaryFormatReadsThem() {
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "{{1,2},{3,4}}")).containsExactly(1, 2, 3, 4);
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "[1:2][1:1]={{1},{2}}")).containsExactly(1, 2);
  }

  @Test
  void theLeniencyTheProtocolAlwaysHadIsKept() {
    assertThat((List<Object>) text(PostgresType.ARRAY_TEXT, "")).isEmpty();
    assertThat((List<Object>) text(PostgresType.ARRAY_INT, "1,NULL,3")).containsExactly(1, null, 3);
  }

  @Test
  void malformedLiteralsAreRefusedNotReinterpreted() {
    for (final String malformed : List.of("{a,,b}", "{a", "a,,b", "{\"a\"b}", "{a}x", "{{1,2},{3}}", "{{1},2}", "{1,{2}}",
        "{a\"b}"))
      assertThatThrownBy(() -> text(PostgresType.ARRAY_TEXT, malformed)).as(malformed).isInstanceOf(PostgresProtocolException.class)
          .hasMessageContaining("malformed array literal");
  }

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
    values.put(PostgresType.ARRAY_CHAR, Arrays.asList('a', null, ',', '"', '\\', '{'));
    final List<Object> strings = Arrays.asList("a", "", null, "NULL", "b,c", "{d}", "e\"f", "g\\h", " i ");
    values.put(PostgresType.ARRAY_TEXT, strings);
    values.put(PostgresType.ARRAY_VARCHAR, strings);
    values.put(PostgresType.ARRAY_BPCHAR, strings);
    values.put(PostgresType.ARRAY_JSON, Arrays.asList("{\"k\":1}", null, "[1,\"a,b\"]"));
    values.put(PostgresType.ARRAY_BYTEA, Arrays.asList(new byte[] { 1, 2, (byte) 0xff }, null, new byte[0]));
    values.put(PostgresType.ARRAY_DATE,
        Arrays.asList(Date.from(LocalDate.of(2024, 2, 29).atStartOfDay(ZoneOffset.UTC).toInstant()), null));
    values.put(PostgresType.ARRAY_TIMESTAMP, Arrays.asList(LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123456000), null));

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

  /** Byte arrays compare by content and dates by instant, so the comparison is on the value and not on the Java identity. */
  private static List<Object> normalize(final Object list) {
    final List<Object> normalized = new ArrayList<>();
    for (final Object element : (List<?>) list)
      normalized.add(switch (element) {
        case byte[] bytes -> Arrays.toString(bytes);
        case Date date -> date.getTime();
        case null -> null;
        default -> element;
      });
    return normalized;
  }
}
