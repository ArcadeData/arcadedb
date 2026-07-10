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
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for PostgresType enum covering type detection, serialization, and deserialization.
 */
class PostgresTypeTest {

  // ==================== Type Detection Tests ====================

  @Test
  void getTypeForValueNull() {
    assertThat(PostgresType.getTypeForValue(null)).isEqualTo(PostgresType.VARCHAR);
  }

  @Test
  void getTypeForValueFloat() {
    assertThat(PostgresType.getTypeForValue(1.5f)).isEqualTo(PostgresType.REAL);
    assertThat(PostgresType.getTypeForValue(Float.valueOf(2.5f))).isEqualTo(PostgresType.REAL);
  }

  @Test
  void getTypeForValueDouble() {
    assertThat(PostgresType.getTypeForValue(1.5d)).isEqualTo(PostgresType.DOUBLE);
    assertThat(PostgresType.getTypeForValue(Double.valueOf(2.5d))).isEqualTo(PostgresType.DOUBLE);
  }

  @Test
  void getTypeForValueInteger() {
    assertThat(PostgresType.getTypeForValue(42)).isEqualTo(PostgresType.INTEGER);
    assertThat(PostgresType.getTypeForValue(Integer.valueOf(42))).isEqualTo(PostgresType.INTEGER);
  }

  @Test
  void getTypeForValueShort() {
    assertThat(PostgresType.getTypeForValue((short) 10)).isEqualTo(PostgresType.INTEGER);
    assertThat(PostgresType.getTypeForValue(Short.valueOf((short) 10))).isEqualTo(PostgresType.INTEGER);
  }

  @Test
  void getTypeForValueByte() {
    assertThat(PostgresType.getTypeForValue((byte) 5)).isEqualTo(PostgresType.INTEGER);
    assertThat(PostgresType.getTypeForValue(Byte.valueOf((byte) 5))).isEqualTo(PostgresType.INTEGER);
  }

  @Test
  void getTypeForValueLong() {
    assertThat(PostgresType.getTypeForValue(100L)).isEqualTo(PostgresType.LONG);
    assertThat(PostgresType.getTypeForValue(Long.valueOf(100L))).isEqualTo(PostgresType.LONG);
  }

  @Test
  void getTypeForValueBoolean() {
    assertThat(PostgresType.getTypeForValue(true)).isEqualTo(PostgresType.BOOLEAN);
    assertThat(PostgresType.getTypeForValue(Boolean.FALSE)).isEqualTo(PostgresType.BOOLEAN);
  }

  @Test
  void getTypeForValueString() {
    assertThat(PostgresType.getTypeForValue("hello")).isEqualTo(PostgresType.VARCHAR);
    assertThat(PostgresType.getTypeForValue("")).isEqualTo(PostgresType.VARCHAR);
  }

  @Test
  void getTypeForValueCharacter() {
    assertThat(PostgresType.getTypeForValue('x')).isEqualTo(PostgresType.CHAR);
    assertThat(PostgresType.getTypeForValue(Character.valueOf('y'))).isEqualTo(PostgresType.CHAR);
  }

  @Test
  void getTypeForValueJSONObject() {
    assertThat(PostgresType.getTypeForValue(new JSONObject())).isEqualTo(PostgresType.JSON);
    assertThat(PostgresType.getTypeForValue(new JSONObject("{\"key\":\"value\"}"))).isEqualTo(PostgresType.JSON);
  }

  @Test
  void getTypeForValueMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    assertThat(PostgresType.getTypeForValue(map)).isEqualTo(PostgresType.JSON);
    assertThat(PostgresType.getTypeForValue(new HashMap<>())).isEqualTo(PostgresType.JSON);
  }

  @Test
  void getTypeForValueDate() {
    assertThat(PostgresType.getTypeForValue(new Date())).isEqualTo(PostgresType.DATE);
  }

  @Test
  void getTypeForValueLocalDateTime() {
    assertThat(PostgresType.getTypeForValue(LocalDateTime.now())).isEqualTo(PostgresType.TIMESTAMP);
  }

  // ==================== Collection Type Detection Tests ====================

  @Test
  void getTypeForValueCollectionOfIntegers() {
    List<Integer> list = Arrays.asList(1, 2, 3);
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getTypeForValueCollectionOfLongs() {
    List<Long> list = Arrays.asList(1L, 2L, 3L);
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_LONG);
  }

  @Test
  void getTypeForValueCollectionOfFloats() {
    List<Float> list = Arrays.asList(1.0f, 2.0f, 3.0f);
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_REAL);
  }

  @Test
  void getTypeForValueCollectionOfDoubles() {
    List<Double> list = Arrays.asList(1.0d, 2.0d, 3.0d);
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_DOUBLE);
  }

  @Test
  void getTypeForValueCollectionOfBooleans() {
    List<Boolean> list = Arrays.asList(true, false, true);
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_BOOLEAN);
  }

  @Test
  void getTypeForValueCollectionOfStrings() {
    List<String> list = Arrays.asList("a", "b", "c");
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getTypeForValueCollectionOfJSONObjects() {
    List<JSONObject> list = Arrays.asList(new JSONObject(), new JSONObject());
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_JSON);
  }

  @Test
  void getTypeForValueCollectionOfMaps() {
    List<Map<String, Object>> list = Arrays.asList(new HashMap<>(), new HashMap<>());
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_JSON);
  }

  @Test
  void getTypeForValueEmptyCollection() {
    assertThat(PostgresType.getTypeForValue(new ArrayList<>())).isEqualTo(PostgresType.ARRAY_TEXT);
    assertThat(PostgresType.getTypeForValue(List.of())).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getTypeForValueCollectionWithNullsFirst() {
    List<Object> list = new ArrayList<>();
    list.add(null);
    list.add(42);
    assertThat(PostgresType.getTypeForValue(list)).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getTypeForValueSet() {
    Set<Integer> set = new HashSet<>(Arrays.asList(1, 2, 3));
    assertThat(PostgresType.getTypeForValue(set)).isEqualTo(PostgresType.ARRAY_INT);
  }

  // ==================== Iterable Type Detection Tests ====================

  @Test
  void getTypeForValueIterableOfIntegers() {
    Iterable<Integer> iterable = () -> Arrays.asList(1, 2, 3).iterator();
    assertThat(PostgresType.getTypeForValue(iterable)).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getTypeForValueIterableEmpty() {
    Iterable<Integer> iterable = Collections::emptyIterator;
    assertThat(PostgresType.getTypeForValue(iterable)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getTypeForValueIterableWithNullsFirst() {
    List<Object> source = new ArrayList<>();
    source.add(null);
    source.add(null);
    source.add("text");
    Iterable<Object> iterable = source::iterator;
    assertThat(PostgresType.getTypeForValue(iterable)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  // ==================== Iterator Type Detection Tests ====================

  @Test
  void getTypeForValueIteratorOfIntegers() {
    Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
    assertThat(PostgresType.getTypeForValue(iterator)).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getTypeForValueIteratorOfStrings() {
    Iterator<String> iterator = Arrays.asList("a", "b", "c").iterator();
    assertThat(PostgresType.getTypeForValue(iterator)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getTypeForValueIteratorEmpty() {
    Iterator<Object> iterator = Collections.emptyIterator();
    assertThat(PostgresType.getTypeForValue(iterator)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getTypeForValueIteratorWithNullsFirst() {
    List<Object> source = new ArrayList<>();
    source.add(null);
    source.add(100L);
    Iterator<Object> iterator = source.iterator();
    assertThat(PostgresType.getTypeForValue(iterator)).isEqualTo(PostgresType.ARRAY_LONG);
  }

  @Test
  void getTypeForValueIteratorAllNulls() {
    List<Object> source = new ArrayList<>();
    source.add(null);
    source.add(null);
    Iterator<Object> iterator = source.iterator();
    assertThat(PostgresType.getTypeForValue(iterator)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  // ==================== Primitive Array Type Detection Tests ====================

  @Test
  void getTypeForValueByteArray() {
    assertThat(PostgresType.getTypeForValue(new byte[]{1, 2, 3})).isEqualTo(PostgresType.ARRAY_CHAR);
  }

  @Test
  void getTypeForValueIntArray() {
    assertThat(PostgresType.getTypeForValue(new int[]{1, 2, 3})).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getTypeForValueLongArray() {
    assertThat(PostgresType.getTypeForValue(new long[]{1L, 2L, 3L})).isEqualTo(PostgresType.ARRAY_LONG);
  }

  @Test
  void getTypeForValueDoubleArray() {
    assertThat(PostgresType.getTypeForValue(new double[]{1.0, 2.0, 3.0})).isEqualTo(PostgresType.ARRAY_DOUBLE);
  }

  @Test
  void getTypeForValueFloatArray() {
    assertThat(PostgresType.getTypeForValue(new float[]{1.0f, 2.0f, 3.0f})).isEqualTo(PostgresType.ARRAY_REAL);
  }

  @Test
  void getTypeForValueBooleanArray() {
    assertThat(PostgresType.getTypeForValue(new boolean[]{true, false})).isEqualTo(PostgresType.ARRAY_BOOLEAN);
  }

  @Test
  void getTypeForValueCharArray() {
    assertThat(PostgresType.getTypeForValue(new char[]{'a', 'b', 'c'})).isEqualTo(PostgresType.ARRAY_CHAR);
  }

  @Test
  void getTypeForValueStringArray() {
    assertThat(PostgresType.getTypeForValue(new String[]{"a", "b", "c"})).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  // ==================== Arcade Type Mapping Tests ====================

  @Test
  void getTypeFromArcadeNull() {
    assertThat(PostgresType.getTypeFromArcade(null)).isEqualTo(PostgresType.VARCHAR);
  }

  @Test
  void getTypeFromArcadeBoolean() {
    assertThat(PostgresType.getTypeFromArcade(Type.BOOLEAN)).isEqualTo(PostgresType.BOOLEAN);
  }

  @Test
  void getTypeFromArcadeInteger() {
    assertThat(PostgresType.getTypeFromArcade(Type.INTEGER)).isEqualTo(PostgresType.INTEGER);
  }

  @Test
  void getTypeFromArcadeShort() {
    assertThat(PostgresType.getTypeFromArcade(Type.SHORT)).isEqualTo(PostgresType.SMALLINT);
  }

  @Test
  void getTypeFromArcadeLong() {
    assertThat(PostgresType.getTypeFromArcade(Type.LONG)).isEqualTo(PostgresType.LONG);
  }

  @Test
  void getTypeFromArcadeFloat() {
    assertThat(PostgresType.getTypeFromArcade(Type.FLOAT)).isEqualTo(PostgresType.REAL);
  }

  @Test
  void getTypeFromArcadeDouble() {
    assertThat(PostgresType.getTypeFromArcade(Type.DOUBLE)).isEqualTo(PostgresType.DOUBLE);
  }

  @Test
  void getTypeFromArcadeByte() {
    assertThat(PostgresType.getTypeFromArcade(Type.BYTE)).isEqualTo(PostgresType.SMALLINT);
  }

  @Test
  void getTypeFromArcadeString() {
    assertThat(PostgresType.getTypeFromArcade(Type.STRING)).isEqualTo(PostgresType.VARCHAR);
  }

  @Test
  void getTypeFromArcadeDatetime() {
    assertThat(PostgresType.getTypeFromArcade(Type.DATETIME)).isEqualTo(PostgresType.TIMESTAMP);
  }

  @Test
  void getTypeFromArcadeDate() {
    assertThat(PostgresType.getTypeFromArcade(Type.DATE)).isEqualTo(PostgresType.DATE);
  }

  @Test
  void getTypeFromArcadeBinary() {
    assertThat(PostgresType.getTypeFromArcade(Type.BINARY)).isEqualTo(PostgresType.VARCHAR);
  }

  @Test
  void getTypeFromArcadeList() {
    assertThat(PostgresType.getTypeFromArcade(Type.LIST)).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getTypeFromArcadeMap() {
    assertThat(PostgresType.getTypeFromArcade(Type.MAP)).isEqualTo(PostgresType.JSON);
  }

  @Test
  void getTypeFromArcadeEmbedded() {
    assertThat(PostgresType.getTypeFromArcade(Type.EMBEDDED)).isEqualTo(PostgresType.JSON);
  }

  @Test
  void getTypeFromArcadeLink() {
    assertThat(PostgresType.getTypeFromArcade(Type.LINK)).isEqualTo(PostgresType.VARCHAR);
  }

  @Test
  void getTypeFromArcadeDecimal() {
    assertThat(PostgresType.getTypeFromArcade(Type.DECIMAL)).isEqualTo(PostgresType.DOUBLE);
  }

  // ==================== Text Deserialization Tests ====================

  @Test
  void deserializeTextUnspecifiedPlainString() {
    byte[] data = "hello".getBytes();
    Object result = PostgresType.deserialize(0, 0, data);
    assertThat(result).isEqualTo("hello");
  }

  @Test
  void deserializeTextUnspecifiedArrayFormat() {
    byte[] data = "{1,2,3}".getBytes();
    Object result = PostgresType.deserialize(0, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<String> list = (ArrayList<String>) result;
    assertThat(list).containsExactly("1", "2", "3");
  }

  @Test
  void deserializeTextInteger() {
    byte[] data = "42".getBytes();
    Object result = PostgresType.deserialize(PostgresType.INTEGER.code, 0, data);
    assertThat(result).isEqualTo(42);
  }

  @Test
  void deserializeTextLong() {
    byte[] data = "9223372036854775807".getBytes();
    Object result = PostgresType.deserialize(PostgresType.LONG.code, 0, data);
    assertThat(result).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void deserializeTextSmallint() {
    byte[] data = "32767".getBytes();
    Object result = PostgresType.deserialize(PostgresType.SMALLINT.code, 0, data);
    assertThat(result).isEqualTo(Short.MAX_VALUE);
  }

  @Test
  void deserializeTextReal() {
    byte[] data = "3.14".getBytes();
    Object result = PostgresType.deserialize(PostgresType.REAL.code, 0, data);
    assertThat(result).isEqualTo(3.14f);
  }

  @Test
  void deserializeTextDouble() {
    byte[] data = "3.14159265359".getBytes();
    Object result = PostgresType.deserialize(PostgresType.DOUBLE.code, 0, data);
    assertThat(result).isEqualTo(3.14159265359d);
  }

  @Test
  void deserializeTextBoolean() {
    // Canonical Postgres text format
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "t".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "f".getBytes())).isEqualTo(false);
    // Legacy / verbose forms also accepted
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "true".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "TRUE".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "false".getBytes())).isEqualTo(false);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "1".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "0".getBytes())).isEqualTo(false);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "yes".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "no".getBytes())).isEqualTo(false);
  }

  @Test
  void deserializeTextDateIsoFormat() {
    // After advertising DATE columns natively, clients send dates as "YYYY-MM-DD" text
    Object result = PostgresType.deserialize(PostgresType.DATE.code, 0, "2024-08-22".getBytes());
    assertThat(result).isInstanceOf(Date.class);
    final long expectedMillis = LocalDate.of(2024, 8, 22)
        .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    assertThat(((Date) result).getTime()).isEqualTo(expectedMillis);
  }

  @Test
  void deserializeBinaryTimestamp() {
    // PostgreSQL binary TIMESTAMP: int64 microseconds since 2000-01-01T00:00:00Z
    final long microsSince2000 = 777600000000L; // 2000-01-01 + 9 days
    ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(microsSince2000);
    Object result = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 1, buffer.array());
    assertThat(result).isInstanceOf(LocalDateTime.class);
    assertThat((LocalDateTime) result).isEqualTo(LocalDateTime.of(2000, 1, 10, 0, 0, 0));
  }

  @Test
  void deserializeTextChar() {
    byte[] data = "x".getBytes();
    Object result = PostgresType.deserialize(PostgresType.CHAR.code, 0, data);
    assertThat(result).isEqualTo('x');
  }

  @Test
  void deserializeTextVarchar() {
    byte[] data = "hello world".getBytes();
    Object result = PostgresType.deserialize(PostgresType.VARCHAR.code, 0, data);
    assertThat(result).isEqualTo("hello world");
  }

  @Test
  void deserializeTextOid25AsString() {
    // OID 25 is PostgreSQL TEXT type - must be treated the same as VARCHAR
    byte[] data = "hello world".getBytes();
    Object result = PostgresType.deserialize(PostgresType.TEXT.code, 0, data);
    assertThat(result).isEqualTo("hello world");
  }

  @Test
  void deserializeBinaryOid25AsString() {
    // OID 25 in binary format - Npgsql 10 sends strings as text OID in binary format
    byte[] data = "Alice".getBytes();
    Object result = PostgresType.deserialize(PostgresType.TEXT.code, 1, data);
    assertThat(result).isEqualTo("Alice");
  }

  @Test
  void textTypeCodeIs25() {
    assertThat(PostgresType.TEXT.code).isEqualTo(25);
  }

  @Test
  void deserializeTextText() {
    // Issue #4036: Npgsql sends string parameters with TEXT (OID 25) by default.
    // Make sure the server accepts text format text deserialization.
    byte[] data = "Keanu Reeves".getBytes();
    Object result = PostgresType.deserialize(PostgresType.TEXT.code, 0, data);
    assertThat(result).isEqualTo("Keanu Reeves");
  }

  @Test
  void deserializeTextBpchar() {
    // BPCHAR (OID 1042) is the blank-padded char type. Treat the same as VARCHAR.
    byte[] data = "value".getBytes();
    Object result = PostgresType.deserialize(PostgresType.BPCHAR.code, 0, data);
    assertThat(result).isEqualTo("value");
  }

  @Test
  void deserializeTextJson() {
    byte[] data = "{\"key\":\"value\"}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.JSON.code, 0, data);
    assertThat(result).isInstanceOf(JSONObject.class);
    assertThat(((JSONObject) result).getString("key")).isEqualTo("value");
  }

  @Test
  void deserializeTextArrayInt() {
    byte[] data = "{1,2,3}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_INT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Integer> list = (ArrayList<Integer>) result;
    assertThat(list).containsExactly(1, 2, 3);
  }

  @Test
  void deserializeTextArrayLong() {
    byte[] data = "{100,200,300}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_LONG.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Long> list = (ArrayList<Long>) result;
    assertThat(list).containsExactly(100L, 200L, 300L);
  }

  @Test
  void deserializeTextArrayDouble() {
    byte[] data = "{1.1,2.2,3.3}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_DOUBLE.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Double> list = (ArrayList<Double>) result;
    assertThat(list).containsExactly(1.1d, 2.2d, 3.3d);
  }

  @Test
  void deserializeTextArrayReal() {
    byte[] data = "{1.5,2.5,3.5}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_REAL.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Float> list = (ArrayList<Float>) result;
    assertThat(list).containsExactly(1.5f, 2.5f, 3.5f);
  }

  @Test
  void deserializeTextArrayText() {
    byte[] data = "{\"hello\",\"world\"}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<String> list = (ArrayList<String>) result;
    assertThat(list).containsExactly("hello", "world");
  }

  @Test
  void deserializeTextArrayBoolean() {
    byte[] data = "{true,false,true}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_BOOLEAN.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Boolean> list = (ArrayList<Boolean>) result;
    assertThat(list).containsExactly(true, false, true);
  }

  @Test
  void deserializeTextArrayChar() {
    byte[] data = "{a,b,c}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_CHAR.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Character> list = (ArrayList<Character>) result;
    assertThat(list).containsExactly('a', 'b', 'c');
  }

  @Test
  void deserializeTextUnsupportedType() {
    byte[] data = "test".getBytes();
    assertThatThrownBy(() -> PostgresType.deserialize(99999, 0, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("not supported");
  }

  @Test
  void deserializeInvalidFormatCode() {
    byte[] data = "test".getBytes();
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.VARCHAR.code, 2, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("Invalid format code");
  }

  // ==================== Binary Deserialization Tests ====================

  @Test
  void deserializeBinarySmallint() {
    ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
    buffer.putShort((short) 12345);
    Object result = PostgresType.deserialize(PostgresType.SMALLINT.code, 1, buffer.array());
    assertThat(result).isEqualTo((short) 12345);
  }

  @Test
  void deserializeBinaryInteger() {
    ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(123456789);
    Object result = PostgresType.deserialize(PostgresType.INTEGER.code, 1, buffer.array());
    assertThat(result).isEqualTo(123456789);
  }

  @Test
  void deserializeBinaryLong() {
    ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(9876543210L);
    Object result = PostgresType.deserialize(PostgresType.LONG.code, 1, buffer.array());
    assertThat(result).isEqualTo(9876543210L);
  }

  @Test
  void deserializeBinaryReal() {
    ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    buffer.putFloat(3.14f);
    Object result = PostgresType.deserialize(PostgresType.REAL.code, 1, buffer.array());
    assertThat(result).isEqualTo(3.14f);
  }

  @Test
  void deserializeBinaryDouble() {
    ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    buffer.putDouble(3.14159265359);
    Object result = PostgresType.deserialize(PostgresType.DOUBLE.code, 1, buffer.array());
    assertThat(result).isEqualTo(3.14159265359);
  }

  @Test
  void deserializeBinaryBoolean() {
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 1, new byte[]{1})).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 1, new byte[]{0})).isEqualTo(false);
  }

  @Test
  void deserializeBinaryChar() {
    // PostgreSQL "char" (OID 18) is a single byte on the wire.
    Object result = PostgresType.deserialize(PostgresType.CHAR.code, 1, new byte[]{(byte) 'X'});
    assertThat(result).isEqualTo('X');
  }

  @Test
  void serializeAsBinaryCharRoundTrip() {
    final Binary buffer = new Binary();
    PostgresType.CHAR.serializeAsBinary(PostgresType.CHAR, buffer, 'Z');
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(1);
    final byte b = buffer.getByte();
    final Object decoded = PostgresType.deserialize(PostgresType.CHAR.code, 1, new byte[]{b});
    assertThat(decoded).isEqualTo('Z');
  }

  @Test
  void deserializeBinaryVarchar() {
    byte[] data = "hello binary".getBytes();
    Object result = PostgresType.deserialize(PostgresType.VARCHAR.code, 1, data);
    assertThat(result).isEqualTo("hello binary");
  }

  @Test
  void deserializeBinaryText() {
    // Issue #4036: Npgsql may send TEXT (OID 25) parameters in binary format.
    byte[] data = "Keanu Reeves".getBytes();
    Object result = PostgresType.deserialize(PostgresType.TEXT.code, 1, data);
    assertThat(result).isEqualTo("Keanu Reeves");
  }

  @Test
  void deserializeBinaryBpchar() {
    byte[] data = "value".getBytes();
    Object result = PostgresType.deserialize(PostgresType.BPCHAR.code, 1, data);
    assertThat(result).isEqualTo("value");
  }

  @Test
  void deserializeBinaryDate() {
    // PostgreSQL binary DATE: int32 days since 2000-01-01 UTC
    final int daysSince2000 = 9000; // 2000-01-01 + 9000 days = 2024-08-22
    ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(daysSince2000);
    Object result = PostgresType.deserialize(PostgresType.DATE.code, 1, buffer.array());
    assertThat(result).isInstanceOf(Date.class);
    final long expectedMillis = LocalDate.of(2000, 1, 1).plusDays(daysSince2000)
        .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    assertThat(((Date) result).getTime()).isEqualTo(expectedMillis);
  }

  @Test
  void deserializeBinaryJson() {
    String jsonStr = "{\"key\":\"value\"}";
    ByteBuffer buffer = ByteBuffer.allocate(4 + jsonStr.length()).order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(jsonStr.length());
    buffer.put(jsonStr.getBytes());
    Object result = PostgresType.deserialize(PostgresType.JSON.code, 1, buffer.array());
    assertThat(result).isInstanceOf(JSONObject.class);
  }

  @Test
  void deserializeBinaryArrayText() {
    // PostgreSQL binary format for text[] {'foo','bar'}:
    // ndim=1, hasnull=0, elemOid=25 (text), dim=2, lb=1, then two elements
    ByteBuffer buf = ByteBuffer.allocate(256).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);        // ndim
    buf.putInt(0);        // hasnull
    buf.putInt(25);       // elemOid: text
    buf.putInt(2);        // dim size
    buf.putInt(1);        // lower bound
    byte[] foo = "foo".getBytes();
    buf.putInt(foo.length);
    buf.put(foo);
    byte[] bar = "bar".getBytes();
    buf.putInt(bar.length);
    buf.put(bar);
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<String> list = (ArrayList<String>) result;
    assertThat(list).containsExactly("foo", "bar");
  }

  @Test
  void deserializeBinaryArrayLong() {
    // PostgreSQL binary format for int8[] {100, 200, 300}
    ByteBuffer buf = ByteBuffer.allocate(256).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);        // ndim
    buf.putInt(0);        // hasnull
    buf.putInt(20);       // elemOid: int8
    buf.putInt(3);        // dim size
    buf.putInt(1);        // lower bound
    for (long v : new long[]{100L, 200L, 300L}) {
      buf.putInt(8);
      buf.putLong(v);
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    Object result = PostgresType.deserialize(PostgresType.ARRAY_LONG.code, 1, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Long> list = (ArrayList<Long>) result;
    assertThat(list).containsExactly(100L, 200L, 300L);
  }

  @Test
  void deserializeBinaryArrayInt() {
    // PostgreSQL binary format for int4[] {1, 2, 3}
    ByteBuffer buf = ByteBuffer.allocate(256).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);        // ndim
    buf.putInt(0);        // hasnull
    buf.putInt(23);       // elemOid: int4
    buf.putInt(3);        // dim size
    buf.putInt(1);        // lower bound
    for (int v : new int[]{1, 2, 3}) {
      buf.putInt(4);
      buf.putInt(v);
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    Object result = PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Integer> list = (ArrayList<Integer>) result;
    assertThat(list).containsExactly(1, 2, 3);
  }

  @Test
  void deserializeBinaryArrayEmpty() {
    // ndim=0 should return an empty list
    ByteBuffer buf = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(0);   // ndim
    buf.putInt(0);   // hasnull
    buf.putInt(25);  // elemOid: text
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, buf.array());
    assertThat(result).isInstanceOf(ArrayList.class);
    assertThat((ArrayList<?>) result).isEmpty();
  }

  @Test
  void deserializeBinaryArrayBool() {
    // bool[] {true, false, true}
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);   // ndim
    buf.putInt(0);   // hasnull
    buf.putInt(16);  // elemOid: bool
    buf.putInt(3);   // dim size
    buf.putInt(1);   // lower bound
    for (boolean v : new boolean[]{true, false, true}) {
      buf.putInt(1);
      buf.put((byte) (v ? 1 : 0));
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    @SuppressWarnings("unchecked")
    ArrayList<Boolean> list = (ArrayList<Boolean>) PostgresType.deserialize(PostgresType.ARRAY_BOOLEAN.code, 1, data);
    assertThat(list).containsExactly(true, false, true);
  }

  @Test
  void deserializeBinaryArrayInt2() {
    // int2[] (smallint) {10, 20, 30}
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);   // ndim
    buf.putInt(0);   // hasnull
    buf.putInt(21);  // elemOid: int2
    buf.putInt(3);   // dim size
    buf.putInt(1);   // lower bound
    for (short v : new short[]{10, 20, 30}) {
      buf.putInt(2);
      buf.putShort(v);
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    @SuppressWarnings("unchecked")
    ArrayList<Short> list = (ArrayList<Short>) PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data);
    assertThat(list).containsExactly((short) 10, (short) 20, (short) 30);
  }

  @Test
  void deserializeBinaryArrayFloat4() {
    // float4[] {1.5, 2.5, 3.5}
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);   // ndim
    buf.putInt(0);   // hasnull
    buf.putInt(700); // elemOid: float4
    buf.putInt(3);   // dim size
    buf.putInt(1);   // lower bound
    for (float v : new float[]{1.5f, 2.5f, 3.5f}) {
      buf.putInt(4);
      buf.putFloat(v);
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    @SuppressWarnings("unchecked")
    ArrayList<Float> list = (ArrayList<Float>) PostgresType.deserialize(PostgresType.ARRAY_REAL.code, 1, data);
    assertThat(list).containsExactly(1.5f, 2.5f, 3.5f);
  }

  @Test
  void deserializeBinaryArrayFloat8() {
    // float8[] {1.1, 2.2, 3.3}
    ByteBuffer buf = ByteBuffer.allocate(96).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);   // ndim
    buf.putInt(0);   // hasnull
    buf.putInt(701); // elemOid: float8
    buf.putInt(3);   // dim size
    buf.putInt(1);   // lower bound
    for (double v : new double[]{1.1, 2.2, 3.3}) {
      buf.putInt(8);
      buf.putDouble(v);
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    @SuppressWarnings("unchecked")
    ArrayList<Double> list = (ArrayList<Double>) PostgresType.deserialize(PostgresType.ARRAY_DOUBLE.code, 1, data);
    assertThat(list).containsExactly(1.1, 2.2, 3.3);
  }

  @Test
  void deserializeBinaryArray2D() {
    // int4[][] {{1, 2}, {3, 4}}: ndim=2, dims 2x2, four elements row-major.
    // Multi-dimensional arrays are flattened into a single List - the query engine consumes
    // the result as a flat collection for IN-parameter binding, so the dimensionality is
    // intentionally not preserved.
    ByteBuffer buf = ByteBuffer.allocate(96).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(2);   // ndim
    buf.putInt(0);   // hasnull
    buf.putInt(23);  // elemOid: int4
    buf.putInt(2);   // dim1 size
    buf.putInt(1);   // dim1 lb
    buf.putInt(2);   // dim2 size
    buf.putInt(1);   // dim2 lb
    for (int v : new int[]{1, 2, 3, 4}) {
      buf.putInt(4);
      buf.putInt(v);
    }
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    @SuppressWarnings("unchecked")
    ArrayList<Integer> list = (ArrayList<Integer>) PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data);
    assertThat(list).containsExactly(1, 2, 3, 4);
  }

  @Test
  void deserializeBinaryArrayRejectsNegativeDimension() {
    // ndim header with a negative dimension size must be rejected, not silently mis-read.
    ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);    // ndim
    buf.putInt(0);    // hasnull
    buf.putInt(25);   // elemOid: text
    buf.putInt(-1);   // negative dim size
    buf.putInt(1);    // lb
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("Negative array dimension");
  }

  @Test
  void deserializeBinaryArrayRejectsTooManyDimensions() {
    // PostgreSQL caps ndim at 6 (MAXDIM). Anything larger is rejected without allocating.
    ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(7);    // ndim > MAXDIM
    buf.putInt(0);    // hasnull
    buf.putInt(25);   // elemOid: text
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("Invalid array dimension count");
  }

  @Test
  void deserializeBinaryArrayRejectsOversizedElementCount() {
    // dim_size large enough that totalElements*4 exceeds remaining buffer bytes - rejected
    // before the result ArrayList is over-allocated.
    ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);                 // ndim
    buf.putInt(0);                 // hasnull
    buf.putInt(25);                // elemOid: text
    buf.putInt(Integer.MAX_VALUE); // absurd dim size
    buf.putInt(1);                 // lb
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("exceeds remaining buffer");
  }

  @Test
  void deserializeBinaryArrayRejectsOverflowAcrossDimensions() {
    // Two dimensions of ~65000 each: product overflows Integer.MAX_VALUE. Must be rejected
    // instead of silently wrapping to a small loop count (the original bug class).
    ByteBuffer buf = ByteBuffer.allocate(40).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(2);      // ndim
    buf.putInt(0);      // hasnull
    buf.putInt(23);     // elemOid: int4
    buf.putInt(65000);  // dim1 size
    buf.putInt(1);      // dim1 lb
    buf.putInt(65000);  // dim2 size: 65000 * 65000 = 4.225e9 > Integer.MAX_VALUE
    buf.putInt(1);      // dim2 lb
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("exceeds Integer.MAX_VALUE");
  }

  @Test
  void deserializeBinaryArrayWithNullElement() {
    // text[] {'foo', NULL, 'baz'} - null elements have elemLen == -1
    ByteBuffer buf = ByteBuffer.allocate(256).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1);   // ndim
    buf.putInt(1);   // hasnull
    buf.putInt(25);  // elemOid: text
    buf.putInt(3);   // dim size
    buf.putInt(1);   // lower bound
    byte[] foo = "foo".getBytes();
    buf.putInt(foo.length);
    buf.put(foo);
    buf.putInt(-1);  // null element
    byte[] baz = "baz".getBytes();
    buf.putInt(baz.length);
    buf.put(baz);
    byte[] data = Arrays.copyOf(buf.array(), buf.position());
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<String> list = (ArrayList<String>) result;
    assertThat(list).hasSize(3);
    assertThat(list.getFirst()).isEqualTo("foo");
    assertThat(list.get(1)).isNull();
    assertThat(list.get(2)).isEqualTo("baz");
  }

  @Test
  void deserializeBinaryUnsupportedType() {
    byte[] data = new byte[10];
    assertThatThrownBy(() -> PostgresType.deserialize(99999, 1, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("not supported");
  }

  // ==================== Array Parsing Edge Cases ====================

  @Test
  void parseArrayFromStringEmpty() {
    byte[] data = "{}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_INT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    assertThat((Collection<?>) result).isEmpty();
  }

  @Test
  void parseArrayFromStringNull() {
    byte[] data = "".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    assertThat((Collection<?>) result).isEmpty();
  }

  @Test
  void parseArrayFromStringWithQuotedStrings() {
    byte[] data = "{\"hello world\",\"foo bar\"}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<String> list = (ArrayList<String>) result;
    assertThat(list).containsExactly("hello world", "foo bar");
  }

  @Test
  void parseArrayFromStringWithMixedQuotedUnquoted() {
    byte[] data = "{simple,\"with spaces\",another}".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<String> list = (ArrayList<String>) result;
    assertThat(list).containsExactly("simple", "with spaces", "another");
  }

  @Test
  void parseArrayFromStringWithoutBraces() {
    byte[] data = "1,2,3".getBytes();
    Object result = PostgresType.deserialize(PostgresType.ARRAY_INT.code, 0, data);
    assertThat(result).isInstanceOf(ArrayList.class);
    @SuppressWarnings("unchecked")
    ArrayList<Integer> list = (ArrayList<Integer>) result;
    assertThat(list).containsExactly(1, 2, 3);
  }

  // ==================== Serialization Tests ====================

  @Test
  void serializeAsTextNull() {
    Binary buffer = new Binary();
    PostgresType.VARCHAR.serializeAsText(PostgresType.VARCHAR, buffer, null);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(-1);
  }

  @Test
  void serializeAsTextNullBoolean() {
    Binary buffer = new Binary();
    PostgresType.BOOLEAN.serializeAsText(PostgresType.BOOLEAN, buffer, null);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("0");
  }

  @Test
  void serializeAsTextString() {
    Binary buffer = new Binary();
    PostgresType.VARCHAR.serializeAsText(PostgresType.VARCHAR, buffer, "hello");
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("hello");
  }

  @Test
  void serializeAsTextInteger() {
    Binary buffer = new Binary();
    PostgresType.INTEGER.serializeAsText(PostgresType.INTEGER, buffer, 42);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("42");
  }

  @Test
  void serializeAsTextDate() {
    Binary buffer = new Binary();
    Date date = new Date(1716138311000L); // 2024-05-19 17:05:11 UTC
    PostgresType.DATE.serializeAsText(PostgresType.DATE, buffer, date);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    // DATE (OID 1082) must be serialized as "YYYY-MM-DD" only
    assertThat(result).matches("\\d{4}-\\d{2}-\\d{2}");
    assertThat(result).isEqualTo("2024-05-19");
  }

  @Test
  void serializeAsTextBoolean() {
    final Binary t = new Binary();
    PostgresType.BOOLEAN.serializeAsText(PostgresType.BOOLEAN, t, Boolean.TRUE);
    t.flip();
    final byte[] tData = new byte[t.getInt()];
    t.getByteBuffer().get(tData);
    assertThat(new String(tData)).isEqualTo("t");

    final Binary f = new Binary();
    PostgresType.BOOLEAN.serializeAsText(PostgresType.BOOLEAN, f, Boolean.FALSE);
    f.flip();
    final byte[] fData = new byte[f.getInt()];
    f.getByteBuffer().get(fData);
    assertThat(new String(fData)).isEqualTo("f");
  }

  @Test
  void serializeAsBinaryInteger() {
    Binary buffer = new Binary();
    PostgresType.INTEGER.serializeAsBinary(PostgresType.INTEGER, buffer, 12345);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(4);
    assertThat(buffer.getInt()).isEqualTo(12345);
  }

  @Test
  void serializeAsBinaryDouble() {
    Binary buffer = new Binary();
    PostgresType.DOUBLE.serializeAsBinary(PostgresType.DOUBLE, buffer, 3.14159d);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);
    assertThat(Double.longBitsToDouble(buffer.getLong())).isEqualTo(3.14159d);
  }

  @Test
  void serializeAsBinaryBoolean() {
    final Binary t = new Binary();
    PostgresType.BOOLEAN.serializeAsBinary(PostgresType.BOOLEAN, t, Boolean.TRUE);
    t.flip();
    assertThat(t.getInt()).isEqualTo(1);
    assertThat(t.getByte()).isEqualTo((byte) 1);

    final Binary f = new Binary();
    PostgresType.BOOLEAN.serializeAsBinary(PostgresType.BOOLEAN, f, Boolean.FALSE);
    f.flip();
    assertThat(f.getInt()).isEqualTo(1);
    assertThat(f.getByte()).isEqualTo((byte) 0);
  }

  @Test
  void serializeAsBinaryDateRoundTrip() {
    // Round-trip: binary serialize -> binary deserialize must preserve the date portion
    final LocalDate ld = LocalDate.of(2024, 8, 22);
    final Date input = Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());

    final Binary buffer = new Binary();
    PostgresType.DATE.serializeAsBinary(PostgresType.DATE, buffer, input);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(4);

    final byte[] bytes = new byte[4];
    buffer.getByteBuffer().get(bytes);
    final Object decoded = PostgresType.deserialize(PostgresType.DATE.code, 1, bytes);
    assertThat(decoded).isInstanceOf(Date.class);
    assertThat(((Date) decoded).toInstant().atZone(ZoneOffset.UTC).toLocalDate()).isEqualTo(ld);
  }

  @Test
  void serializeAsBinaryTimestampRoundTrip() {
    final LocalDateTime ldt = LocalDateTime.of(2024, 8, 22, 14, 30, 45, 123_456_000);
    final Binary buffer = new Binary();
    PostgresType.TIMESTAMP.serializeAsBinary(PostgresType.TIMESTAMP, buffer, ldt);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);

    final byte[] bytes = new byte[8];
    buffer.getByteBuffer().get(bytes);
    final Object decoded = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 1, bytes);
    assertThat(decoded).isEqualTo(ldt);
  }

  @Test
  void serializeAsTextLocalDateTime() {
    Binary buffer = new Binary();
    LocalDateTime ldt = LocalDateTime.of(2024, 5, 19, 17, 5, 11);
    PostgresType.DATE.serializeAsText(PostgresType.DATE, buffer, ldt);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    assertThat(result).startsWith("2024-05-19 17:05:11");
  }

  @Test
  void serializeAsTextJSONObject() {
    Binary buffer = new Binary();
    JSONObject json = new JSONObject("{\"key\":\"value\"}");
    PostgresType.JSON.serializeAsText(PostgresType.JSON, buffer, json);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).contains("\"key\"").contains("\"value\"");
  }

  @Test
  void serializeAsTextMap() {
    Binary buffer = new Binary();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    PostgresType.JSON.serializeAsText(PostgresType.JSON, buffer, map);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).contains("\"key\"").contains("\"value\"");
  }

  @Test
  void serializeAsTextCollectionOfIntegers() {
    Binary buffer = new Binary();
    List<Integer> list = Arrays.asList(1, 2, 3);
    PostgresType.ARRAY_INT.serializeAsText(PostgresType.ARRAY_INT, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1,2,3}");
  }

  @Test
  void serializeAsTextCollectionOfFloats() {
    Binary buffer = new Binary();
    List<Float> list = Arrays.asList(1.5f, 2.5f);
    PostgresType.ARRAY_REAL.serializeAsText(PostgresType.ARRAY_REAL, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1.5,2.5}");
  }

  @Test
  void serializeAsTextCollectionOfDoubles() {
    Binary buffer = new Binary();
    List<Double> list = Arrays.asList(1.5d, 2.5d);
    PostgresType.ARRAY_DOUBLE.serializeAsText(PostgresType.ARRAY_DOUBLE, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1.5,2.5}");
  }

  @Test
  void serializeAsTextCollectionOfBooleans() {
    Binary buffer = new Binary();
    List<Boolean> list = Arrays.asList(true, false);
    PostgresType.ARRAY_BOOLEAN.serializeAsText(PostgresType.ARRAY_BOOLEAN, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{true,false}");
  }

  @Test
  void serializeAsTextCollectionOfStrings() {
    Binary buffer = new Binary();
    List<String> list = Arrays.asList("hello", "world");
    PostgresType.ARRAY_TEXT.serializeAsText(PostgresType.ARRAY_TEXT, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{\"hello\",\"world\"}");
  }

  @Test
  void serializeAsTextCollectionOfCharacters() {
    Binary buffer = new Binary();
    List<Character> list = Arrays.asList('a', 'b');
    PostgresType.ARRAY_CHAR.serializeAsText(PostgresType.ARRAY_CHAR, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{'a','b'}");
  }

  @Test
  void serializeAsTextCollectionAllNonNull() {
    // Test with non-null values only (null handling in arrays has a known limitation)
    Binary buffer = new Binary();
    List<Object> list = new ArrayList<>(List.of(
        1,
        2,
        3));
    PostgresType.ARRAY_INT.serializeAsText(PostgresType.ARRAY_INT, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1,2,3}");
  }

  @Test
  void serializeAsTextEmptyCollection() {
    Binary buffer = new Binary();
    PostgresType.ARRAY_INT.serializeAsText(PostgresType.ARRAY_INT, buffer, new ArrayList<>());
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{}");
  }

  @Test
  void serializeAsTextPrimitiveIntArray() {
    Binary buffer = new Binary();
    int[] array = {1, 2, 3};
    PostgresType.ARRAY_INT.serializeAsText(PostgresType.ARRAY_INT, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1,2,3}");
  }

  @Test
  void serializeAsTextPrimitiveLongArray() {
    Binary buffer = new Binary();
    long[] array = {100L, 200L};
    PostgresType.ARRAY_LONG.serializeAsText(PostgresType.ARRAY_LONG, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{100,200}");
  }

  @Test
  void serializeAsTextPrimitiveFloatArray() {
    Binary buffer = new Binary();
    float[] array = {1.5f, 2.5f};
    PostgresType.ARRAY_REAL.serializeAsText(PostgresType.ARRAY_REAL, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1.5,2.5}");
  }

  @Test
  void serializeAsTextPrimitiveDoubleArray() {
    Binary buffer = new Binary();
    double[] array = {1.5d, 2.5d};
    PostgresType.ARRAY_DOUBLE.serializeAsText(PostgresType.ARRAY_DOUBLE, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1.5,2.5}");
  }

  @Test
  void serializeAsTextPrimitiveShortArray() {
    Binary buffer = new Binary();
    short[] array = {10, 20};
    PostgresType.ARRAY_INT.serializeAsText(PostgresType.ARRAY_INT, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{10,20}");
  }

  @Test
  void serializeAsTextPrimitiveBooleanArray() {
    Binary buffer = new Binary();
    boolean[] array = {true, false};
    PostgresType.ARRAY_BOOLEAN.serializeAsText(PostgresType.ARRAY_BOOLEAN, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{true,false}");
  }

  @Test
  void serializeAsTextPrimitiveCharArray() {
    Binary buffer = new Binary();
    char[] array = {'a', 'b'};
    PostgresType.ARRAY_CHAR.serializeAsText(PostgresType.ARRAY_CHAR, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{'a','b'}");
  }

  @Test
  void serializeAsTextPrimitiveByteArray() {
    Binary buffer = new Binary();
    byte[] array = {1, 2, 3};
    PostgresType.ARRAY_CHAR.serializeAsText(PostgresType.ARRAY_CHAR, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{1,2,3}");
  }

  @Test
  void serializeAsTextObjectArray() {
    Binary buffer = new Binary();
    String[] array = {"hello", "world"};
    PostgresType.ARRAY_TEXT.serializeAsText(PostgresType.ARRAY_TEXT, buffer, array);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    assertThat(new String(data)).isEqualTo("{\"hello\",\"world\"}");
  }

  @Test
  void serializeAsTextCollectionOfMaps() {
    Binary buffer = new Binary();
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    list.add(map);
    PostgresType.ARRAY_JSON.serializeAsText(PostgresType.ARRAY_JSON, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    assertThat(result).startsWith("{").endsWith("}");
    assertThat(result).contains("key");
  }

  @Test
  void serializeAsTextCollectionOfJSONObjects() {
    Binary buffer = new Binary();
    List<JSONObject> list = Arrays.asList(new JSONObject("{\"a\":1}"));
    PostgresType.ARRAY_JSON.serializeAsText(PostgresType.ARRAY_JSON, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    assertThat(result).contains("a");
  }

  @Test
  void serializeAsTextCollectionOfDates() {
    Binary buffer = new Binary();
    List<Date> list = Arrays.asList(new Date(1716138311000L));
    PostgresType.ARRAY_TEXT.serializeAsText(PostgresType.ARRAY_TEXT, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    assertThat(result).matches("\\{\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+\"\\}");
  }

  @Test
  void serializeAsTextCollectionOfLocalDateTimes() {
    Binary buffer = new Binary();
    List<LocalDateTime> list = Arrays.asList(LocalDateTime.of(2024, 5, 19, 17, 5, 11));
    PostgresType.ARRAY_TEXT.serializeAsText(PostgresType.ARRAY_TEXT, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    assertThat(result).contains("2024-05-19");
  }

  @Test
  void serializeAsTextNestedCollections() {
    Binary buffer = new Binary();
    List<List<Integer>> list = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4));
    PostgresType.ARRAY_TEXT.serializeAsText(PostgresType.ARRAY_TEXT, buffer, list);
    buffer.flip();
    int length = buffer.getInt();
    byte[] data = new byte[length];
    buffer.getByteBuffer().get(data);
    String result = new String(data);
    assertThat(result).isEqualTo("{{1,2},{3,4}}");
  }

  // ==================== isArrayType Tests ====================

  @Test
  void isArrayTypeTrue() {
    assertThat(PostgresType.ARRAY_INT.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_LONG.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_DOUBLE.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_REAL.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_TEXT.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_BOOLEAN.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_CHAR.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_JSON.isArrayType()).isTrue();
  }

  @Test
  void isArrayTypeFalse() {
    assertThat(PostgresType.INTEGER.isArrayType()).isFalse();
    assertThat(PostgresType.LONG.isArrayType()).isFalse();
    assertThat(PostgresType.VARCHAR.isArrayType()).isFalse();
    assertThat(PostgresType.TEXT.isArrayType()).isFalse();
    assertThat(PostgresType.BOOLEAN.isArrayType()).isFalse();
    assertThat(PostgresType.DATE.isArrayType()).isFalse();
    assertThat(PostgresType.JSON.isArrayType()).isFalse();
    assertThat(PostgresType.REAL.isArrayType()).isFalse();
    assertThat(PostgresType.DOUBLE.isArrayType()).isFalse();
    assertThat(PostgresType.SMALLINT.isArrayType()).isFalse();
    assertThat(PostgresType.CHAR.isArrayType()).isFalse();
  }

  // ==================== getArrayTypeForElementType Tests ====================

  @Test
  void getArrayTypeForElementTypeInteger() {
    assertThat(PostgresType.getArrayTypeForElementType(1)).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getArrayTypeForElementType(Integer.valueOf(1))).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getArrayTypeForElementTypeShort() {
    assertThat(PostgresType.getArrayTypeForElementType((short) 1)).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getArrayTypeForElementType(Short.valueOf((short) 1))).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getArrayTypeForElementTypeByte() {
    assertThat(PostgresType.getArrayTypeForElementType((byte) 1)).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getArrayTypeForElementType(Byte.valueOf((byte) 1))).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void getArrayTypeForElementTypeLong() {
    assertThat(PostgresType.getArrayTypeForElementType(1L)).isEqualTo(PostgresType.ARRAY_LONG);
    assertThat(PostgresType.getArrayTypeForElementType(Long.valueOf(1L))).isEqualTo(PostgresType.ARRAY_LONG);
  }

  @Test
  void getArrayTypeForElementTypeFloat() {
    assertThat(PostgresType.getArrayTypeForElementType(1.0f)).isEqualTo(PostgresType.ARRAY_REAL);
    assertThat(PostgresType.getArrayTypeForElementType(Float.valueOf(1.0f))).isEqualTo(PostgresType.ARRAY_REAL);
  }

  @Test
  void getArrayTypeForElementTypeDouble() {
    assertThat(PostgresType.getArrayTypeForElementType(1.0d)).isEqualTo(PostgresType.ARRAY_DOUBLE);
    assertThat(PostgresType.getArrayTypeForElementType(Double.valueOf(1.0d))).isEqualTo(PostgresType.ARRAY_DOUBLE);
  }

  @Test
  void getArrayTypeForElementTypeBoolean() {
    assertThat(PostgresType.getArrayTypeForElementType(true)).isEqualTo(PostgresType.ARRAY_BOOLEAN);
    assertThat(PostgresType.getArrayTypeForElementType(Boolean.FALSE)).isEqualTo(PostgresType.ARRAY_BOOLEAN);
  }

  @Test
  void getArrayTypeForElementTypeString() {
    assertThat(PostgresType.getArrayTypeForElementType("hello")).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  @Test
  void getArrayTypeForElementTypeJSONObject() {
    assertThat(PostgresType.getArrayTypeForElementType(new JSONObject())).isEqualTo(PostgresType.ARRAY_JSON);
  }

  @Test
  void getArrayTypeForElementTypeMap() {
    assertThat(PostgresType.getArrayTypeForElementType(new HashMap<>())).isEqualTo(PostgresType.ARRAY_JSON);
  }

  @Test
  void getArrayTypeForElementTypeUnknown() {
    // For unknown types, default to ARRAY_TEXT
    assertThat(PostgresType.getArrayTypeForElementType(new Object())).isEqualTo(PostgresType.ARRAY_TEXT);
  }

  // ==================== Type Code and Properties Tests ====================

  @Test
  void typeCodesAreCorrect() {
    assertThat(PostgresType.SMALLINT.code).isEqualTo(21);
    assertThat(PostgresType.INTEGER.code).isEqualTo(23);
    assertThat(PostgresType.LONG.code).isEqualTo(20);
    assertThat(PostgresType.REAL.code).isEqualTo(700);
    assertThat(PostgresType.DOUBLE.code).isEqualTo(701);
    assertThat(PostgresType.CHAR.code).isEqualTo(18);
    assertThat(PostgresType.BOOLEAN.code).isEqualTo(16);
    assertThat(PostgresType.DATE.code).isEqualTo(1082);
    assertThat(PostgresType.VARCHAR.code).isEqualTo(1043);
    assertThat(PostgresType.TEXT.code).isEqualTo(25);
    assertThat(PostgresType.BPCHAR.code).isEqualTo(1042);
    assertThat(PostgresType.JSON.code).isEqualTo(114);
    assertThat(PostgresType.ARRAY_INT.code).isEqualTo(1007);
    assertThat(PostgresType.ARRAY_LONG.code).isEqualTo(1016);
    assertThat(PostgresType.ARRAY_REAL.code).isEqualTo(1021);
    assertThat(PostgresType.ARRAY_DOUBLE.code).isEqualTo(1022);
    assertThat(PostgresType.ARRAY_TEXT.code).isEqualTo(1009);
    assertThat(PostgresType.ARRAY_BOOLEAN.code).isEqualTo(1000);
    assertThat(PostgresType.ARRAY_CHAR.code).isEqualTo(1003);
    assertThat(PostgresType.ARRAY_JSON.code).isEqualTo(199);
  }

  @Test
  void typeSizesAreCorrect() {
    assertThat(PostgresType.SMALLINT.size).isEqualTo(2);
    assertThat(PostgresType.INTEGER.size).isEqualTo(4);
    assertThat(PostgresType.LONG.size).isEqualTo(8);
    assertThat(PostgresType.REAL.size).isEqualTo(4);
    assertThat(PostgresType.DOUBLE.size).isEqualTo(8);
    assertThat(PostgresType.CHAR.size).isEqualTo(1);
    assertThat(PostgresType.BOOLEAN.size).isEqualTo(1);
    assertThat(PostgresType.DATE.size).isEqualTo(4);
    assertThat(PostgresType.TIMESTAMP.size).isEqualTo(8);
    assertThat(PostgresType.VARCHAR.size).isEqualTo(-1); // variable
    assertThat(PostgresType.TEXT.size).isEqualTo(-1); // variable
    assertThat(PostgresType.JSON.size).isEqualTo(-1); // variable
    // Arrays are variable length
    assertThat(PostgresType.ARRAY_INT.size).isEqualTo(-1);
    assertThat(PostgresType.ARRAY_TEXT.size).isEqualTo(-1);
  }

  // ==================== Defensive serializeAsBinary coercion ====================
  // Schemaless documents can hold values whose runtime Java type does not match the column type
  // advertised in RowDescription (the announcement is derived from the first row). The Postgres
  // wire protocol fixes one format code per column for the whole result, so the binary serializer
  // must coerce mismatched values rather than throwing.

  @Test
  void serializeAsBinaryIntegerCoercesNumericString() {
    final Binary buffer = new Binary();
    PostgresType.INTEGER.serializeAsBinary(PostgresType.INTEGER, buffer, "12345");
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(4);
    assertThat(buffer.getInt()).isEqualTo(12345);
  }

  @Test
  void serializeAsBinaryLongCoercesNumericString() {
    final Binary buffer = new Binary();
    PostgresType.LONG.serializeAsBinary(PostgresType.LONG, buffer, "9876543210");
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);
    assertThat(buffer.getLong()).isEqualTo(9876543210L);
  }

  @Test
  void serializeAsBinaryDoubleCoercesNumericString() {
    final Binary buffer = new Binary();
    PostgresType.DOUBLE.serializeAsBinary(PostgresType.DOUBLE, buffer, "3.14159");
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);
    assertThat(Double.longBitsToDouble(buffer.getLong())).isEqualTo(3.14159d);
  }

  @Test
  void serializeAsBinaryBooleanCoercesString() {
    final Binary t = new Binary();
    PostgresType.BOOLEAN.serializeAsBinary(PostgresType.BOOLEAN, t, "t");
    t.flip();
    assertThat(t.getInt()).isEqualTo(1);
    assertThat(t.getByte()).isEqualTo((byte) 1);

    final Binary f = new Binary();
    PostgresType.BOOLEAN.serializeAsBinary(PostgresType.BOOLEAN, f, "false");
    f.flip();
    assertThat(f.getInt()).isEqualTo(1);
    assertThat(f.getByte()).isEqualTo((byte) 0);
  }

  @Test
  void serializeAsBinaryBooleanCoercesNumber() {
    final Binary t = new Binary();
    PostgresType.BOOLEAN.serializeAsBinary(PostgresType.BOOLEAN, t, 1);
    t.flip();
    assertThat(t.getInt()).isEqualTo(1);
    assertThat(t.getByte()).isEqualTo((byte) 1);

    final Binary f = new Binary();
    PostgresType.BOOLEAN.serializeAsBinary(PostgresType.BOOLEAN, f, 0);
    f.flip();
    assertThat(f.getInt()).isEqualTo(1);
    assertThat(f.getByte()).isEqualTo((byte) 0);
  }

  @Test
  void serializeAsBinaryDateCoercesIsoString() {
    final Binary buffer = new Binary();
    PostgresType.DATE.serializeAsBinary(PostgresType.DATE, buffer, "2024-08-22");
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(4);
    final byte[] bytes = new byte[4];
    buffer.getByteBuffer().get(bytes);
    final Object decoded = PostgresType.deserialize(PostgresType.DATE.code, 1, bytes);
    assertThat(decoded).isInstanceOf(Date.class);
    assertThat(((Date) decoded).toInstant().atZone(ZoneOffset.UTC).toLocalDate())
        .isEqualTo(LocalDate.of(2024, 8, 22));
  }

  @Test
  void serializeAsBinaryTimestampCoercesString() {
    final Binary buffer = new Binary();
    PostgresType.TIMESTAMP.serializeAsBinary(PostgresType.TIMESTAMP, buffer, "2024-08-22 14:30:45.123456");
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);
    final byte[] bytes = new byte[8];
    buffer.getByteBuffer().get(bytes);
    final Object decoded = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 1, bytes);
    assertThat(decoded).isEqualTo(LocalDateTime.of(2024, 8, 22, 14, 30, 45, 123_456_000));
  }

  @Test
  void serializeAsBinaryTimestampTruncatesSubMicrosecondNanos() {
    // PostgreSQL timestamp has microsecond resolution. Sub-microsecond nanos are intentionally
    // dropped on the wire, so a value with 123_456_789 nanos round-trips as 123_456_000.
    final LocalDateTime input = LocalDateTime.of(2024, 8, 22, 14, 30, 45, 123_456_789);

    final Binary buffer = new Binary();
    PostgresType.TIMESTAMP.serializeAsBinary(PostgresType.TIMESTAMP, buffer, input);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);

    final byte[] bytes = new byte[8];
    buffer.getByteBuffer().get(bytes);
    final Object decoded = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 1, bytes);
    assertThat(decoded).isEqualTo(LocalDateTime.of(2024, 8, 22, 14, 30, 45, 123_456_000));
  }

  @Test
  void hasBinaryEncodingScalarTypesTrue() {
    assertThat(PostgresType.INTEGER.hasBinaryEncoding()).isTrue();
    assertThat(PostgresType.LONG.hasBinaryEncoding()).isTrue();
    assertThat(PostgresType.BOOLEAN.hasBinaryEncoding()).isTrue();
    assertThat(PostgresType.DATE.hasBinaryEncoding()).isTrue();
    assertThat(PostgresType.TIMESTAMP.hasBinaryEncoding()).isTrue();
    // Strings/JSON have identical text and binary wire bytes - safe to honor binary requests.
    assertThat(PostgresType.VARCHAR.hasBinaryEncoding()).isTrue();
    assertThat(PostgresType.JSON.hasBinaryEncoding()).isTrue();
  }

  @Test
  void hasBinaryEncodingArrayTypesFalse() {
    // Array binary wire format is not yet implemented in serializeAsBinary, so callers must
    // advertise text format in RowDescription for arrays regardless of what the client requested.
    assertThat(PostgresType.ARRAY_INT.hasBinaryEncoding()).isFalse();
    assertThat(PostgresType.ARRAY_LONG.hasBinaryEncoding()).isFalse();
    assertThat(PostgresType.ARRAY_TEXT.hasBinaryEncoding()).isFalse();
    assertThat(PostgresType.ARRAY_DOUBLE.hasBinaryEncoding()).isFalse();
    assertThat(PostgresType.ARRAY_BOOLEAN.hasBinaryEncoding()).isFalse();
    assertThat(PostgresType.ARRAY_JSON.hasBinaryEncoding()).isFalse();
  }

  @Test
  void serializeAsBinaryTimestampPre2000RoundTrip() {
    // 1999-12-31T23:59:59.999999 - exercises the nanos < 0 borrow branch in deserializeBinary.
    final LocalDateTime ldt = LocalDateTime.of(1999, 12, 31, 23, 59, 59, 999_999_000);

    final Binary buffer = new Binary();
    PostgresType.TIMESTAMP.serializeAsBinary(PostgresType.TIMESTAMP, buffer, ldt);
    buffer.flip();
    assertThat(buffer.getInt()).isEqualTo(8);

    final byte[] bytes = new byte[8];
    buffer.getByteBuffer().get(bytes);
    final Object decoded = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 1, bytes);
    assertThat(decoded).isEqualTo(ldt);
  }

  // ==================== Text parser edge cases ====================

  @Test
  void parseBooleanTextNullThrows() {
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.BOOLEAN.code, 0,
        new byte[0])) // empty string after charset decode goes to switch and falls through
        .isInstanceOf(PostgresProtocolException.class);
  }

  @Test
  void parseBooleanTextAcceptsCanonicalForms() {
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "t".getBytes())).isEqualTo(Boolean.TRUE);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "true".getBytes())).isEqualTo(Boolean.TRUE);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "yes".getBytes())).isEqualTo(Boolean.TRUE);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "1".getBytes())).isEqualTo(Boolean.TRUE);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "f".getBytes())).isEqualTo(Boolean.FALSE);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "false".getBytes())).isEqualTo(Boolean.FALSE);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "0".getBytes())).isEqualTo(Boolean.FALSE);
  }

  @Test
  void parseBooleanTextRejectsUnknown() {
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "maybe".getBytes()))
        .isInstanceOf(PostgresProtocolException.class);
  }

  @Test
  void parseTimestampTextAcceptsIso() {
    final Object decoded = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 0,
        "2024-06-15 12:30:45".getBytes());
    assertThat(decoded).isEqualTo(LocalDateTime.of(2024, 6, 15, 12, 30, 45));
  }

  @Test
  void parseTimestampTextAcceptsOffset() {
    // Postgres servers/clients can send timestamps with offset suffix - the parser must accept
    // ISO_OFFSET_DATE_TIME and reduce to LocalDateTime for the schema-less binding path.
    final Object decoded = PostgresType.deserialize(PostgresType.TIMESTAMP.code, 0,
        "2024-06-15 12:30:45+00:00".getBytes());
    assertThat(decoded).isEqualTo(LocalDateTime.of(2024, 6, 15, 12, 30, 45));
  }

  @Test
  void parseDateTextRejectsNonIso() {
    // Removed legacy Long.parseLong fallback - epoch-millis strings are no longer accepted.
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.DATE.code, 0,
        "1716138311000".getBytes()))
        .isInstanceOf(DateTimeParseException.class);
  }
}
