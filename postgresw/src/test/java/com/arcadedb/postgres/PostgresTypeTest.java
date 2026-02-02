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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
    assertThat(PostgresType.getTypeForValue(LocalDateTime.now())).isEqualTo(PostgresType.DATE);
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
    assertThat(PostgresType.getTypeForValue(Collections.emptyList())).isEqualTo(PostgresType.ARRAY_TEXT);
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
    assertThat(PostgresType.getTypeFromArcade(Type.DATETIME)).isEqualTo(PostgresType.DATE);
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
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "true".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "TRUE".getBytes())).isEqualTo(true);
    assertThat(PostgresType.deserialize(PostgresType.BOOLEAN.code, 0, "false".getBytes())).isEqualTo(false);
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
    ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
    buffer.putChar('X');
    Object result = PostgresType.deserialize(PostgresType.CHAR.code, 1, buffer.array());
    assertThat(result).isEqualTo('X');
  }

  @Test
  void deserializeBinaryVarchar() {
    byte[] data = "hello binary".getBytes();
    Object result = PostgresType.deserialize(PostgresType.VARCHAR.code, 1, data);
    assertThat(result).isEqualTo("hello binary");
  }

  @Test
  void deserializeBinaryDate() {
    long timestamp = System.currentTimeMillis();
    ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(timestamp);
    Object result = PostgresType.deserialize(PostgresType.DATE.code, 1, buffer.array());
    assertThat(result).isInstanceOf(Date.class);
    assertThat(((Date) result).getTime()).isEqualTo(timestamp);
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
  void deserializeBinaryArrayNotSupported() {
    byte[] data = new byte[10];
    assertThatThrownBy(() -> PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data))
        .isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("not yet implemented");
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
    assertThat(result).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+");
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
    List<Object> list = new ArrayList<>();
    list.add(1);
    list.add(2);
    list.add(3);
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
    assertThat(PostgresType.DATE.size).isEqualTo(8);
    assertThat(PostgresType.VARCHAR.size).isEqualTo(-1); // variable
    assertThat(PostgresType.JSON.size).isEqualTo(-1); // variable
    // Arrays are variable length
    assertThat(PostgresType.ARRAY_INT.size).isEqualTo(-1);
    assertThat(PostgresType.ARRAY_TEXT.size).isEqualTo(-1);
  }
}
