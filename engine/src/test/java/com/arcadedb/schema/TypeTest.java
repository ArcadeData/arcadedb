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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.MultiIterator;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TypeTest extends TestHelper {

  // ───── getById ─────

  @Test
  void getByIdValidIds() {
    assertThat(Type.getById((byte) 0)).isEqualTo(Type.BOOLEAN);
    assertThat(Type.getById((byte) 7)).isEqualTo(Type.STRING);
    assertThat(Type.getById((byte) 23)).isEqualTo(Type.ARRAY_OF_DOUBLES);
  }

  @Test
  void getByIdOutOfRange() {
    assertThat(Type.getById((byte) -1)).isNull();
    assertThat(Type.getById((byte) 24)).isNull();
    assertThat(Type.getById((byte) 100)).isNull();
  }

  // ───── getByBinaryType ─────

  @Test
  void getByBinaryType() {
    for (final Type type : Type.values())
      assertThat(Type.getByBinaryType(type.getBinaryType())).isEqualTo(type);
  }

  @Test
  void getByBinaryTypeNotFound() {
    assertThat(Type.getByBinaryType((byte) -99)).isNull();
  }

  // ───── validateValue ─────

  @Test
  void validateValueAcceptsNull() {
    Type.validateValue(null); // should not throw
  }

  @Test
  void validateValueAcceptsKnownTypes() {
    Type.validateValue("hello");
    Type.validateValue(42);
    Type.validateValue(3.14);
    Type.validateValue(Map.of("a", 1));
    Type.validateValue(List.of(1, 2));
    Type.validateValue(true);
    Type.validateValue((byte) 1);
    Type.validateValue((short) 1);
    Type.validateValue(1L);
    Type.validateValue(1f);
    Type.validateValue(new BigDecimal("1.0"));
  }

  @Test
  void validateValueRejectsUnknown() {
    assertThatThrownBy(() -> Type.validateValue(new Object()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not supported");
  }

  // ───── getTypeByName ─────

  @Test
  void getTypeByName() {
    assertThat(Type.getTypeByName("Boolean")).isEqualTo(Type.BOOLEAN);
    assertThat(Type.getTypeByName("STRING")).isEqualTo(Type.STRING);
    assertThat(Type.getTypeByName("integer")).isEqualTo(Type.INTEGER);
    assertThat(Type.getTypeByName("short")).isEqualTo(Type.SHORT);
    assertThat(Type.getTypeByName("LONG")).isEqualTo(Type.LONG);
    assertThat(Type.getTypeByName("float")).isEqualTo(Type.FLOAT);
    assertThat(Type.getTypeByName("DOUBLE")).isEqualTo(Type.DOUBLE);
    assertThat(Type.getTypeByName("datetime")).isEqualTo(Type.DATETIME);
    assertThat(Type.getTypeByName("binary")).isEqualTo(Type.BINARY);
    assertThat(Type.getTypeByName("list")).isEqualTo(Type.LIST);
    assertThat(Type.getTypeByName("map")).isEqualTo(Type.MAP);
    assertThat(Type.getTypeByName("link")).isEqualTo(Type.LINK);
    assertThat(Type.getTypeByName("byte")).isEqualTo(Type.BYTE);
    assertThat(Type.getTypeByName("date")).isEqualTo(Type.DATE);
    assertThat(Type.getTypeByName("decimal")).isEqualTo(Type.DECIMAL);
    assertThat(Type.getTypeByName("embedded")).isEqualTo(Type.EMBEDDED);
    assertThat(Type.getTypeByName("datetime_micros")).isEqualTo(Type.DATETIME_MICROS);
    assertThat(Type.getTypeByName("datetime_nanos")).isEqualTo(Type.DATETIME_NANOS);
    assertThat(Type.getTypeByName("datetime_second")).isEqualTo(Type.DATETIME_SECOND);
  }

  @Test
  void getTypeByNameUnknown() {
    assertThat(Type.getTypeByName("nonexistent")).isNull();
  }

  // ───── getTypeByClass ─────

  @Test
  void getTypeByClassNull() {
    assertThat(Type.getTypeByClass(null)).isNull();
  }

  @Test
  void getTypeByClassPrimitives() {
    assertThat(Type.getTypeByClass(Boolean.class)).isEqualTo(Type.BOOLEAN);
    assertThat(Type.getTypeByClass(Boolean.TYPE)).isEqualTo(Type.BOOLEAN);
    assertThat(Type.getTypeByClass(Integer.class)).isEqualTo(Type.INTEGER);
    assertThat(Type.getTypeByClass(Integer.TYPE)).isEqualTo(Type.INTEGER);
    assertThat(Type.getTypeByClass(Short.class)).isEqualTo(Type.SHORT);
    assertThat(Type.getTypeByClass(Short.TYPE)).isEqualTo(Type.SHORT);
    assertThat(Type.getTypeByClass(Long.class)).isEqualTo(Type.LONG);
    assertThat(Type.getTypeByClass(Long.TYPE)).isEqualTo(Type.LONG);
    assertThat(Type.getTypeByClass(Float.class)).isEqualTo(Type.FLOAT);
    assertThat(Type.getTypeByClass(Float.TYPE)).isEqualTo(Type.FLOAT);
    assertThat(Type.getTypeByClass(Double.class)).isEqualTo(Type.DOUBLE);
    assertThat(Type.getTypeByClass(Double.TYPE)).isEqualTo(Type.DOUBLE);
    assertThat(Type.getTypeByClass(Byte.class)).isEqualTo(Type.BYTE);
    assertThat(Type.getTypeByClass(Byte.TYPE)).isEqualTo(Type.BYTE);
    assertThat(Type.getTypeByClass(String.class)).isEqualTo(Type.STRING);
    assertThat(Type.getTypeByClass(Character.class)).isEqualTo(Type.STRING);
    assertThat(Type.getTypeByClass(Character.TYPE)).isEqualTo(Type.STRING);
  }

  @Test
  void getTypeByClassCollections() {
    assertThat(Type.getTypeByClass(List.class)).isEqualTo(Type.LIST);
    assertThat(Type.getTypeByClass(Map.class)).isEqualTo(Type.MAP);
    assertThat(Type.getTypeByClass(byte[].class)).isEqualTo(Type.BINARY);
    assertThat(Type.getTypeByClass(BigDecimal.class)).isEqualTo(Type.DECIMAL);
  }

  @Test
  void getTypeByClassInheritArray() {
    // Non-byte arrays should resolve to LIST via getTypeByClassInherit
    assertThat(Type.getTypeByClass(String[].class)).isEqualTo(Type.LIST);
    assertThat(Type.getTypeByClass(Object[].class)).isEqualTo(Type.LIST);
  }

  @Test
  void getTypeByClassInheritSubclass() {
    // ArrayList is assignable from List
    assertThat(Type.getTypeByClass(ArrayList.class)).isEqualTo(Type.LIST);
    // HashMap is assignable from Map
    assertThat(Type.getTypeByClass(HashMap.class)).isEqualTo(Type.MAP);
    // LinkedList
    assertThat(Type.getTypeByClass(LinkedList.class)).isEqualTo(Type.LIST);
  }

  @Test
  void getTypeByClassArrayTypes() {
    assertThat(Type.getTypeByClass(short[].class)).isEqualTo(Type.ARRAY_OF_SHORTS);
    assertThat(Type.getTypeByClass(int[].class)).isEqualTo(Type.ARRAY_OF_INTEGERS);
    assertThat(Type.getTypeByClass(long[].class)).isEqualTo(Type.ARRAY_OF_LONGS);
    assertThat(Type.getTypeByClass(float[].class)).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(Type.getTypeByClass(double[].class)).isEqualTo(Type.ARRAY_OF_DOUBLES);
  }

  // ───── getTypeByValue ─────

  @Test
  void getTypeByValueNull() {
    assertThat(Type.getTypeByValue(null)).isNull();
  }

  @Test
  void getTypeByValueKnown() {
    assertThat(Type.getTypeByValue("hello")).isEqualTo(Type.STRING);
    assertThat(Type.getTypeByValue(42)).isEqualTo(Type.INTEGER);
    assertThat(Type.getTypeByValue(42L)).isEqualTo(Type.LONG);
    assertThat(Type.getTypeByValue(3.14f)).isEqualTo(Type.FLOAT);
    assertThat(Type.getTypeByValue(3.14d)).isEqualTo(Type.DOUBLE);
    assertThat(Type.getTypeByValue(true)).isEqualTo(Type.BOOLEAN);
    assertThat(Type.getTypeByValue((byte) 1)).isEqualTo(Type.BYTE);
    assertThat(Type.getTypeByValue((short) 1)).isEqualTo(Type.SHORT);
    assertThat(Type.getTypeByValue(new BigDecimal("1.0"))).isEqualTo(Type.DECIMAL);
    assertThat(Type.getTypeByValue(new Date())).isEqualTo(Type.DATETIME);
    assertThat(Type.getTypeByValue(new byte[] { 1, 2 })).isEqualTo(Type.BINARY);
  }

  @Test
  void getTypeByValueInherit() {
    assertThat(Type.getTypeByValue(new ArrayList<>())).isEqualTo(Type.LIST);
    assertThat(Type.getTypeByValue(new HashMap<>())).isEqualTo(Type.MAP);
    assertThat(Type.getTypeByValue(new String[] { "a" })).isEqualTo(Type.LIST);
  }

  // ───── Instance methods: isMultiValue, isLink, isEmbedded ─────

  @Test
  void isMultiValue() {
    assertThat(Type.LIST.isMultiValue()).isTrue();
    assertThat(Type.MAP.isMultiValue()).isTrue();
    assertThat(Type.STRING.isMultiValue()).isFalse();
    assertThat(Type.INTEGER.isMultiValue()).isFalse();
    assertThat(Type.LINK.isMultiValue()).isFalse();
  }

  @Test
  void isLink() {
    assertThat(Type.LINK.isLink()).isTrue();
    assertThat(Type.STRING.isLink()).isFalse();
    assertThat(Type.LIST.isLink()).isFalse();
  }

  @Test
  void isEmbedded() {
    assertThat(Type.LIST.isEmbedded()).isTrue();
    assertThat(Type.MAP.isEmbedded()).isTrue();
    assertThat(Type.EMBEDDED.isEmbedded()).isFalse();
    assertThat(Type.STRING.isEmbedded()).isFalse();
  }

  // ───── getCastable, getDefaultJavaType, getBinaryType, getId ─────

  @Test
  void getCastable() {
    assertThat(Type.BYTE.getCastable()).contains(Type.BYTE, Type.BOOLEAN);
    assertThat(Type.SHORT.getCastable()).contains(Type.SHORT, Type.BOOLEAN, Type.BYTE);
    assertThat(Type.INTEGER.getCastable()).contains(Type.INTEGER, Type.BOOLEAN, Type.BYTE, Type.SHORT);
    assertThat(Type.LONG.getCastable()).contains(Type.LONG, Type.BOOLEAN, Type.BYTE, Type.SHORT, Type.INTEGER);
    assertThat(Type.DECIMAL.getCastable()).contains(Type.DECIMAL, Type.BOOLEAN, Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG,
        Type.FLOAT, Type.DOUBLE);
    // STRING only castable to itself
    assertThat(Type.STRING.getCastable()).containsExactly(Type.STRING);
  }

  @Test
  void getDefaultJavaType() {
    assertThat(Type.BOOLEAN.getDefaultJavaType()).isEqualTo(Boolean.class);
    assertThat(Type.STRING.getDefaultJavaType()).isEqualTo(String.class);
    assertThat(Type.INTEGER.getDefaultJavaType()).isEqualTo(Integer.class);
    assertThat(Type.LONG.getDefaultJavaType()).isEqualTo(Long.class);
    assertThat(Type.FLOAT.getDefaultJavaType()).isEqualTo(Float.class);
    assertThat(Type.DOUBLE.getDefaultJavaType()).isEqualTo(Double.class);
    assertThat(Type.LINK.getDefaultJavaType()).isEqualTo(Identifiable.class);
    assertThat(Type.BINARY.getDefaultJavaType()).isEqualTo(byte[].class);
    assertThat(Type.LIST.getDefaultJavaType()).isEqualTo(List.class);
    assertThat(Type.MAP.getDefaultJavaType()).isEqualTo(Map.class);
  }

  @Test
  void getId() {
    assertThat(Type.BOOLEAN.getId()).isEqualTo(0);
    assertThat(Type.INTEGER.getId()).isEqualTo(1);
    assertThat(Type.STRING.getId()).isEqualTo(7);
    assertThat(Type.ARRAY_OF_DOUBLES.getId()).isEqualTo(23);
  }

  @Test
  void getBinaryType() {
    for (final Type type : Type.values())
      assertThat(type.getBinaryType()).isNotEqualTo((byte) -1);
  }

  @Test
  void getJavaTypesDeprecated() {
    assertThat(Type.STRING.getJavaTypes()).isNull();
  }

  // ───── asInt, asLong, asFloat, asDouble ─────

  @Test
  void asInt() {
    assertThat(Type.INTEGER.asInt(42)).isEqualTo(42);
    assertThat(Type.INTEGER.asInt(42L)).isEqualTo(42);
    assertThat(Type.INTEGER.asInt(42.9)).isEqualTo(42);
    assertThat(Type.INTEGER.asInt("123")).isEqualTo(123);
    assertThat(Type.INTEGER.asInt(true)).isEqualTo(1);
    assertThat(Type.INTEGER.asInt(false)).isEqualTo(0);
  }

  @Test
  void asIntInvalid() {
    assertThatThrownBy(() -> Type.INTEGER.asInt(new Date()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void asLong() {
    assertThat(Type.LONG.asLong(42L)).isEqualTo(42L);
    assertThat(Type.LONG.asLong(42)).isEqualTo(42L);
    assertThat(Type.LONG.asLong("123")).isEqualTo(123L);
    assertThat(Type.LONG.asLong(true)).isEqualTo(1L);
    assertThat(Type.LONG.asLong(false)).isEqualTo(0L);
  }

  @Test
  void asLongInvalid() {
    assertThatThrownBy(() -> Type.LONG.asLong(new Date()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void asFloat() {
    assertThat(Type.FLOAT.asFloat(3.14f)).isEqualTo(3.14f);
    assertThat(Type.FLOAT.asFloat(42)).isEqualTo(42.0f);
    assertThat(Type.FLOAT.asFloat("3.14")).isEqualTo(3.14f);
  }

  @Test
  void asFloatInvalid() {
    assertThatThrownBy(() -> Type.FLOAT.asFloat(new Date()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void asDouble() {
    assertThat(Type.DOUBLE.asDouble(3.14)).isEqualTo(3.14);
    assertThat(Type.DOUBLE.asDouble(42)).isEqualTo(42.0);
    assertThat(Type.DOUBLE.asDouble("3.14")).isEqualTo(3.14);
  }

  @Test
  void asDoubleInvalid() {
    assertThatThrownBy(() -> Type.DOUBLE.asDouble(new Date()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ───── newInstance ─────

  @Test
  void newInstance() {
    assertThat(Type.STRING.newInstance(42)).isEqualTo("42");
    assertThat(Type.INTEGER.newInstance("42")).isEqualTo(42);
    assertThat(Type.LONG.newInstance("100")).isEqualTo(100L);
    assertThat(Type.DOUBLE.newInstance("3.14")).isEqualTo(3.14);
    assertThat(Type.FLOAT.newInstance("3.14")).isEqualTo(3.14f);
    assertThat(Type.BOOLEAN.newInstance("true")).isEqualTo(true);
  }

  // ───── convert ─────

  @Test
  void convertNullValue() {
    assertThat(Type.convert(database, null, String.class)).isNull();
  }

  @Test
  void convertNullTargetClass() {
    assertThat(Type.convert(database, "hello", null)).isEqualTo("hello");
  }

  @Test
  void convertSameType() {
    assertThat(Type.convert(database, "hello", String.class)).isEqualTo("hello");
    assertThat(Type.convert(database, 42, Integer.class)).isEqualTo(42);
  }

  @Test
  void convertToString() {
    assertThat(Type.convert(database, 42, String.class)).isEqualTo("42");
    assertThat(Type.convert(database, true, String.class)).isEqualTo("true");
    assertThat(Type.convert(database, 3.14, String.class)).isEqualTo("3.14");
  }

  @Test
  void convertBinaryToByteArray() {
    final Binary binary = new Binary(new byte[] { 1, 2, 3 });
    final Object result = Type.convert(database, binary, byte[].class);
    assertThat(result).isInstanceOf(byte[].class);
  }

  @Test
  void convertByteArrayPassthrough() {
    final byte[] bytes = new byte[] { 1, 2, 3 };
    // byte[] target class that is not String returns the byte[] as-is
    final Object result = Type.convert(database, bytes, Integer.class);
    assertThat(result).isSameAs(bytes);
  }

  @Test
  void convertCollectionToFloatArray() {
    final List<Number> list = List.of(1.0f, 2.0f, 3.0f);
    final Object result = Type.convert(database, list, float[].class);
    assertThat(result).isInstanceOf(float[].class);
    assertThat((float[]) result).containsExactly(1.0f, 2.0f, 3.0f);
  }

  @Test
  void convertCollectionToDoubleArray() {
    final List<Number> list = List.of(1.0, 2.0, 3.0);
    final Object result = Type.convert(database, list, double[].class);
    assertThat(result).isInstanceOf(double[].class);
    assertThat((double[]) result).containsExactly(1.0, 2.0, 3.0);
  }

  @Test
  void convertCollectionToIntArray() {
    final List<Number> list = List.of(1, 2, 3);
    final Object result = Type.convert(database, list, int[].class);
    assertThat(result).isInstanceOf(int[].class);
    assertThat((int[]) result).containsExactly(1, 2, 3);
  }

  @Test
  void convertCollectionToLongArray() {
    final List<Number> list = List.of(1L, 2L, 3L);
    final Object result = Type.convert(database, list, long[].class);
    assertThat(result).isInstanceOf(long[].class);
    assertThat((long[]) result).containsExactly(1L, 2L, 3L);
  }

  @Test
  void convertCollectionToShortArray() {
    final List<Number> list = List.of((short) 1, (short) 2, (short) 3);
    final Object result = Type.convert(database, list, short[].class);
    assertThat(result).isInstanceOf(short[].class);
    assertThat((short[]) result).containsExactly((short) 1, (short) 2, (short) 3);
  }

  @Test
  void convertToEnum() {
    assertThat(Type.convert(database, "STRING", Type.class)).isEqualTo(Type.STRING);
    assertThat(Type.convert(database, 0, Type.class)).isEqualTo(Type.BOOLEAN);
  }

  @Test
  void convertToByte() {
    assertThat(Type.convert(database, (byte) 1, Byte.class)).isEqualTo((byte) 1);
    assertThat(Type.convert(database, "42", Byte.class)).isEqualTo((byte) 42);
    assertThat(Type.convert(database, 42, Byte.class)).isEqualTo((byte) 42);
    assertThat(Type.convert(database, 42L, Byte.TYPE)).isEqualTo((byte) 42);
  }

  @Test
  void convertToShort() {
    assertThat(Type.convert(database, (short) 1, Short.class)).isEqualTo((short) 1);
    assertThat(Type.convert(database, "42", Short.class)).isEqualTo((short) 42);
    assertThat(Type.convert(database, "", Short.class)).isEqualTo((short) 0);
    assertThat(Type.convert(database, 42, Short.class)).isEqualTo((short) 42);
    assertThat(Type.convert(database, 42L, Short.TYPE)).isEqualTo((short) 42);
  }

  @Test
  void convertToInteger() {
    assertThat(Type.convert(database, 1, Integer.class)).isEqualTo(1);
    assertThat(Type.convert(database, "42", Integer.class)).isEqualTo(42);
    assertThat(Type.convert(database, "", Integer.class)).isEqualTo(0);
    assertThat(Type.convert(database, 42L, Integer.class)).isEqualTo(42);
    assertThat(Type.convert(database, 42L, Integer.TYPE)).isEqualTo(42);
  }

  @Test
  void convertToLong() {
    assertThat(Type.convert(database, 1L, Long.class)).isEqualTo(1L);
    assertThat(Type.convert(database, "42", Long.class)).isEqualTo(42L);
    assertThat(Type.convert(database, "", Long.class)).isEqualTo(0L);
    assertThat(Type.convert(database, 42, Long.class)).isEqualTo(42L);
    assertThat(Type.convert(database, 42, Long.TYPE)).isEqualTo(42L);
  }

  @Test
  void convertToFloat() {
    assertThat(Type.convert(database, 1.0f, Float.class)).isEqualTo(1.0f);
    assertThat(Type.convert(database, "3.14", Float.class)).isEqualTo(3.14f);
    assertThat(Type.convert(database, "", Float.class)).isEqualTo(0f);
    assertThat(Type.convert(database, 42, Float.class)).isEqualTo(42.0f);
    assertThat(Type.convert(database, 42, Float.TYPE)).isEqualTo(42.0f);
  }

  @Test
  void convertToDouble() {
    assertThat(Type.convert(database, 1.0, Double.class)).isEqualTo(1.0);
    assertThat(Type.convert(database, "3.14", Double.class)).isEqualTo(3.14);
    assertThat(Type.convert(database, "", Double.class)).isEqualTo(0.0);
    assertThat(Type.convert(database, 42, Double.class)).isEqualTo(42.0);
    assertThat(Type.convert(database, 42, Double.TYPE)).isEqualTo(42.0);
    // Float precision fix: Float -> Double via string parsing
    assertThat(Type.convert(database, 3.14f, Double.class)).isEqualTo(Double.parseDouble("3.14"));
  }

  @Test
  void convertToBigDecimal() {
    assertThat(Type.convert(database, "3.14", BigDecimal.class)).isEqualTo(new BigDecimal("3.14"));
    assertThat(Type.convert(database, 42, BigDecimal.class)).isEqualTo(new BigDecimal("42"));
    assertThat(Type.convert(database, 3.14, BigDecimal.class)).isEqualTo(new BigDecimal("3.14"));
  }

  @Test
  void convertToBoolean() {
    assertThat(Type.convert(database, true, Boolean.class)).isEqualTo(true);
    assertThat(Type.convert(database, "true", Boolean.class)).isEqualTo(true);
    assertThat(Type.convert(database, "TRUE", Boolean.class)).isEqualTo(true);
    assertThat(Type.convert(database, "false", Boolean.class)).isEqualTo(false);
    assertThat(Type.convert(database, "FALSE", Boolean.class)).isEqualTo(false);
    assertThat(Type.convert(database, 1, Boolean.class)).isEqualTo(true);
    assertThat(Type.convert(database, 0, Boolean.class)).isEqualTo(false);
    assertThat(Type.convert(database, 1, Boolean.TYPE)).isEqualTo(true);
  }

  @Test
  void convertToBooleanInvalid() {
    assertThatThrownBy(() -> Type.convert(database, "maybe", Boolean.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Value is not boolean");
  }

  @SuppressWarnings("unchecked")
  @Test
  void convertToSet() {
    final List<String> list = List.of("a", "b", "c");
    final Object result = Type.convert(database, list, Set.class);
    assertThat(result).isInstanceOf(Set.class);
    assertThat((Set<Object>) result).containsExactlyInAnyOrder("a", "b", "c");

    // Single value wraps into singleton set
    final Object single = Type.convert(database, "hello", Set.class);
    assertThat(single).isInstanceOf(Set.class);
    assertThat((Set<Object>) single).containsExactly("hello");
  }

  @SuppressWarnings("unchecked")
  @Test
  void convertToList() {
    final Set<String> set = Set.of("a", "b");
    final Object result = Type.convert(database, set, List.class);
    assertThat(result).isInstanceOf(List.class);
    assertThat((List<Object>) result).containsExactlyInAnyOrder("a", "b");

    // Single value wraps into singleton list
    final Object single = Type.convert(database, "hello", List.class);
    assertThat(single).isInstanceOf(List.class);
    assertThat((List<Object>) single).containsExactly("hello");
  }

  @SuppressWarnings("unchecked")
  @Test
  void convertToCollection() {
    final List<String> list = List.of("a", "b");
    final Object result = Type.convert(database, list, Collection.class);
    assertThat(result).isInstanceOf(Collection.class);
    assertThat((Collection<Object>) result).containsExactlyInAnyOrder("a", "b");

    // Single value wraps into singleton set
    final Object single = Type.convert(database, "hello", Collection.class);
    assertThat(single).isInstanceOf(Set.class);
    assertThat((Set<Object>) single).containsExactly("hello");
  }

  @Test
  void convertToDate() {
    final Date now = new Date();
    // Number -> Date
    assertThat(Type.convert(database, now.getTime(), Date.class)).isEqualTo(now);
    // Calendar -> Date
    final Calendar cal = Calendar.getInstance();
    assertThat(Type.convert(database, cal, Date.class)).isEqualTo(cal.getTime());
    // LocalDateTime -> Date
    final LocalDateTime ldt = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
    assertThat(Type.convert(database, ldt, Date.class)).isInstanceOf(Date.class);
    // Instant -> Date
    final Instant instant = Instant.ofEpochMilli(1000000L);
    assertThat(Type.convert(database, instant, Date.class)).isEqualTo(new Date(1000000L));
    // ZonedDateTime -> Date
    final ZonedDateTime zdt = ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);
    assertThat(Type.convert(database, zdt, Date.class)).isInstanceOf(Date.class);
    // LocalDate -> Date
    final LocalDate ld = LocalDate.of(2024, 1, 1);
    assertThat(Type.convert(database, ld, Date.class)).isInstanceOf(Date.class);
    // String as number -> Date
    assertThat(Type.convert(database, "1000000", Date.class)).isEqualTo(new Date(1000000L));
  }

  @Test
  void convertToCalendar() {
    final Date now = new Date();
    final Object result = Type.convert(database, now, Calendar.class);
    assertThat(result).isInstanceOf(Calendar.class);
    assertThat(((Calendar) result).getTime()).isEqualTo(now);
  }

  @Test
  void convertToLocalDate() {
    final LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 12, 30);
    final Object result = Type.convert(database, ldt, LocalDate.class);
    assertThat(result).isEqualTo(LocalDate.of(2024, 6, 15));

    // String as number
    final Object fromNumber = Type.convert(database, 42, LocalDate.class);
    assertThat(fromNumber).isInstanceOf(LocalDate.class);

    // String date
    final Object fromString = Type.convert(database, "2024-06-15", LocalDate.class);
    assertThat(fromString).isInstanceOf(LocalDate.class);
  }

  @Test
  void convertToLocalDateFromDate() {
    final Date now = new Date();
    final Object result = Type.convert(database, now, LocalDate.class);
    assertThat(result).isInstanceOf(LocalDate.class);
  }

  @Test
  void convertToLocalDateFromCalendar() {
    final Calendar cal = Calendar.getInstance();
    final Object result = Type.convert(database, cal, LocalDate.class);
    assertThat(result).isInstanceOf(LocalDate.class);
  }

  @Test
  void convertToLocalDateFromStringNumber() {
    final Object result = Type.convert(database, "19724", LocalDate.class);
    assertThat(result).isInstanceOf(LocalDate.class);
  }

  @Test
  void convertToLocalDateGuessFormatNullDb() {
    // Without database, guesses format by string length
    final Object result = Type.convert(null, "2024-06-15", LocalDate.class);
    assertThat(result).isEqualTo(LocalDate.of(2024, 6, 15));
  }

  @Test
  void convertToLocalDateTime() {
    // Number -> LocalDateTime
    final Object fromNumber = Type.convert(database, 1000000L, LocalDateTime.class);
    assertThat(fromNumber).isInstanceOf(LocalDateTime.class);

    // Date -> LocalDateTime
    final Date now = new Date();
    final Object fromDate = Type.convert(database, now, LocalDateTime.class);
    assertThat(fromDate).isInstanceOf(LocalDateTime.class);

    // Calendar -> LocalDateTime
    final Calendar cal = Calendar.getInstance();
    final Object fromCal = Type.convert(database, cal, LocalDateTime.class);
    assertThat(fromCal).isInstanceOf(LocalDateTime.class);

    // String ISO format -> LocalDateTime (with database)
    final Object fromString = Type.convert(database, "2024-06-15T12:30:00", LocalDateTime.class);
    assertThat(fromString).isEqualTo(LocalDateTime.of(2024, 6, 15, 12, 30, 0));
  }

  @Test
  void convertToLocalDateTimeGuessFormatNullDb() {
    // Without database, guesses format by string length
    final Object secsFmt = Type.convert(null, "2024-06-15 12:30:00", LocalDateTime.class);
    assertThat(secsFmt).isInstanceOf(LocalDateTime.class);

    final Object millisFmt = Type.convert(null, "2024-06-15 12:30:00.000", LocalDateTime.class);
    assertThat(millisFmt).isInstanceOf(LocalDateTime.class);
  }

  @Test
  void convertToZonedDateTimeFromDate() {
    final Date now = new Date();
    final Object result = Type.convert(database, now, ZonedDateTime.class);
    assertThat(result).isNotNull();
  }

  @Test
  void convertToZonedDateTimeFromCalendar() {
    final Calendar cal = Calendar.getInstance();
    final Object result = Type.convert(database, cal, ZonedDateTime.class);
    assertThat(result).isNotNull();
  }

  @Test
  void convertToInstantFromDate() {
    final Date now = new Date();
    final Object result = Type.convert(database, now, Instant.class);
    assertThat(result).isNotNull();
  }

  @Test
  void convertToInstantFromCalendar() {
    final Calendar cal = Calendar.getInstance();
    final Object result = Type.convert(database, cal, Instant.class);
    assertThat(result).isNotNull();
  }

  @Test
  void convertToRIDFromString() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestRID");
      final var doc = database.newDocument("TestRID");
      doc.save();

      final Object result = Type.convert(database, doc.getIdentity().toString(), RID.class);
      assertThat(result).isInstanceOf(RID.class);
      assertThat(result).isEqualTo(doc.getIdentity());
    });
  }

  @Test
  void convertToRIDFromMultiValue() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestRIDMulti");
      final var doc = database.newDocument("TestRIDMulti");
      doc.save();

      final List<Identifiable> input = List.of(doc.getIdentity());
      final Object result = Type.convert(database, input, Identifiable.class);
      assertThat(result).isInstanceOf(List.class);
      assertThat((List<?>) result).hasSize(1);
    });
  }

  @Test
  void convertToRIDFromMultiValueWithResult() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestRIDResult");
      final var doc = database.newDocument("TestRIDResult");
      doc.save();

      final ResultInternal resultObj = new ResultInternal();
      resultObj.setElement(doc);

      final List<Result> input = List.of(resultObj);
      final Object result = Type.convert(database, input, Identifiable.class);
      assertThat(result).isInstanceOf(List.class);
      assertThat((List<?>) result).hasSize(1);
    });
  }

  @Test
  void convertToRIDFromStringInvalid() {
    // Invalid RID string should return null (logs error)
    final Object result = Type.convert(database, "not-a-rid", RID.class);
    assertThat(result).isEqualTo("not-a-rid");
  }

  @Test
  void convertCompatibleTypes() {
    // An Integer is assignable to Number
    assertThat(Type.convert(database, 42, Number.class)).isEqualTo(42);
  }

  // ───── increment ─────

  @Test
  void incrementNulls() {
    assertThatThrownBy(() -> Type.increment(null, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Type.increment(1, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void incrementIntegerCombinations() {
    assertThat(Type.increment(10, 20)).isEqualTo(30);
    assertThat(Type.increment(10, 20L)).isEqualTo(30L);
    assertThat(Type.increment(10, (short) 5)).isEqualTo(15);
    assertThat(Type.increment(10, 1.5f)).isEqualTo(11.5f);
    assertThat(Type.increment(10, 1.5)).isEqualTo(11.5);
    assertThat(Type.increment(10, new BigDecimal("5"))).isEqualTo(new BigDecimal("15"));
  }

  @Test
  void incrementIntegerOverflow() {
    // Integer + Integer overflow -> upgrade to Long
    final Number result = Type.increment(Integer.MAX_VALUE, 1);
    assertThat(result).isInstanceOf(Long.class);
  }

  @Test
  void incrementLongCombinations() {
    assertThat(Type.increment(10L, 20)).isEqualTo(30L);
    assertThat(Type.increment(10L, 20L)).isEqualTo(30L);
    assertThat(Type.increment(10L, (short) 5)).isEqualTo(15L);
    assertThat(Type.increment(10L, 1.5f)).isEqualTo(11.5f);
    assertThat(Type.increment(10L, 1.5)).isEqualTo(11.5);
    assertThat(Type.increment(10L, new BigDecimal("5"))).isEqualTo(new BigDecimal("15"));
  }

  @Test
  void incrementShortCombinations() {
    assertThat(Type.increment((short) 10, 20)).isEqualTo(30);
    assertThat(Type.increment((short) 10, 20L)).isEqualTo(30L);
    assertThat(Type.increment((short) 10, (short) 5)).isEqualTo(15);
    assertThat(Type.increment((short) 10, 1.5f)).isEqualTo(11.5f);
    assertThat(Type.increment((short) 10, 1.5)).isEqualTo(11.5);
    assertThat(Type.increment((short) 10, new BigDecimal("5"))).isEqualTo(new BigDecimal("15"));
  }

  @Test
  void incrementShortOverflow() {
    // Short + Short overflow -> upgrade to Integer
    final Number result = Type.increment((short) Short.MAX_VALUE, (short) 1);
    assertThat(result.intValue()).isEqualTo(Short.MAX_VALUE + 1);
  }

  @Test
  void incrementFloatCombinations() {
    assertThat(Type.increment(1.5f, 2)).isEqualTo(3.5f);
    assertThat(Type.increment(1.5f, 2L)).isEqualTo(3.5f);
    assertThat(Type.increment(1.5f, (short) 2)).isEqualTo(3.5f);
    assertThat(Type.increment(1.5f, 2.5f)).isEqualTo(4.0f);
    assertThat(Type.increment(1.5f, 2.5)).isEqualTo(4.0);
    assertThat(Type.increment(1.5f, new BigDecimal("2.5"))).isInstanceOf(BigDecimal.class);
  }

  @Test
  void incrementDoubleCombinations() {
    assertThat(Type.increment(1.5, 2)).isEqualTo(3.5);
    assertThat(Type.increment(1.5, 2L)).isEqualTo(3.5);
    assertThat(Type.increment(1.5, (short) 2)).isEqualTo(3.5);
    assertThat(Type.increment(1.5, 2.5f)).isEqualTo(4.0);
    assertThat(Type.increment(1.5, 2.5)).isEqualTo(4.0);
    assertThat(Type.increment(1.5, new BigDecimal("2.5"))).isInstanceOf(BigDecimal.class);
  }

  @Test
  void incrementBigDecimalCombinations() {
    final BigDecimal ten = new BigDecimal("10");
    assertThat(Type.increment(ten, 5)).isEqualTo(new BigDecimal("15"));
    assertThat(Type.increment(ten, 5L)).isEqualTo(new BigDecimal("15"));
    assertThat(Type.increment(ten, (short) 5)).isEqualTo(new BigDecimal("15"));
    assertThat(Type.increment(ten, 2.5f)).isInstanceOf(BigDecimal.class);
    assertThat(Type.increment(ten, 2.5)).isInstanceOf(BigDecimal.class);
    assertThat(Type.increment(ten, new BigDecimal("5"))).isEqualTo(new BigDecimal("15"));
  }

  // ───── decrement ─────

  @Test
  void decrementNulls() {
    assertThatThrownBy(() -> Type.decrement(null, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Type.decrement(1, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decrementIntegerCombinations() {
    assertThat(Type.decrement(30, 20)).isEqualTo(10);
    assertThat(Type.decrement(30, 20L)).isEqualTo(10L);
    assertThat(Type.decrement(30, (short) 5)).isEqualTo(25);
    assertThat(Type.decrement(10, 1.5f)).isEqualTo(8.5f);
    assertThat(Type.decrement(10, 1.5)).isEqualTo(8.5);
    assertThat(Type.decrement(10, new BigDecimal("5"))).isEqualTo(new BigDecimal("5"));
  }

  @Test
  void decrementLongCombinations() {
    assertThat(Type.decrement(30L, 20)).isEqualTo(10L);
    assertThat(Type.decrement(30L, 20L)).isEqualTo(10L);
    assertThat(Type.decrement(30L, (short) 5)).isEqualTo(25L);
    assertThat(Type.decrement(10L, 1.5f)).isEqualTo(8.5f);
    assertThat(Type.decrement(10L, 1.5)).isEqualTo(8.5);
    assertThat(Type.decrement(10L, new BigDecimal("5"))).isEqualTo(new BigDecimal("5"));
  }

  @Test
  void decrementShortCombinations() {
    assertThat(Type.decrement((short) 30, 20)).isEqualTo(10);
    assertThat(Type.decrement((short) 30, 20L)).isEqualTo(10L);
    assertThat(Type.decrement((short) 30, (short) 5)).isEqualTo(25);
    assertThat(Type.decrement((short) 10, 1.5f)).isEqualTo(8.5f);
    assertThat(Type.decrement((short) 10, 1.5)).isEqualTo(8.5);
    assertThat(Type.decrement((short) 10, new BigDecimal("5"))).isEqualTo(new BigDecimal("5"));
  }

  @Test
  void decrementFloatCombinations() {
    assertThat(Type.decrement(3.5f, 2)).isEqualTo(1.5f);
    assertThat(Type.decrement(3.5f, 2L)).isEqualTo(1.5f);
    assertThat(Type.decrement(3.5f, (short) 2)).isEqualTo(1.5f);
    assertThat(Type.decrement(4.0f, 2.5f)).isEqualTo(1.5f);
    assertThat(Type.decrement(4.0f, 2.5)).isEqualTo(1.5);
    assertThat(Type.decrement(1.5f, new BigDecimal("0.5"))).isInstanceOf(BigDecimal.class);
  }

  @Test
  void decrementDoubleCombinations() {
    assertThat(Type.decrement(3.5, 2)).isEqualTo(1.5);
    assertThat(Type.decrement(3.5, 2L)).isEqualTo(1.5);
    assertThat(Type.decrement(3.5, (short) 2)).isEqualTo(1.5);
    assertThat(Type.decrement(4.0, 2.5f)).isEqualTo(1.5);
    assertThat(Type.decrement(4.0, 2.5)).isEqualTo(1.5);
    assertThat(Type.decrement(1.5, new BigDecimal("0.5"))).isInstanceOf(BigDecimal.class);
  }

  @Test
  void decrementBigDecimalCombinations() {
    final BigDecimal ten = new BigDecimal("10");
    assertThat(Type.decrement(ten, 5)).isEqualTo(new BigDecimal("5"));
    assertThat(Type.decrement(ten, 5L)).isEqualTo(new BigDecimal("5"));
    assertThat(Type.decrement(ten, (short) 5)).isEqualTo(new BigDecimal("5"));
    assertThat(Type.decrement(ten, 2.5f)).isInstanceOf(BigDecimal.class);
    assertThat(Type.decrement(ten, 2.5)).isInstanceOf(BigDecimal.class);
    assertThat(Type.decrement(ten, new BigDecimal("5"))).isEqualTo(new BigDecimal("5"));
  }

  // ───── castComparableNumber ─────

  @Test
  void castComparableNumberShort() {
    Number[] result = Type.castComparableNumber((short) 1, 2);
    assertThat(result[0]).isInstanceOf(Integer.class);

    result = Type.castComparableNumber((short) 1, 2L);
    assertThat(result[0]).isInstanceOf(Long.class);

    result = Type.castComparableNumber((short) 1, 2.0f);
    assertThat(result[0]).isInstanceOf(Float.class);

    result = Type.castComparableNumber((short) 1, 2.0);
    assertThat(result[0]).isInstanceOf(Double.class);

    result = Type.castComparableNumber((short) 1, new BigDecimal("2"));
    assertThat(result[0]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber((short) 1, (byte) 2);
    assertThat(result[0]).isInstanceOf(Byte.class);
  }

  @Test
  void castComparableNumberInteger() {
    Number[] result = Type.castComparableNumber(1, 2L);
    assertThat(result[0]).isInstanceOf(Long.class);

    result = Type.castComparableNumber(1, 2.0f);
    assertThat(result[0]).isInstanceOf(Float.class);

    result = Type.castComparableNumber(1, 2.0);
    assertThat(result[0]).isInstanceOf(Double.class);

    result = Type.castComparableNumber(1, new BigDecimal("2"));
    assertThat(result[0]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(1, (short) 2);
    assertThat(result[1]).isInstanceOf(Integer.class);

    result = Type.castComparableNumber(1, (byte) 2);
    assertThat(result[1]).isInstanceOf(Integer.class);
  }

  @Test
  void castComparableNumberLong() {
    Number[] result = Type.castComparableNumber(1L, 2.0f);
    assertThat(result[0]).isInstanceOf(Float.class);

    result = Type.castComparableNumber(1L, 2.0);
    assertThat(result[0]).isInstanceOf(Double.class);

    result = Type.castComparableNumber(1L, new BigDecimal("2"));
    assertThat(result[0]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(1L, 2);
    assertThat(result[1]).isInstanceOf(Long.class);

    result = Type.castComparableNumber(1L, (short) 2);
    assertThat(result[1]).isInstanceOf(Long.class);

    result = Type.castComparableNumber(1L, (byte) 2);
    assertThat(result[1]).isInstanceOf(Long.class);
  }

  @Test
  void castComparableNumberFloat() {
    Number[] result = Type.castComparableNumber(1.0f, 2.0);
    assertThat(result[0]).isInstanceOf(Double.class);

    result = Type.castComparableNumber(1.0f, new BigDecimal("2"));
    assertThat(result[0]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(1.0f, 2);
    assertThat(result[1]).isInstanceOf(Float.class);

    result = Type.castComparableNumber(1.0f, 2L);
    assertThat(result[1]).isInstanceOf(Float.class);

    result = Type.castComparableNumber(1.0f, (short) 2);
    assertThat(result[1]).isInstanceOf(Float.class);
  }

  @Test
  void castComparableNumberDouble() {
    Number[] result = Type.castComparableNumber(1.0, new BigDecimal("2"));
    assertThat(result[0]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(1.0, 2);
    assertThat(result[1]).isInstanceOf(Double.class);

    result = Type.castComparableNumber(1.0, 2L);
    assertThat(result[1]).isInstanceOf(Double.class);

    result = Type.castComparableNumber(1.0, 2.0f);
    assertThat(result[1]).isInstanceOf(Double.class);
  }

  @Test
  void castComparableNumberBigDecimal() {
    Number[] result = Type.castComparableNumber(new BigDecimal("1"), 2);
    assertThat(result[1]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(new BigDecimal("1"), 2.0f);
    assertThat(result[1]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(new BigDecimal("1"), 2.0);
    assertThat(result[1]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(new BigDecimal("1"), (short) 2);
    assertThat(result[1]).isInstanceOf(BigDecimal.class);

    result = Type.castComparableNumber(new BigDecimal("1"), (byte) 2);
    assertThat(result[1]).isInstanceOf(BigDecimal.class);
  }

  @Test
  void castComparableNumberByte() {
    Number[] result = Type.castComparableNumber((byte) 1, (short) 2);
    assertThat(result[0]).isInstanceOf(Short.class);

    result = Type.castComparableNumber((byte) 1, 2);
    assertThat(result[0]).isInstanceOf(Integer.class);

    result = Type.castComparableNumber((byte) 1, 2L);
    assertThat(result[0]).isInstanceOf(Long.class);

    result = Type.castComparableNumber((byte) 1, 2.0f);
    assertThat(result[0]).isInstanceOf(Float.class);

    result = Type.castComparableNumber((byte) 1, 2.0);
    assertThat(result[0]).isInstanceOf(Double.class);

    result = Type.castComparableNumber((byte) 1, new BigDecimal("2"));
    assertThat(result[0]).isInstanceOf(BigDecimal.class);
  }

  @Test
  void castComparableNumberSameTypes() {
    Number[] result = Type.castComparableNumber(1, 2);
    assertThat(result[0]).isEqualTo(1);
    assertThat(result[1]).isEqualTo(2);

    result = Type.castComparableNumber(1L, 2L);
    assertThat(result[0]).isEqualTo(1L);
    assertThat(result[1]).isEqualTo(2L);
  }

  // ───── asString (deprecated) ─────

  @Test
  void asString() {
    assertThat(Type.STRING.asString(42)).isEqualTo("42");
    assertThat(Type.STRING.asString("hello")).isEqualTo("hello");
  }

  // ───── convertToDate (private, tested via convert) ─────

  @Test
  void convertToDateFromStringWithFormat() {
    // Tests the database-aware date format parsing
    final Object result = Type.convert(database, "2024-06-15", Date.class);
    assertThat(result).isInstanceOf(Date.class);
  }

  @Test
  void convertToDateGuessFormatNullDb() {
    // Without database, guesses format: days
    final Object days = Type.convert(null, "2024-06-15", Date.class);
    assertThat(days).isInstanceOf(Date.class);

    // seconds format
    final Object secs = Type.convert(null, "2024-06-15 12:30:00", Date.class);
    assertThat(secs).isInstanceOf(Date.class);

    // millis format
    final Object millis = Type.convert(null, "2024-06-15 12:30:00.000", Date.class);
    assertThat(millis).isInstanceOf(Date.class);
  }

  @Test
  void convertToDateFromUnknownType() {
    // Conversion from unsupported type throws IllegalArgumentException which is re-thrown
    assertThatThrownBy(() -> Type.convert(database, new MultiIterator<>(), Date.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be converted to Date");
  }

  // ───── convert: Long target from date types ─────

  @Test
  void convertDateToLong() {
    final Date now = new Date();
    assertThat(Type.convert(database, now, Long.class)).isEqualTo(now.getTime());
  }
}
