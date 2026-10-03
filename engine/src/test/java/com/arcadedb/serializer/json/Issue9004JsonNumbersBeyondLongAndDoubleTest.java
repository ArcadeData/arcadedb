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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issues #9004 (an integer beyond the long range wrapped around to its low 64 bits, a decimal with more digits
 * than a double holds was narrowed to a double), #9003 (a numeric array was narrowed to float32) and #9002 (a primitive array written
 * to a LIST property became its one element).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9004JsonNumbersBeyondLongAndDoubleTest extends TestHelper {

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
  void tokenLengthBoundaryAndExponentOverflow() {
    // 15 characters: always a double. 16 characters that a double does not hold exactly: exact. 1e400 overflows a double: exact
    final Map<String, Object> map = new JSONObject("{\"a\":0.1234567890123,\"b\":0.12345678901234567,\"c\":0.1234567890123456789,\"d\":1e400}").toMap();
    assertThat(map.get("a")).isEqualTo(0.1234567890123);
    // the shortest rendering of that double is ...66, so the token holds a digit the double loses
    assertThat(map.get("b")).isEqualTo(new BigDecimal("0.12345678901234567"));
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
