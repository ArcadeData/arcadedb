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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #13029: {@code convert('LONG')} answered a wrapped or clamped number for a value outside the 64-bit
 * range, where {@code convert('INTEGER')}, {@code convert('SHORT')} and {@code convert('BYTE')} answer {@code null} as the method
 * documents ("a value that cannot be expressed in the target type answers null for that row").
 */
class SQLMethodConvertLongRangeTest extends TestHelper {

  private Object convert(final Object value, final String target) {
    try (final ResultSet rs = database.query("sql", "SELECT :v.convert('" + target + "') AS r", Map.of("v", value))) {
      return rs.next().getProperty("r");
    }
  }

  @Test
  void aBigIntegerBeyondTheLongRangeAnswersNull() {
    assertThat(convert(new BigInteger("18446744073709551617"), "LONG")).isNull();
    assertThat(convert(new BigInteger("-18446744073709551617"), "LONG")).isNull();
    assertThat(convert(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), "LONG")).isNull();
    assertThat(convert(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), "LONG")).isNull();
  }

  @Test
  void aBigDecimalBeyondTheLongRangeAnswersNull() {
    assertThat(convert(new BigDecimal("18446744073709551617.5"), "LONG")).isNull();
    assertThat(convert(new BigDecimal("-18446744073709551617.5"), "LONG")).isNull();
    assertThat(convert(new BigDecimal("9223372036854775808"), "LONG")).isNull();
    assertThat(convert(new BigDecimal("-9223372036854775809"), "LONG")).isNull();
    assertThat(convert(new BigDecimal("1E+400"), "LONG")).isNull();
    assertThat(convert(new BigDecimal("-1E+400"), "LONG")).isNull();
  }

  @Test
  void aDoubleOrFloatBeyondTheLongRangeAnswersNull() {
    assertThat(convert(1e30, "LONG")).isNull();
    assertThat(convert(-1e30, "LONG")).isNull();
    assertThat(convert(0x1p63, "LONG")).isNull();
    assertThat(convert(Double.POSITIVE_INFINITY, "LONG")).isNull();
    assertThat(convert(Double.NEGATIVE_INFINITY, "LONG")).isNull();
    assertThat(convert(Double.NaN, "LONG")).isNull();
    assertThat(convert(1e30f, "LONG")).isNull();
    assertThat(convert(0x1p63f, "LONG")).isNull();
    assertThat(convert(-1e30f, "LONG")).isNull();
  }

  @Test
  void theLastValuesOfTheLongRangeAreStillConverted() {
    assertThat(convert(BigInteger.valueOf(Long.MAX_VALUE), "LONG")).isEqualTo(Long.MAX_VALUE);
    assertThat(convert(BigInteger.valueOf(Long.MIN_VALUE), "LONG")).isEqualTo(Long.MIN_VALUE);
    assertThat(convert(new BigDecimal("9223372036854775807"), "LONG")).isEqualTo(Long.MAX_VALUE);
    assertThat(convert(new BigDecimal("-9223372036854775808"), "LONG")).isEqualTo(Long.MIN_VALUE);
    // THE FRACTION IS DISCARDED, SO THESE TRUNCATE TO THE EDGES OF THE RANGE, AS THEY DO FOR A LONG PROPERTY
    assertThat(convert(new BigDecimal("9223372036854775807.5"), "LONG")).isEqualTo(Long.MAX_VALUE);
    assertThat(convert(new BigDecimal("-9223372036854775808.5"), "LONG")).isEqualTo(Long.MIN_VALUE);
    // THE LAST DOUBLE BELOW 2^63 AND EXACTLY -2^63
    assertThat(convert(Math.nextDown(0x1p63), "LONG")).isEqualTo(9223372036854774784L);
    assertThat(convert(-0x1p63, "LONG")).isEqualTo(Long.MIN_VALUE);
    assertThat(convert(Math.nextDown(0x1p63f), "LONG")).isEqualTo(9223371487098961920L);
    assertThat(convert(-0x1p63f, "LONG")).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void ordinaryValuesAreConvertedAsBefore() {
    assertThat(convert(42, "LONG")).isEqualTo(42L);
    assertThat(convert("42", "LONG")).isEqualTo(42L);
    assertThat(convert(1.9, "LONG")).isEqualTo(1L);
    assertThat(convert(-1.9f, "LONG")).isEqualTo(-1L);
    assertThat(convert(new BigDecimal("5.5"), "LONG")).isEqualTo(5L);
    assertThat(convert(new BigInteger("5"), "LONG")).isEqualTo(5L);
    assertThat(convert(Long.MAX_VALUE, "LONG")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void theJavaClassNameOfTheTargetIsHeldToTheSameRange() {
    assertThat(convert(new BigInteger("18446744073709551617"), "java.lang.Long")).isNull();
    assertThat(convert(1e30, "java.lang.Long")).isNull();
    assertThat(convert(new BigInteger("5"), "java.lang.Long")).isEqualTo(5L);
  }

  @Test
  void aStoredDecimalBeyondTheLongRangeAnswersNull() {
    database.getSchema().createDocumentType("Amounts").createProperty("amount", Type.DECIMAL);
    database.transaction(() -> {
      database.newDocument("Amounts").set("id", 1).set("amount", new BigDecimal("18446744073709551617.5")).save();
      database.newDocument("Amounts").set("id", 2).set("amount", new BigDecimal("42.5")).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT id, amount.convert('LONG') AS r FROM Amounts ORDER BY id")) {
      assertThat(rs.next().<Object>getProperty("r")).isNull();
      assertThat(rs.next().<Object>getProperty("r")).isEqualTo(42L);
    }
  }

  @Test
  void theNarrowerIntegralTargetsKeepAnsweringNull() {
    // THE CONTRACT THE LONG TARGET NOW SHARES
    assertThat(convert(new BigInteger("18446744073709551617"), "INTEGER")).isNull();
    assertThat(convert(new BigInteger("18446744073709551617"), "SHORT")).isNull();
    assertThat(convert(new BigInteger("18446744073709551617"), "BYTE")).isNull();
    assertThat(convert(1e30, "INTEGER")).isNull();
  }
}
