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
import com.arcadedb.exception.ValueOutOfRangeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The range check of a number written to an integral property (BYTE, SHORT, INTEGER, LONG) is one rule for every source class: the value is
 * accepted when its truncation toward zero is in the range of the property, so {@code 127.5} is a BYTE (127) and {@code 128.0} is not, and
 * {@code 9223372036854775807.5} is a LONG (9223372036854775807) and {@code 9223372036854775808} is not. NaN and the infinities are refused.
 */
class IntegralNarrowingBoundaryTest extends TestHelper {

  private static final Map<Type, long[]> RANGES = new LinkedHashMap<>();

  static {
    RANGES.put(Type.BYTE, new long[] { Byte.MIN_VALUE, Byte.MAX_VALUE });
    RANGES.put(Type.SHORT, new long[] { Short.MIN_VALUE, Short.MAX_VALUE });
    RANGES.put(Type.INTEGER, new long[] { Integer.MIN_VALUE, Integer.MAX_VALUE });
    RANGES.put(Type.LONG, new long[] { Long.MIN_VALUE, Long.MAX_VALUE });
  }

  /** The values around the two ends of every range, as every class a number reaches a property in. */
  private static List<Number> boundaryValues(final long[] range) {
    final List<Number> values = new ArrayList<>();
    for (final long end : range) {
      final BigDecimal exact = new BigDecimal(end);
      final BigDecimal direction = BigDecimal.valueOf(end < 0 ? -1 : 1);
      for (final String step : new String[] { "-2", "-1", "-0.9999", "-0.5", "0", "0.5", "0.9999", "1", "1.5", "2" }) {
        final BigDecimal value = exact.add(direction.multiply(new BigDecimal(step)));
        values.add(value);
        values.add(value.toBigInteger());
        // a double or a float is the value its class can hold: the nearest one, whose truncation the check judges
        values.add(value.doubleValue());
        values.add(value.floatValue());
      }
    }
    return values;
  }

  private static BigInteger truncation(final Number value) {
    return switch (value) {
      case Double d -> new BigDecimal(d).setScale(0, RoundingMode.DOWN).toBigInteger();
      case Float f -> new BigDecimal(f.doubleValue()).setScale(0, RoundingMode.DOWN).toBigInteger();
      case BigDecimal b -> b.setScale(0, RoundingMode.DOWN).toBigInteger();
      case BigInteger b -> b;
      default -> BigInteger.valueOf(value.longValue());
    };
  }

  private static boolean isFinite(final Number value) {
    return !(value instanceof Double d && (d.isNaN() || d.isInfinite())) && !(value instanceof Float f && (f.isNaN() || f.isInfinite()));
  }

  @Test
  void aNumberIsAcceptedWhenItsTruncationIsInTheRangeOfTheProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Holder");
    int checked = 0;
    final List<String> wrong = new ArrayList<>();
    for (final Map.Entry<Type, long[]> range : RANGES.entrySet()) {
      final Property property = type.createProperty("p_" + range.getKey().name().toLowerCase(), range.getKey());
      final BigInteger min = BigInteger.valueOf(range.getValue()[0]);
      final BigInteger max = BigInteger.valueOf(range.getValue()[1]);
      for (final Number value : boundaryValues(range.getValue())) {
        final BigInteger truncated = truncation(value);
        final boolean expected = truncated.compareTo(min) >= 0 && truncated.compareTo(max) <= 0;
        final Object converted;
        try {
          converted = Type.convert(database, value, range.getKey().getDefaultJavaType(), property);
        } catch (final ValueOutOfRangeException e) {
          if (expected)
            wrong.add(range.getKey() + " refused " + value.getClass().getSimpleName() + " " + value + " (truncation " + truncated + " is in range)");
          checked++;
          continue;
        }
        if (!expected)
          wrong.add(range.getKey() + " accepted " + value.getClass().getSimpleName() + " " + value + " as " + converted + " (truncation " + truncated + " is out of range)");
        else if (!converted.toString().equals(truncated.toString()))
          wrong.add(range.getKey() + " read " + value.getClass().getSimpleName() + " " + value + " as " + converted + ", expected " + truncated);
        checked++;
      }
    }
    assertThat(checked).as("values checked").isGreaterThan(200);
    assertThat(wrong).isEmpty();
  }

  @Test
  void nonFiniteNumbersAreRefusedByEveryIntegralProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Holder");
    for (final Type target : RANGES.keySet()) {
      final Property property = type.createProperty("p_" + target.name().toLowerCase(), target);
      for (final Number value : new Number[] { Double.NaN, Float.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY })
        assertThatThrownBy(() -> Type.convert(database, value, target.getDefaultJavaType(), property)).as("%s <- %s", target, value)
            .isInstanceOf(RuntimeException.class);
    }
  }
}
