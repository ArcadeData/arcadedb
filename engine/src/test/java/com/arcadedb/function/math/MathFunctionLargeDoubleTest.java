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
package com.arcadedb.function.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4391: MathUnaryFunction / MathBinaryFunction must not
 * saturate large doubles to Long.MAX_VALUE / Long.MIN_VALUE when the double result
 * exceeds the long range.
 */
class MathFunctionLargeDoubleTest {

  // ---- MathUnaryFunction (floor) ----

  @Test
  void floorLargePositiveDoubleReturnsDouble() {
    final MathUnaryFunction floor = new MathUnaryFunction("floor", Math::floor);
    // 1e30 is far above Long.MAX_VALUE (~9.2e18); must stay a Double, not saturate
    final Object result = floor.execute(new Object[] { 1e30 }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(1e30);
  }

  @Test
  void floorLargeNegativeDoubleReturnsDouble() {
    final MathUnaryFunction floor = new MathUnaryFunction("floor", Math::floor);
    // -1e30 is far below Long.MIN_VALUE; must stay a Double
    final Object result = floor.execute(new Object[] { -1e30 }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(-1e30);
  }

  @Test
  void ceilLargePositiveDoubleReturnsDouble() {
    final MathUnaryFunction ceil = new MathUnaryFunction("ceil", Math::ceil);
    final Object result = ceil.execute(new Object[] { 1e30 }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(1e30);
  }

  @Test
  void floorSmallWholeNumberReturnsDouble() {
    // Issue #5382: the Cypher return type of floor() is FLOAT, so whole-number results
    // must stay Double and never collapse to Long (it silently changed division semantics)
    final MathUnaryFunction floor = new MathUnaryFunction("floor", Math::floor);
    final Object result = floor.execute(new Object[] { 42.7 }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(42.0);
  }

  @Test
  void floorValueJustBelowLongMaxReturnsDouble() {
    final MathUnaryFunction floor = new MathUnaryFunction("floor", Math::floor);
    final Object result = floor.execute(new Object[] { 1.5e18 }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(1.5e18);
  }

  @Test
  void floorPositiveInfinityReturnsDouble() {
    final MathUnaryFunction floor = new MathUnaryFunction("floor", Math::floor);
    final Object result = floor.execute(new Object[] { Double.POSITIVE_INFINITY }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(Double.POSITIVE_INFINITY);
  }

  @Test
  void floorNegativeInfinityReturnsDouble() {
    final MathUnaryFunction floor = new MathUnaryFunction("floor", Math::floor);
    final Object result = floor.execute(new Object[] { Double.NEGATIVE_INFINITY }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(Double.NEGATIVE_INFINITY);
  }

  // ---- MathBinaryFunction (atan2) ----

  @Test
  void binaryFunctionLargeDoubleResultReturnsDouble() {
    // atan2(1e30, 1e30) = pi/4; result is small so this stays as Long (pi/4 != floor(pi/4))
    // Use a case where result is a whole large number: not straightforward with atan2.
    // Instead test the product-like function with large operands that might produce whole results.
    // Simplest approach: supply a custom binary op that returns a large whole double.
    final MathBinaryFunction largeWhole = new MathBinaryFunction("test", (a, b) -> 1e30);
    final Object result = largeWhole.execute(new Object[] { 1.0, 2.0 }, null);
    assertThat(result).isInstanceOf(Double.class);
    assertThat((Double) result).isEqualTo(1e30);
  }

  // ---- Integration via Cypher query ----

  @Test
  void cypherFloorLargeDoubleReturnsDouble() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testMathLargeDouble", db -> {
      try (final ResultSet rs = db.query("opencypher", "RETURN floor(1e30) AS v")) {
        assertThat(rs.hasNext()).isTrue();
        final Object v = rs.next().getProperty("v");
        assertThat(v).isInstanceOf(Double.class);
        assertThat((Double) v).isEqualTo(1e30);
      }
    });
  }

  @Test
  void cypherCeilLargeDoubleReturnsDouble() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testMathLargeDoubleCeil", db -> {
      try (final ResultSet rs = db.query("opencypher", "RETURN ceil(-1e30) AS v")) {
        assertThat(rs.hasNext()).isTrue();
        final Object v = rs.next().getProperty("v");
        assertThat(v).isInstanceOf(Double.class);
        assertThat((Double) v).isEqualTo(-1e30);
      }
    });
  }
}
