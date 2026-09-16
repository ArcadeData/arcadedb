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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7614: {@link Type#castComparableNumber} brought an {@code Integer}/{@code Long} and
 * a {@code Float} to a common type by narrowing the integral operand into float's 24-bit mantissa instead of
 * widening both to {@code double}. Every {@code long} above 2^24 collapses onto the nearest representable float,
 * so a comparison against a {@code FLOAT} property reported equality - and wrong ordering - for a whole band of
 * distinct integers. Fixed by promoting both operands to {@code double}, the same treatment
 * {@code BinaryComparator.compareWideningLong}/{@code compareNarrowIntegral} already apply to the same pair.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7614CastComparableNumberFloatNarrowingTest extends TestHelper {

  @Test
  void aLongAboveTheFloatMantissaIsNotCollapsedOntoTheNearestFloat() {
    final Number[] couple = Type.castComparableNumber(16777217L, 1.0f);
    assertThat(couple[0].doubleValue()).isEqualTo(16777217.0d);

    // The consequence: two distinct longs must not compare equal against the same FLOAT
    assertThat(Type.castComparableNumber(16777217L, 16777216.0f)[0])
        .isNotEqualTo(Type.castComparableNumber(16777217L, 16777216.0f)[1]);
  }

  @Test
  void integerMaxValueIsNotCollapsedOntoTheNearestFloat() {
    final Number[] couple = Type.castComparableNumber(Integer.MAX_VALUE, 1.0f);
    assertThat(couple[0].doubleValue()).isEqualTo((double) Integer.MAX_VALUE);
  }

  @Test
  void endToEndAFloatColumnDoesNotMatchAWrongLong() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE FloatVsLong");
      database.command("sql", "CREATE PROPERTY FloatVsLong.f FLOAT");
      database.newDocument("FloatVsLong").set("f", 16777216.0f).save();
    });

    // 16777217 is not 16777216: a LONG literal one above the stored FLOAT must not match it
    final ResultSet rs = database.query("sql", "SELECT FROM FloatVsLong WHERE f = 16777217");
    assertThat(rs.hasNext()).as("distinct long must not equal a different FLOAT").isFalse();

    final ResultSet rsEq = database.query("sql", "SELECT FROM FloatVsLong WHERE f = 16777216");
    assertThat(rsEq.hasNext()).as("the identical long must still equal the FLOAT").isTrue();
  }
}
