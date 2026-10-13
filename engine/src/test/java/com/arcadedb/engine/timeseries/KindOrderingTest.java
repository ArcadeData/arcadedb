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
package com.arcadedb.engine.timeseries;

import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code ColumnDefinition.storableValue} tells a numeric column from the others by {@code kind <= KIND_BYTE}, so the numeric kinds
 * must stay numerically below BOOLEAN, STRING and TEXT. Renumbering the constants would turn that test into a wrong answer for
 * every value of every write, without failing anything else (issue #9809).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class KindOrderingTest {
  private static final Set<Type> NUMERIC = EnumSet.of(Type.DOUBLE, Type.FLOAT, Type.LONG, Type.INTEGER, Type.SHORT, Type.BYTE, Type.DATE,
      Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS);

  @Test
  void everyNumericTypeHasAKindUpToByte() {
    for (final Type type : NUMERIC)
      assertThat(TimeSeriesBatch.kindOf(type)).as(type.name()).isLessThanOrEqualTo(TimeSeriesBatch.KIND_BYTE);
  }

  @Test
  void everyOtherTypeHasAKindAfterByte() {
    for (final Type type : Type.values())
      if (!NUMERIC.contains(type))
        assertThat(TimeSeriesBatch.kindOf(type)).as(type.name()).isGreaterThan(TimeSeriesBatch.KIND_BYTE);
  }

  @Test
  void theNumericKindsAreContiguousFromZero() {
    assertThat(new int[] { TimeSeriesBatch.KIND_DOUBLE, TimeSeriesBatch.KIND_FLOAT, TimeSeriesBatch.KIND_LONG, TimeSeriesBatch.KIND_INTEGER,
        TimeSeriesBatch.KIND_SHORT, TimeSeriesBatch.KIND_BYTE }).containsExactly(0, 1, 2, 3, 4, 5);
    assertThat(TimeSeriesBatch.KIND_BOOLEAN).isGreaterThan(TimeSeriesBatch.KIND_BYTE);
    assertThat(TimeSeriesBatch.KIND_STRING).isGreaterThan(TimeSeriesBatch.KIND_BYTE);
    assertThat(TimeSeriesBatch.KIND_TEXT).isGreaterThan(TimeSeriesBatch.KIND_BYTE);
  }
}
