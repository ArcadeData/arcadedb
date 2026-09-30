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
package com.arcadedb.serializer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7754: a BOOLEAN compared against a timestamp type answered one way forwards and threw
 * {@code NullPointerException} backwards. The comparison has no meaning (true is not "one millisecond after the
 * epoch"), so both directions now refuse it with the same {@code IllegalArgumentException} naming both types, which
 * the SQL comparison operators already turn into "no match".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7754BooleanVersusTemporalComparisonTest {
  private final BinaryComparator comparator = new BinaryComparator();

  @ParameterizedTest
  @ValueSource(bytes = { BinaryTypes.TYPE_DATE, BinaryTypes.TYPE_DATETIME, BinaryTypes.TYPE_DATETIME_SECOND, BinaryTypes.TYPE_DATETIME_MICROS,
      BinaryTypes.TYPE_DATETIME_NANOS })
  void booleanAgainstTemporalIsRefusedInBothDirections(final byte temporalType) {
    assertThatThrownBy(() -> comparator.compare(true, BinaryTypes.TYPE_BOOLEAN, 5L, temporalType))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> comparator.compare(5L, temporalType, true, BinaryTypes.TYPE_BOOLEAN))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(bytes = { BinaryTypes.TYPE_INT, BinaryTypes.TYPE_LONG })
  void booleanAgainstNumericStillAgreesInBothDirections(final byte numericType) {
    final Object one = numericType == BinaryTypes.TYPE_INT ? (Object) 1 : (Object) 1L;
    assertThat(comparator.compare(true, BinaryTypes.TYPE_BOOLEAN, one, numericType)).isZero();
    assertThat(comparator.compare(one, numericType, true, BinaryTypes.TYPE_BOOLEAN)).isZero();
  }
}
