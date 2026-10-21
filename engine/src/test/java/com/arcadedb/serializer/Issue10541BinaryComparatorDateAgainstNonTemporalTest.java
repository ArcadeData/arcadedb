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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10541: {@link BinaryComparator#compareTo(Object, Object)} on a date and an operand that has no timestamp (a list, a map) threw a
 * {@code NullPointerException} unboxing the answer of the timestamp helper, which is {@code null} for them. A Boolean already got the
 * "not supported" refusal (issue #7754), which the SQL operators catch to report "not comparable"; every other operand without a
 * timestamp gets the same one, in both directions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10541BinaryComparatorDateAgainstNonTemporalTest {

  private static final LocalDate     DATE      = LocalDate.of(2020, 1, 1);
  private static final LocalDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 0, 0);

  @Test
  void aDateAgainstAListOrAMapIsRefusedInBothDirections() {
    for (final Object date : new Object[] { DATE, DATE_TIME })
      for (final Object other : new Object[] { List.of(7), Map.of("a", 7) }) {
        assertThatThrownBy(() -> BinaryComparator.compareTo(date, other)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not supported");
        assertThatThrownBy(() -> BinaryComparator.compareTo(other, date)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not supported");
      }
  }

  @Test
  void aBooleanKeepsItsRefusal() {
    assertThatThrownBy(() -> BinaryComparator.compareTo(DATE, true)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void twoDatesStillCompare() {
    assertThat(BinaryComparator.compareTo(DATE, DATE.plusDays(1))).isNegative();
    assertThat(BinaryComparator.compareTo(DATE_TIME.plusHours(1), DATE_TIME)).isPositive();
    assertThat(BinaryComparator.compareTo(DATE, DATE_TIME)).isZero();
  }
}
