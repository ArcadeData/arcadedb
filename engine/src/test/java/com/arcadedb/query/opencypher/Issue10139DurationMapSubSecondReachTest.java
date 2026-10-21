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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10139: {@code duration({milliseconds: X})} past ~292 years was accepted, and the same duration written with any
 * second field ({@code duration({years: 0, milliseconds: X})}) was refused as an overflow, because the multi-field path summed
 * every sub-second field into one nanosecond count before carrying the whole seconds out of it. A field's reach must not depend
 * on how many other fields the map has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10139DurationMapSubSecondReachTest extends TestHelper {
  private static final String[] SUB_SECOND_FIELDS = { "milliseconds", "microseconds", "nanoseconds" };

  private static final long[] MAGNITUDES = { 0L, 1L, -1L, 999L, 1_000L, 1_001L, 999_999L, 1_000_000L, 1_000_001L, 999_999_999L,
      1_000_000_000L, Integer.MAX_VALUE, 1L << 31, 1L << 53, (1L << 53) + 1, 31_536_000_000_000L,
      Long.MAX_VALUE / 1_000_000L - 1, Long.MAX_VALUE / 1_000_000L, Long.MAX_VALUE / 1_000_000L + 1,
      Long.MAX_VALUE / 1_000L - 1, Long.MAX_VALUE / 1_000L, Long.MAX_VALUE / 1_000L + 1, Long.MAX_VALUE - 1, Long.MAX_VALUE,
      Long.MIN_VALUE + 1, Long.MIN_VALUE, -(Long.MAX_VALUE / 1_000_000L + 1), -(Long.MAX_VALUE / 1_000L + 1), -31_536_000_000_000L };

  @Test
  void theIssueReproduction() {
    final String oneField = durationText("duration({milliseconds: 31536000000000})");
    assertThat(oneField).isEqualTo("PT8760000H");
    assertThat(durationText("duration({years: 0, milliseconds: 31536000000000})")).isEqualTo(oneField);
    assertThat(durationText("duration('PT31536000000S')")).isEqualTo(oneField);
  }

  @Test
  void theBoundaryOfTheNanosecondTotalDoesNotMatter() {
    assertThat(durationText("duration({milliseconds: 9223372036854, days: 0})")).isEqualTo("PT2562047H47M16.854S");
    assertThat(durationText("duration({milliseconds: 9223372036855, days: 0})")).isEqualTo("PT2562047H47M16.855S");
    assertThat(durationText("duration({microseconds: 9223372036854776, days: 0})")).isEqualTo("PT2562047H47M16.854776S");
    assertThat(durationText("duration({microseconds: 9223372036854777, days: 0})")).isEqualTo("PT2562047H47M16.854777S");
  }

  /** The guarantee: one field alone, or the same field with another one that adds nothing, is the same duration or the same refusal. */
  @Test
  void aSecondFieldThatAddsNothingNeverChangesTheAnswer() {
    for (final String field : SUB_SECOND_FIELDS)
      for (final long magnitude : MAGNITUDES) {
        final CypherDuration alone = fromMap(field, magnitude, null);
        for (final String other : new String[] { "days", "years", "seconds", "hours" }) {
          final CypherDuration withOther = fromMap(field, magnitude, other);
          assertThat(withOther).as("%s: %d alone vs with %s: 0", field, magnitude, other).isEqualTo(alone);
        }
      }
  }

  /** The general path with a fractional field must reach as far as the single-field one: the half day is the only difference. */
  @Test
  void aFractionalSecondFieldAddsItsOwnValueAndNothingElse() {
    final long halfDay = 12 * 3600;
    for (final String field : SUB_SECOND_FIELDS)
      for (final long magnitude : MAGNITUDES) {
        final CypherDuration alone = fromMap(field, magnitude, null);
        if (alone == null) {
          assertThat(fromMap(field, magnitude, "fractionalDays")).as("%s: %d", field, magnitude).isNull();
          continue;
        }
        final CypherDuration withHalfDay = fromMap(field, magnitude, "fractionalDays");
        assertThat(withHalfDay).as("%s: %d with days: 0.5", field, magnitude).isNotNull();
        assertThat(withHalfDay.getSeconds()).isEqualTo(alone.getSeconds() + halfDay);
        assertThat(withHalfDay.getNanosAdjustment()).isEqualTo(alone.getNanosAdjustment());
        assertThat(withHalfDay.getDays()).isEqualTo(alone.getDays());
      }
  }

  @Test
  void theSubSecondFieldsAreSummedExactly() {
    // 1 s + 1 ms + 1 us + 1 ns, in every combination of fields that includes a second one
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("days", 0L);
    map.put("milliseconds", 1_001L);
    map.put("microseconds", 1_000_001L);
    map.put("nanoseconds", 1_000_000_001L);
    final CypherDuration sum = CypherDuration.fromMap(map);
    // 1.001 s + 1.000001 s + 1.000000001 s
    assertThat(sum.getSeconds()).isEqualTo(3L);
    assertThat(sum.getNanosAdjustment()).isEqualTo(1_000_000 + 1_000 + 1);

    // the largest of them together: each reaches the whole long range, and the carried seconds are added exactly
    map.put("milliseconds", Long.MAX_VALUE);
    map.put("microseconds", Long.MAX_VALUE);
    map.put("nanoseconds", Long.MAX_VALUE);
    final CypherDuration largest = CypherDuration.fromMap(map);
    final long seconds = Long.MAX_VALUE / 1_000L + Long.MAX_VALUE / 1_000_000L + Long.MAX_VALUE / 1_000_000_000L;
    final long nanos = Long.MAX_VALUE % 1_000L * 1_000_000L + Long.MAX_VALUE % 1_000_000L * 1_000L + Long.MAX_VALUE % 1_000_000_000L;
    assertThat(largest).isEqualTo(new CypherDuration(0, 0, seconds, nanos));
  }

  @Test
  void aFractionalSubSecondValueKeepsItsFractionAtAnyMagnitude() {
    // 4e15 is below 2^52, so the half is representable: the value is 4e12 s and 500000 ns. It was rounded through 4e21 ns,
    // which saturates at Long.MAX_VALUE without a word
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("days", 0L);
    map.put("milliseconds", 4_000_000_000_000_000.5d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 4_000_000_000_000L, 500_000L));

    map.put("milliseconds", 1.5d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 0, 1_500_000L));
    map.put("milliseconds", -1.5d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 0, -1_500_000L));
    map.put("milliseconds", 1_000.25d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 1, 250_000L));

    // the sign is carried by both halves, the whole part and the rounded fraction
    map.put("milliseconds", -4_000_000_000_000_000.5d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, -4_000_000_000_000L, -500_000L));
    map.put("milliseconds", -1_000.25d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, -1, -250_000L));

    map.remove("milliseconds");
    map.put("microseconds", 2_000_000.5d);
    assertThat(CypherDuration.fromMap(map)).isEqualTo(new CypherDuration(0, 0, 2, 500L));
  }

  @Test
  void aDurationTheFieldsCannotHoldIsStillRefused() {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("seconds", Long.MAX_VALUE);
    map.put("milliseconds", 1_000L);
    assertThatThrownBy(() -> CypherDuration.fromMap(map)).isInstanceOf(ArithmeticErrorException.class);

    final Map<String, Object> hours = new LinkedHashMap<>();
    hours.put("hours", Long.MAX_VALUE);
    hours.put("milliseconds", 1L);
    assertThatThrownBy(() -> CypherDuration.fromMap(hours)).isInstanceOf(ArithmeticErrorException.class);

    // a double past the long range is no value either way
    final Map<String, Object> huge = new LinkedHashMap<>();
    huge.put("days", 0L);
    huge.put("milliseconds", 1e300d);
    assertThatThrownBy(() -> CypherDuration.fromMap(huge)).isInstanceOf(ArithmeticErrorException.class);
  }

  /**
   * The duration {@code field: magnitude} alone, or with one more field: {@code null}, a whole {@code 0} for that field, or
   * {@code fractionalDays} for a half day. Null when the map is refused.
   */
  private static CypherDuration fromMap(final String field, final long magnitude, final String other) {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put(field, magnitude);
    if ("fractionalDays".equals(other))
      map.put("days", 0.5d);
    else if (other != null)
      map.put(other, 0L);
    try {
      return CypherDuration.fromMap(map);
    } catch (final ArithmeticErrorException e) {
      return null;
    }
  }

  private String durationText(final String expression) {
    try (final ResultSet resultSet = database.query("opencypher", "RETURN " + expression + " AS d")) {
      return String.valueOf(resultSet.next().<Object>getProperty("d"));
    }
  }
}
