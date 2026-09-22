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
package com.arcadedb.utility;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8216: every date/time conversion read a {@link Number} with {@link Number#longValue()}, which truncates a
 * fractional value toward zero (the LATER unit for a pre-epoch instant), maps {@code NaN} to the epoch and wraps a
 * {@link BigInteger} outside the {@code long} range. A fractional value is now floored (the unit containing the
 * instant) and a value that denotes no instant is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8216FractionalTimestampTest extends TestHelper {

  @Test
  void fractionalNumberIsFlooredNotTruncated() {
    assertThat(DateUtils.dateTimeToTimestamp(1770000000000.9d, ChronoUnit.MILLIS)).isEqualTo(1770000000000L);
    assertThat(DateUtils.dateTimeToTimestamp(1770000000000.9f, ChronoUnit.MILLIS)).isEqualTo((long) Math.floor(1770000000000.9f));
    // PRE-EPOCH: -1.5 ms is an instant inside millisecond -2, truncation toward zero answered -1
    assertThat(DateUtils.dateTimeToTimestamp(-1.5d, ChronoUnit.MILLIS)).isEqualTo(-2L);
    assertThat(DateUtils.dateTimeToTimestamp(new BigDecimal("-1.5"), ChronoUnit.MILLIS)).isEqualTo(-2L);
    assertThat(DateUtils.dateTimeToTimestamp(new BigDecimal("1770000000000.9"), ChronoUnit.MILLIS)).isEqualTo(1770000000000L);
    // AN INTEGRAL VALUE IS UNCHANGED
    assertThat(DateUtils.dateTimeToTimestamp(-5.0d, ChronoUnit.MILLIS)).isEqualTo(-5L);
    assertThat(DateUtils.dateTimeToTimestamp(Long.MIN_VALUE, ChronoUnit.MILLIS)).isEqualTo(Long.MIN_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(BigInteger.valueOf(Long.MAX_VALUE), ChronoUnit.MILLIS)).isEqualTo(Long.MAX_VALUE);
    // A DATE IS EPOCH DAYS: -0.5 is the day before the epoch
    assertThat(DateUtils.dateToEpochDays(-0.5d)).isEqualTo(-1L);
  }

  @Test
  void valueDenotingNoInstantIsRefused() {
    for (final Number n : new Number[] { Double.NaN, Float.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1e19d,
        -1e19d, BigInteger.ONE.shiftLeft(64), new BigDecimal("1e30") })
      assertThatThrownBy(() -> DateUtils.dateTimeToTimestamp(n, ChronoUnit.MILLIS))
          .as("value %s", n).isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> DateUtils.dateToEpochDays(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> DateUtils.format(Double.NaN, "yyyy-MM-dd")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void propertyCoercionFloorsAndRefuses() {
    final DocumentType type = database.getSchema().createDocumentType("Ev8216");
    type.createProperty("ts", Type.DATETIME);
    type.createProperty("d", Type.DATE);

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Ev8216");
      doc.set("ts", -1.5d);
      doc.set("d", 1.5d);
      doc.save();

      assertThat(doc.getLocalDateTime("ts")).isEqualTo(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).minusNanos(2_000_000));
      assertThat(doc.getLocalDate("d")).isEqualTo(LocalDate.ofEpochDay(1));

      assertThatThrownBy(() -> database.newDocument("Ev8216").set("ts", Double.NaN)).isInstanceOf(IllegalArgumentException.class);
    });
  }
}
