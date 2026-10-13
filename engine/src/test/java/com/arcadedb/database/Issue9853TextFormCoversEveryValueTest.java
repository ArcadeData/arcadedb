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
package com.arcadedb.database;

import com.arcadedb.query.opencypher.temporal.CypherDuration;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9853: a REGEXP on a number or a temporal is refused when no text of the type's form matches it, so the form must
 * describe EVERY text a value of the class is written as. A value whose text falls outside it would have a pattern it
 * satisfies refused at declaration time. This pins the form against the edge values of every class.
 */
class Issue9853TextFormCoversEveryValueTest {

  @Test
  void everyValueIsWrittenInsideTheFormOfItsClass() {
    final List<Object> values = List.of(//
        Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0,//
        Short.MIN_VALUE, Short.MAX_VALUE,//
        Integer.MIN_VALUE, Integer.MAX_VALUE,//
        Long.MIN_VALUE, Long.MAX_VALUE, 0L,//
        Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, Float.MAX_VALUE, -0.0f, 1e-5f, 1.5f,//
        Double.NaN, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE, -1.0e300, 1e7, 0.001,//
        new BigDecimal("1E+3"), new BigDecimal("-1.2E-7"), new BigDecimal("0E-10"), new BigDecimal("-123.450"), BigDecimal.ZERO,
        LocalDate.MIN, LocalDate.MAX, LocalDate.of(2024, 1, 15), LocalDate.of(-1, 1, 1),//
        LocalDateTime.MIN, LocalDateTime.MAX, LocalDateTime.of(2024, 1, 15, 0, 0), LocalDateTime.of(2024, 1, 15, 10, 30, 5, 1_000),
        LocalDateTime.of(2024, 1, 15, 10, 30, 0, 1),//
        Instant.EPOCH, Instant.MIN, Instant.MAX, Instant.ofEpochSecond(1, 500),//
        OffsetDateTime.MIN, OffsetDateTime.MAX, OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.ofHoursMinutesSeconds(1, 2, 3)),
        ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("Europe/Rome")),
        ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("Etc/GMT+5")),
        ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("America/Port-au-Prince")),
        ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("America/Argentina/Buenos_Aires")),
        ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("UTC")),//
        LocalTime.MIDNIGHT, LocalTime.MAX, LocalTime.of(10, 30, 5),//
        OffsetTime.MIN, OffsetTime.MAX, OffsetTime.of(10, 30, 0, 0, ZoneOffset.UTC),//
        new Date(0), new Date(Long.MAX_VALUE / 1000), new Date(-62_135_769_600_000L),//
        new CypherDuration(0, 0, 0, 0), new CypherDuration(14, 10, 3_725, 500_000_000), new CypherDuration(-14, -10, -3_725, 0),
        new CypherDuration(0, 0, -1, 500_000_000));

    for (final Object value : values)
      assertCovered(value);

    // java.util.Date writes the JVM's default zone, by its short name
    final TimeZone defaultZone = TimeZone.getDefault();
    try {
      for (final String zone : List.of("Asia/Kolkata", "Pacific/Guam", "America/St_Johns", "Australia/Lord_Howe", "GMT")) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        assertCovered(new Date(1_700_000_000_000L));
      }
    } finally {
      TimeZone.setDefault(defaultZone);
    }
  }

  private static void assertCovered(final Object value) {
    final TextForm form = TextForm.of(value.getClass());
    final String text = value.toString();
    assertThat(form).as(value.getClass().getSimpleName()).isNotNull();
    assertThat(form.shape().matcher(text).matches()).as("'%s' of the shape '%s'", text, form.shape().pattern()).isTrue();
    for (int i = 0; i < text.length(); i++)
      assertThat(form.characters()).as("'%s' written with '%s'", text, form.characters()).contains(String.valueOf(text.charAt(i)));
  }
}
