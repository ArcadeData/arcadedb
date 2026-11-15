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
package com.arcadedb.postgres;

import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11960: a timestamptz, time, timetz, interval or jsonb parameter failed the Bind with "Type with code N not supported for
 * deserializing" (XX000). Each OID now decodes, in text and binary format, to the engine value of the matching type.
 */
class Issue11960BindTemporalTypesTest {

  private static Object text(final int oid, final String value) {
    return PostgresType.deserialize(oid, 0, value.getBytes(StandardCharsets.UTF_8));
  }

  private static Object binary(final int oid, final byte[] value) {
    return PostgresType.deserialize(oid, 1, value);
  }

  private static CypherDuration duration(final long months, final long days, final long seconds, final long nanos) {
    return new CypherDuration(months, days, seconds, nanos);
  }

  @Test
  void timestamptzKeepsTheOffsetItWasSentWith() {
    final ZonedDateTime expected = LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000).atZone(ZoneOffset.ofHours(2));
    assertThat(text(1184, "2024-02-29 13:14:15.123456+02")).isEqualTo(expected);
    assertThat(text(1184, "2024-02-29 13:14:15.123456+02:00")).isEqualTo(expected);
    assertThat(text(1184, "2024-02-29T13:14:15.123456+0200")).isEqualTo(expected);
    assertThat(text(1184, "2024-02-29 13:14:15Z")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 14, 15).atZone(ZoneOffset.UTC));
    assertThat(text(1184, "2024-02-29 13:14:15")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 14, 15).atZone(ZoneOffset.UTC));

    // binary: microseconds since 2000-01-01 UTC, the same instant read at UTC
    final long micros = (expected.toEpochSecond() - 946684800L) * 1_000_000L + 123_456L;
    final Object decoded = binary(1184, ByteBuffer.allocate(8).putLong(micros).array());
    assertThat(((ZonedDateTime) decoded).toInstant()).isEqualTo(expected.toInstant());
  }

  @Test
  void timeAndTimetz() {
    assertThat(text(1083, "10:15:30")).isEqualTo(LocalTime.of(10, 15, 30));
    assertThat(text(1083, "10:15:30.000004")).isEqualTo(LocalTime.of(10, 15, 30, 4_000));
    assertThat(text(1266, "10:15:30+02")).isEqualTo(OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHours(2)));
    assertThat(text(1266, "10:15:30-05:30")).isEqualTo(OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHoursMinutes(-5, -30)));

    final long micros = LocalTime.of(10, 15, 30).toNanoOfDay() / 1_000L;
    assertThat(binary(1083, ByteBuffer.allocate(8).putLong(micros).array())).isEqualTo(LocalTime.of(10, 15, 30));
    // timetz stores the offset in seconds WEST of UTC: +02:00 is -7200
    assertThat(binary(1266, ByteBuffer.allocate(12).putLong(micros).putInt(-7200).array()))
        .isEqualTo(OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHours(2)));
  }

  @Test
  void intervalInEveryStyleAClientSends() {
    // psycopg (postgres style, str(timedelta))
    assertThat(text(1186, "1 day 0:00:05")).isEqualTo(duration(0, 1, 5, 0));
    assertThat(text(1186, "-1 day 23:59:55")).isEqualTo(duration(0, -1, 86395, 0));
    assertThat(text(1186, "0:00:00.000004")).isEqualTo(duration(0, 0, 0, 4_000));
    // psycopg sql_standard
    assertThat(text(1186, "+1 day +5 second +0 microsecond")).isEqualTo(duration(0, 1, 5, 0));
    // pgjdbc PGInterval
    assertThat(text(1186, "0 years 2 mons 1 days 3 hours 0 mins 5.5 secs")).isEqualTo(duration(2, 1, 3 * 3600 + 5, 500_000_000));
    // postgres_verbose and ISO 8601
    assertThat(text(1186, "@ 1 year 2 days ago")).isEqualTo(duration(-12, -2, 0, 0));
    assertThat(text(1186, "P1DT5S")).isEqualTo(duration(0, 1, 5, 0));
    // a fraction of a month spills into days of 30, a fraction of a day into hours
    assertThat(text(1186, "1.5 months")).isEqualTo(duration(1, 15, 0, 0));
    assertThat(text(1186, "1.5 days")).isEqualTo(duration(0, 1, 12 * 3600, 0));

    assertThat(binary(1186, ByteBuffer.allocate(16).putLong(5_000_004L).putInt(1).putInt(2).array()))
        .isEqualTo(duration(2, 1, 5, 4_000));

    assertThatThrownBy(() -> text(1186, "5 fortnights")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void intervalSqlStandardFields() {
    assertThat(text(1186, "1-2")).isEqualTo(duration(14, 0, 0, 0));
    assertThat(text(1186, "-1-2")).isEqualTo(duration(-14, 0, 0, 0));
    assertThat(text(1186, "3 4:05:06")).isEqualTo(duration(0, 3, 4 * 3600 + 5 * 60 + 6, 0));
    assertThat(text(1186, "1-2 3 4:05:06")).isEqualTo(duration(14, 3, 4 * 3600 + 5 * 60 + 6, 0));
    // a lone number is seconds
    assertThat(text(1186, "5")).isEqualTo(duration(0, 0, 5, 0));
  }

  @Test
  void isoFractionalMonthSpillsIntoDaysOfThirty() {
    // as "1.5 months": no hours from an average month length
    assertThat(text(1186, "P1.5M")).isEqualTo(duration(1, 15, 0, 0));
    assertThat(text(1186, "P1.5D")).isEqualTo(duration(0, 1, 12 * 3600, 0));
    assertThat(text(1186, "-P1Y2M")).isEqualTo(duration(-14, 0, 0, 0));
    assertThat(text(1186, "PT1M30S")).isEqualTo(duration(0, 0, 90, 0));
    // the alternative format
    assertThat(text(1186, "P0001-02-03T04:05:06")).isEqualTo(duration(14, 3, 4 * 3600 + 5 * 60 + 6, 0));
  }

  @Test
  void intervalAmountsOfUnboundedMagnitudeAreRefusedBeforeAnyArithmetic() {
    // each of these used to make setScale build a number of about a hundred million digits
    for (final String huge : new String[] { "1e100000000 months", "1e100000000 days", "1e-100000000 seconds", "1e100000000:00" })
      assertThatThrownBy(() -> text(1186, huge)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void timestamptzRefusesADateTheCalendarDoesNotHave() {
    assertThatThrownBy(() -> text(1184, "2024-02-30 12:00:00+00")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> text(1184, "2023-02-29 12:00:00+00")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void scalarJsonDocuments() {
    for (final int oid : new int[] { 114, 3802 }) {
      assertThat(text(oid, "42")).isEqualTo(42);
      assertThat(text(oid, "\"s\"")).isEqualTo("s");
      assertThat(text(oid, "true")).isEqualTo(true);
      assertThat(text(oid, "null")).isNull();
    }
    final byte[] versioned = ByteBuffer.allocate(3).put((byte) 1).put("42".getBytes(StandardCharsets.UTF_8)).array();
    assertThat(binary(3802, versioned)).isEqualTo(42);
  }

  @Test
  void malformedScalarJsonIsRefused() {
    for (final int oid : new int[] { 114, 3802 })
      for (final String malformed : new String[] { "not_json", "'s'", "1 2", "tru", "\"a\" x" })
        assertThatThrownBy(() -> text(oid, malformed)).as("%d %s", oid, malformed).isInstanceOf(IllegalArgumentException.class);
    final byte[] versioned = ByteBuffer.allocate(9).put((byte) 1).put("not_json".getBytes(StandardCharsets.UTF_8)).array();
    assertThatThrownBy(() -> binary(3802, versioned)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aFractionOfAYearIsRoundedToWholeMonths() {
    // PostgreSQL: 1.1 years is 13 months (13.2 rounded), with no days spilled from the fraction
    assertThat(text(1186, "1.1 years")).isEqualTo(duration(13, 0, 0, 0));
    assertThat(text(1186, "P1.1Y")).isEqualTo(duration(13, 0, 0, 0));
    assertThat(text(1186, "1.5 years")).isEqualTo(duration(18, 0, 0, 0));
    // a fraction of a month still spills into days of 30
    assertThat(text(1186, "1.5 months")).isEqualTo(duration(1, 15, 0, 0));
  }

  @Test
  void jsonbReadsLikeJson() {
    assertThat(((JSONObject) text(3802, "{\"a\": 1}")).getInt("a")).isEqualTo(1);
    final byte[] json = "{\"a\": 1}".getBytes(StandardCharsets.UTF_8);
    final byte[] versioned = ByteBuffer.allocate(json.length + 1).put((byte) 1).put(json).array();
    assertThat(((JSONObject) binary(3802, versioned)).getInt("a")).isEqualTo(1);
  }
}
