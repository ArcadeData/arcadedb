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
package com.arcadedb.bolt;

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltStructureMapper;

import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4905: inbound Bolt temporal PackStream structures (Date, Time,
 * LocalTime, LocalDateTime, DateTime, DateTimeZoneId) must be decoded into {@code java.time}
 * values so native temporal query parameters bind instead of arriving as opaque
 * {@link PackStreamReader.StructureValue} objects and being silently dropped.
 * <p>
 * ArcadeDB negotiates Bolt v4.4 max, so clients use the legacy (pre-5.0) DateTime /
 * DateTimeZoneId encoding (seconds field is the LOCAL epoch-second). The Bolt 5.0 "UTC"
 * signatures are also covered.
 */
class PackStreamTemporalInputTest {
  // Legacy (Bolt <= 4.4) temporal signatures
  private static final byte SIG_DATE                    = 0x44; // 'D'
  private static final byte SIG_TIME                    = 0x54; // 'T'
  private static final byte SIG_LOCAL_TIME              = 0x74; // 't'
  private static final byte SIG_LOCAL_DATE_TIME         = 0x64; // 'd'
  private static final byte SIG_DATE_TIME_OFFSET_LEGACY = 0x46; // 'F'
  private static final byte SIG_DATE_TIME_ZONEID_LEGACY = 0x66; // 'f'
  // UTC (Bolt 5.0+) datetime signatures
  private static final byte SIG_DATE_TIME_OFFSET_UTC    = 0x49; // 'I'
  private static final byte SIG_DATE_TIME_ZONEID_UTC    = 0x69; // 'i'

  private static final int NANOS = 123456789;

  @Test
  void rawTemporalStructIsOpaqueBeforeDecoding() throws Exception {
    // Documents the bug: reading a temporal struct off the wire yields a raw StructureValue,
    // which is meaningless to the query engine until decoded.
    final Object raw = readBack(SIG_DATE, LocalDate.of(2026, 7, 2).toEpochDay());
    assertThat(raw).isInstanceOf(PackStreamReader.StructureValue.class);
  }

  @Test
  void decodesDate() throws Exception {
    final LocalDate expected = LocalDate.of(2026, 7, 2);
    assertThat(decode(SIG_DATE, expected.toEpochDay())).isEqualTo(expected);
  }

  @Test
  void decodesLocalTime() throws Exception {
    final LocalTime expected = LocalTime.of(14, 30, 15, NANOS);
    assertThat(decode(SIG_LOCAL_TIME, expected.toNanoOfDay())).isEqualTo(expected);
  }

  @Test
  void decodesTimeWithOffset() throws Exception {
    final OffsetTime expected = OffsetTime.of(LocalTime.of(14, 30, 15, NANOS), ZoneOffset.ofHours(2));
    assertThat(decode(SIG_TIME, expected.toLocalTime().toNanoOfDay(), (long) expected.getOffset().getTotalSeconds()))
        .isEqualTo(expected);
  }

  @Test
  void decodesLocalDateTime() throws Exception {
    final LocalDateTime expected = LocalDateTime.of(2026, 7, 2, 14, 30, 15, NANOS);
    assertThat(decode(SIG_LOCAL_DATE_TIME, expected.toEpochSecond(ZoneOffset.UTC), (long) expected.getNano()))
        .isEqualTo(expected);
  }

  @Test
  void decodesDateTimeWithOffsetLegacyEncoding() throws Exception {
    final OffsetDateTime expected = OffsetDateTime.of(2026, 7, 2, 14, 30, 15, NANOS, ZoneOffset.ofHours(2));
    // Legacy encoding: seconds field is the local epoch-second (offset folded in).
    final long secondsLocal = expected.toLocalDateTime().toEpochSecond(ZoneOffset.UTC);
    final Object decoded = decode(SIG_DATE_TIME_OFFSET_LEGACY, secondsLocal, (long) expected.getNano(),
        (long) expected.getOffset().getTotalSeconds());
    assertThat(decoded).isInstanceOf(OffsetDateTime.class);
    assertThat(decoded).isEqualTo(expected);
  }

  @Test
  void decodesDateTimeWithZoneIdLegacyEncoding() throws Exception {
    final ZonedDateTime expected = ZonedDateTime.of(2026, 7, 2, 14, 30, 15, NANOS, ZoneId.of("Europe/Rome"));
    final long secondsLocal = expected.toLocalDateTime().toEpochSecond(ZoneOffset.UTC);
    final Object decoded = decode(SIG_DATE_TIME_ZONEID_LEGACY, secondsLocal, (long) expected.getNano(), "Europe/Rome");
    assertThat(decoded).isInstanceOf(ZonedDateTime.class);
    assertThat(decoded).isEqualTo(expected);
  }

  @Test
  void decodesDateTimeWithOffsetUtcEncoding() throws Exception {
    final OffsetDateTime expected = OffsetDateTime.of(2026, 7, 2, 14, 30, 15, NANOS, ZoneOffset.ofHours(2));
    // UTC encoding (Bolt 5.0+): seconds field is the true UTC epoch-second.
    final long secondsUtc = expected.toInstant().getEpochSecond();
    final Object decoded = decode(SIG_DATE_TIME_OFFSET_UTC, secondsUtc, (long) expected.getNano(),
        (long) expected.getOffset().getTotalSeconds());
    assertThat(decoded).isInstanceOf(OffsetDateTime.class);
    assertThat(decoded).isEqualTo(expected);
  }

  @Test
  void decodesDateTimeWithZoneIdUtcEncoding() throws Exception {
    final ZonedDateTime expected = ZonedDateTime.of(2026, 7, 2, 14, 30, 15, NANOS, ZoneId.of("Europe/Rome"));
    final long secondsUtc = expected.toInstant().getEpochSecond();
    final Object decoded = decode(SIG_DATE_TIME_ZONEID_UTC, secondsUtc, (long) expected.getNano(), "Europe/Rome");
    assertThat(decoded).isInstanceOf(ZonedDateTime.class);
    assertThat(decoded).isEqualTo(expected);
  }

  @Test
  void hydratesTemporalNestedInParameterMap() throws Exception {
    // A RUN parameters map { reference_time: <DateTime> } must be hydrated recursively - this is
    // exactly the graphiti `WHERE e.valid_at <= $reference_time` scenario.
    final OffsetDateTime expected = OffsetDateTime.of(2026, 7, 2, 14, 30, 15, 0, ZoneOffset.UTC);
    final long secondsLocal = expected.toLocalDateTime().toEpochSecond(ZoneOffset.UTC);

    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeMapHeader(1);
    writer.writeString("reference_time");
    writer.writeStructureHeader(SIG_DATE_TIME_OFFSET_LEGACY, 3);
    writer.writeValue(secondsLocal);
    writer.writeValue(0L);
    writer.writeValue((long) expected.getOffset().getTotalSeconds());

    final Object hydrated = BoltStructureMapper.fromPackStreamValue(new PackStreamReader(writer.toByteArray()).readValue());
    assertThat(hydrated).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) hydrated).get("reference_time")).isEqualTo(expected);
  }

  @Test
  void nonTemporalValuesArePassedThroughUnchanged() throws Exception {
    assertThat(BoltStructureMapper.fromPackStreamValue("hello")).isEqualTo("hello");
    assertThat(BoltStructureMapper.fromPackStreamValue(42L)).isEqualTo(42L);
    // An unknown (non-temporal) struct signature must be left untouched, not misread as a temporal.
    final Object unknown = readBack((byte) 0x4E /* 'N' node - never a valid input param */, 1L);
    assertThat(BoltStructureMapper.fromPackStreamValue(unknown)).isInstanceOf(PackStreamReader.StructureValue.class);
  }

  @Test
  void malformedTemporalStructIsLeftOpaqueInsteadOfCrashing() throws Exception {
    // Wrong field count for the signature (DateTime expects 3 fields, not 1): must degrade to an
    // opaque StructureValue rather than throwing IndexOutOfBoundsException out of RUN parsing.
    final Object wrongArity = decode(SIG_DATE_TIME_OFFSET_LEGACY, 123L);
    assertThat(wrongArity).isInstanceOf(PackStreamReader.StructureValue.class);

    // Right field count but a non-numeric field where a number is required: must not throw ClassCastException.
    final Object wrongType = decode(SIG_DATE, "not-a-number");
    assertThat(wrongType).isInstanceOf(PackStreamReader.StructureValue.class);
  }

  /**
   * Write a temporal struct to the wire, read it back as a StructureValue, and decode it.
   */
  private static Object decode(final byte signature, final Object... fields) throws Exception {
    return BoltStructureMapper.fromPackStreamValue(readBack(signature, fields));
  }

  /**
   * Write a struct to the wire and read it back (as a raw StructureValue, without decoding).
   */
  private static Object readBack(final byte signature, final Object... fields) throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(signature, fields.length);
    for (final Object field : fields)
      writer.writeValue(field);
    return new PackStreamReader(writer.toByteArray()).readValue();
  }
}
