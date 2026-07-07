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
import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltDateTimeStructure;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BoltDateTimeStructureTest {

  // 2024-01-15T10:30:45 at +02:00.
  private static final LocalDateTime LOCAL = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
  private static final ZoneOffset    OFFSET = ZoneOffset.ofHours(2);

  private static PackStreamReader.StructureValue roundTrip(final BoltDateTimeStructure s, final int major)
      throws Exception {
    final PackStreamWriter writer = new PackStreamWriter().boltMajorVersion(major);
    s.writeTo(writer);
    return (PackStreamReader.StructureValue) new PackStreamReader(writer.toByteArray()).readValue();
  }

  // Drives the PRODUCTION mapper path (BoltStructureMapper.toPackStreamValue, the public entrypoint that
  // internally calls the package-private toTemporalStructure) so the epoch-basis arguments the mapper feeds
  // into the two BoltDateTimeStructure factories are pinned end-to-end, not just the factories in isolation.
  private static PackStreamReader.StructureValue mapperRoundTrip(final Object value, final int major)
      throws Exception {
    final PackStreamStructure s = (PackStreamStructure) BoltStructureMapper.toPackStreamValue(value);
    final PackStreamWriter writer = new PackStreamWriter().boltMajorVersion(major);
    s.writeTo(writer);
    return (PackStreamReader.StructureValue) new PackStreamReader(writer.toByteArray()).readValue();
  }

  @Test
  void offsetDateTimeUsesLegacySignatureAndLocalEpochOnBolt4() throws Exception {
    final BoltDateTimeStructure s = BoltDateTimeStructure.offset(
        LOCAL.toEpochSecond(ZoneOffset.UTC), LOCAL.toEpochSecond(OFFSET), LOCAL.getNano(), OFFSET.getTotalSeconds());
    final PackStreamReader.StructureValue v = roundTrip(s, 4);
    assertThat(v.getSignature()).isEqualTo((byte) 0x46);              // 'F'
    assertThat(((Number) v.getFields().get(0)).longValue())
        .isEqualTo(LOCAL.toEpochSecond(ZoneOffset.UTC));             // local epoch-second
    assertThat(((Number) v.getFields().get(2)).longValue()).isEqualTo(7200L);
  }

  @Test
  void offsetDateTimeUsesUtcSignatureAndUtcEpochOnBolt5() throws Exception {
    final BoltDateTimeStructure s = BoltDateTimeStructure.offset(
        LOCAL.toEpochSecond(ZoneOffset.UTC), LOCAL.toEpochSecond(OFFSET), LOCAL.getNano(), OFFSET.getTotalSeconds());
    final PackStreamReader.StructureValue v = roundTrip(s, 5);
    assertThat(v.getSignature()).isEqualTo((byte) 0x49);              // 'I'
    assertThat(((Number) v.getFields().get(0)).longValue())
        .isEqualTo(LOCAL.toEpochSecond(OFFSET));                     // true UTC epoch-second
    assertThat(((Number) v.getFields().get(2)).longValue()).isEqualTo(7200L);
  }

  @Test
  void zoneIdDateTimeSwitchesSignatureByVersion() throws Exception {
    final BoltDateTimeStructure s = BoltDateTimeStructure.zoneId(
        LOCAL.toEpochSecond(ZoneOffset.UTC), LOCAL.toEpochSecond(OFFSET), LOCAL.getNano(), "Europe/Rome");
    final PackStreamReader.StructureValue v4 = roundTrip(s, 4);
    final PackStreamReader.StructureValue v5 = roundTrip(s, 5);
    assertThat(v4.getSignature()).isEqualTo((byte) 0x66); // 'f'
    assertThat(v5.getSignature()).isEqualTo((byte) 0x69); // 'i'
    // v4 emits the local epoch-second (wall clock treated as UTC); v5 emits the true UTC epoch-second.
    assertThat(((Number) v4.getFields().get(0)).longValue()).isEqualTo(LOCAL.toEpochSecond(ZoneOffset.UTC));
    assertThat(((Number) v5.getFields().get(0)).longValue()).isEqualTo(LOCAL.toEpochSecond(OFFSET));
    assertThat(List.of("Europe/Rome")).contains((String) v5.getFields().get(2));
  }

  // ----- Mapper-driven tests: exercise the real BoltStructureMapper path end-to-end so a regression in the
  //       epoch-basis argument the mapper passes to the factories (local vs true-UTC) is caught. -----

  @Test
  void mapperOffsetDateTimeEmitsUtcEpochOnBolt5() throws Exception {
    // Non-zero offset so utc epoch != local epoch.
    final OffsetDateTime odt = OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2));
    final PackStreamReader.StructureValue v = mapperRoundTrip(odt, 5);
    assertThat(v.getSignature()).isEqualTo((byte) 0x49);              // 'I'
    assertThat(((Number) v.getFields().get(0)).longValue()).isEqualTo(odt.toEpochSecond()); // true UTC epoch
    assertThat(((Number) v.getFields().get(2)).longValue()).isEqualTo(7200L);
  }

  @Test
  void mapperOffsetDateTimeEmitsLocalEpochOnBolt4() throws Exception {
    final OffsetDateTime odt = OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2));
    final PackStreamReader.StructureValue v = mapperRoundTrip(odt, 4);
    assertThat(v.getSignature()).isEqualTo((byte) 0x46);              // 'F'
    assertThat(((Number) v.getFields().get(0)).longValue())
        .isEqualTo(odt.toLocalDateTime().toEpochSecond(ZoneOffset.UTC)); // local epoch (wall clock as UTC)
    assertThat(((Number) v.getFields().get(2)).longValue()).isEqualTo(7200L);
  }

  @Test
  void mapperZoneIdDateTimeEmitsUtcEpochOnBolt5() throws Exception {
    // Genuine named zone whose offset is non-zero (Europe/Rome = CEST +2 in June) so utc epoch != local epoch,
    // and so the mapper takes the zone-id branch (not the offset collapse).
    final ZonedDateTime zdt = ZonedDateTime.of(2024, 6, 15, 10, 30, 45, 0, ZoneId.of("Europe/Rome"));
    final PackStreamReader.StructureValue v = mapperRoundTrip(zdt, 5);
    assertThat(v.getSignature()).isEqualTo((byte) 0x69);              // 'i'
    assertThat(((Number) v.getFields().get(0)).longValue()).isEqualTo(zdt.toEpochSecond()); // true UTC epoch
    assertThat((String) v.getFields().get(2)).isEqualTo("Europe/Rome");
  }

  @Test
  void mapperZoneIdDateTimeEmitsLocalEpochOnBolt4() throws Exception {
    final ZonedDateTime zdt = ZonedDateTime.of(2024, 6, 15, 10, 30, 45, 0, ZoneId.of("Europe/Rome"));
    final PackStreamReader.StructureValue v = mapperRoundTrip(zdt, 4);
    assertThat(v.getSignature()).isEqualTo((byte) 0x66);              // 'f'
    assertThat(((Number) v.getFields().get(0)).longValue())
        .isEqualTo(zdt.toLocalDateTime().toEpochSecond(ZoneOffset.UTC)); // local epoch (wall clock as UTC)
    assertThat((String) v.getFields().get(2)).isEqualTo("Europe/Rome");
  }
}
