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

import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.bolt.structure.BoltTemporalStructure;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire-level certification of {@link BoltStructureMapper#toPackStreamValue}
 * for the type-round-trip matrix (conformance spec, issue #4883). Pins the
 * serialization contract at the module level so a #4890 fix is caught exactly
 * where it lands, independent of which server image the e2e layer pulls.
 * Complements the driver-visible round-trips in
 * {@code e2e/RemoteBoltDatabaseIT.TypeRoundTrip}.
 */
class BoltTypeRoundTripTest {

  @Test
  @DisplayName("[TYPE-007] LocalDate serializes as a native Bolt Date structure")
  void type007_localDateNative() {
    final Object out = BoltStructureMapper.toPackStreamValue(LocalDate.of(2026, 1, 15));
    assertThat(out).isInstanceOf(BoltTemporalStructure.class);
  }

  @Test
  @DisplayName("[TYPE-008] LocalTime serializes as a native Bolt LocalTime structure")
  void type008_localTimeNative() {
    final Object out = BoltStructureMapper.toPackStreamValue(LocalTime.of(14, 30));
    assertThat(out).isInstanceOf(BoltTemporalStructure.class);
  }

  @Test
  @DisplayName("[TYPE-009] LocalDateTime serializes as a native Bolt LocalDateTime structure")
  void type009_localDateTimeNative() {
    final Object out = BoltStructureMapper.toPackStreamValue(LocalDateTime.of(2026, 1, 15, 14, 30));
    assertThat(out).isInstanceOf(BoltTemporalStructure.class);
  }

  @Test
  @DisplayName("[TYPE-010] OffsetDateTime serializes as a native Bolt DateTime structure")
  void type010_offsetDateTimeNative() {
    final Object out = BoltStructureMapper.toPackStreamValue(
        OffsetDateTime.of(2026, 1, 15, 14, 30, 0, 0, ZoneOffset.ofHours(2)));
    assertThat(out).isInstanceOf(BoltTemporalStructure.class);
  }

  @Test
  @DisplayName("[TYPE-011] Duration falls back to a String (wire-level gap #4890)")
  void type011_durationGap() {
    // KNOWN GAP (#4890): BoltStructureMapper has no Duration branch, so a
    // java.time.Duration falls through to value.toString(). This characterization
    // pins the current contract - when #4890 adds a native Bolt Duration
    // structure, this assertion FAILS and must be flipped to assert the structure.
    final Object out = BoltStructureMapper.toPackStreamValue(Duration.ofHours(2));
    assertThat(out).isNotInstanceOf(BoltTemporalStructure.class);
    assertThat(out).isInstanceOf(String.class);
  }
}
