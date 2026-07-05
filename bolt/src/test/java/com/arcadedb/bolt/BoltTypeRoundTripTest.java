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

import com.arcadedb.bolt.structure.BoltPointStructure;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.bolt.structure.BoltTemporalStructure;
import com.arcadedb.query.opencypher.temporal.CypherDuration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;

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
  @DisplayName("[TYPE-011] CypherDuration serializes as a native Bolt Duration structure")
  void type011_durationNative() {
    // duration('P1DT2H30M') -> months=0, days=1, seconds=9000, nanos=0
    final CypherDuration d = new CypherDuration(0, 1, 9000, 0);
    final Object out = BoltStructureMapper.toPackStreamValue(d);
    assertThat(out).isInstanceOf(BoltTemporalStructure.class);
    final BoltTemporalStructure s = (BoltTemporalStructure) out;
    assertThat(s.getSignature()).isEqualTo((byte) 0x45);
    assertThat(s.getFieldCount()).isEqualTo(4);
  }

  @Test
  @DisplayName("[TYPE-012] cartesian Point serializes as a native Bolt Point2D structure")
  void type012_cartesianPointNative() {
    final Map<String, Object> point = new LinkedHashMap<>();
    point.put("x", 12.34);
    point.put("y", 56.78);
    point.put("crs", "cartesian");
    final Object out = BoltStructureMapper.toPackStreamValue(point);
    assertThat(out).isInstanceOf(BoltPointStructure.class);
    final BoltPointStructure p = (BoltPointStructure) out;
    assertThat(p.getSignature()).isEqualTo((byte) 0x58);
    assertThat(p.getSrid()).isEqualTo(7203);
    assertThat(p.getX()).isEqualTo(12.34);
    assertThat(p.getY()).isEqualTo(56.78);
    assertThat(p.getZ()).isNull();
  }

  @Test
  @DisplayName("[TYPE-012] WGS-84 3D Point serializes as a native Bolt Point3D structure")
  void type012_wgs84Point3DNative() {
    final Map<String, Object> point = new LinkedHashMap<>();
    point.put("longitude", 12.34);
    point.put("latitude", 56.78);
    point.put("height", 100.0);
    point.put("crs", "WGS-84-3D");
    point.put("srid", 4979);
    final BoltPointStructure p = (BoltPointStructure) BoltStructureMapper.toPackStreamValue(point);
    assertThat(p.getSignature()).isEqualTo((byte) 0x59);
    assertThat(p.getSrid()).isEqualTo(4979);
    assertThat(p.getX()).isEqualTo(12.34);
    assertThat(p.getY()).isEqualTo(56.78);
    assertThat(p.getZ()).isEqualTo(100.0);
  }

  @Test
  @DisplayName("A plain map without crs is not misdetected as a Point")
  void plainMapIsNotPoint() {
    final Map<String, Object> m = new LinkedHashMap<>();
    m.put("a", 1);
    m.put("b", 2);
    assertThat(BoltStructureMapper.toPackStreamValue(m)).isInstanceOf(Map.class);
  }
}
