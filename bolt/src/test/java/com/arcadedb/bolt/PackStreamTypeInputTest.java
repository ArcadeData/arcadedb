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
import com.arcadedb.query.opencypher.temporal.CypherDuration;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for inbound Bolt Duration and Point PackStream structures: a Bolt client
 * sending a native Duration or Point as a query parameter must be decoded into a usable value
 * (a {@link CypherDuration} for Duration, a map carrying x/y(/z)/srid/crs for Point) instead of
 * arriving as an opaque {@link PackStreamReader.StructureValue} and being silently dropped.
 */
class PackStreamTypeInputTest {
  private static final byte SIG_DURATION  = 0x45;
  private static final byte SIG_POINT_2D  = 0x58;
  private static final byte SIG_POINT_3D  = 0x59;

  @Test
  void decodesDuration() throws Exception {
    final Object out = decode(SIG_DURATION, 0L, 1L, 9000L, 0L);
    assertThat(out).isInstanceOf(CypherDuration.class);
    final CypherDuration d = (CypherDuration) out;
    assertThat(d.getMonths()).isEqualTo(0);
    assertThat(d.getDays()).isEqualTo(1);
    assertThat(d.getSeconds()).isEqualTo(9000);
    assertThat(d.getNanosAdjustment()).isEqualTo(0);
  }

  @Test
  void decodesCartesianPoint2D() throws Exception {
    final Object out = decode(SIG_POINT_2D, 7203L, 12.34, 56.78);
    assertThat(out).isInstanceOf(Map.class);
    final Map<?, ?> m = (Map<?, ?>) out;
    assertThat(((Number) m.get("srid")).intValue()).isEqualTo(7203);
    assertThat(((Number) m.get("x")).doubleValue()).isEqualTo(12.34);
    assertThat(((Number) m.get("y")).doubleValue()).isEqualTo(56.78);
    assertThat(m.get("crs")).isEqualTo("cartesian");
  }

  @Test
  void decodesWgs84Point3D() throws Exception {
    final Object out = decode(SIG_POINT_3D, 4979L, 12.34, 56.78, 100.0);
    final Map<?, ?> m = (Map<?, ?>) out;
    assertThat(m.get("crs")).isEqualTo("WGS-84-3D");
    assertThat(((Number) m.get("z")).doubleValue()).isEqualTo(100.0);
  }

  @Test
  void malformedDurationIsLeftOpaque() throws Exception {
    // Duration expects 4 fields; 2 must degrade to an opaque StructureValue, not crash.
    assertThat(decode(SIG_DURATION, 1L, 2L)).isInstanceOf(PackStreamReader.StructureValue.class);
  }

  /**
   * Write a struct to the wire, read it back, and decode it.
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
