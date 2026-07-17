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

import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A RowDescription column is typed either from a sample value ({@link PostgresType#getTypeForValue}, the value
 * path) or from the declared schema ({@link PostgresType#getTypeFromArcade}, the schema path, taken when a query
 * returns no rows). The two must resolve every {@link Type} to the same PostgreSQL type: when they disagree, a
 * column's advertised OID depends on whether the table happens to be empty and clients see it change between
 * result sets.
 */
class PostgresTypeResolutionPathTest {

  /**
   * A representative value of each Type's default Java type, as the value path would receive it from a stored
   * record.
   */
  private static final Map<Type, Object> SAMPLE_VALUES = new EnumMap<>(Type.class);

  static {
    SAMPLE_VALUES.put(Type.BOOLEAN, Boolean.TRUE);
    SAMPLE_VALUES.put(Type.INTEGER, 42);
    SAMPLE_VALUES.put(Type.SHORT, (short) 10);
    SAMPLE_VALUES.put(Type.LONG, 100L);
    SAMPLE_VALUES.put(Type.FLOAT, 1.5f);
    SAMPLE_VALUES.put(Type.DOUBLE, 2.5d);
    SAMPLE_VALUES.put(Type.DATETIME, new Date());
    SAMPLE_VALUES.put(Type.STRING, "text");
    SAMPLE_VALUES.put(Type.BINARY, new byte[] { 1, 2 });
    SAMPLE_VALUES.put(Type.LIST, List.of("a", "b"));
    SAMPLE_VALUES.put(Type.MAP, Map.of("k", "v"));
    SAMPLE_VALUES.put(Type.LINK, new RID(1, 1));
    SAMPLE_VALUES.put(Type.BYTE, (byte) 5);
    SAMPLE_VALUES.put(Type.DATE, new Date());
    SAMPLE_VALUES.put(Type.DECIMAL, new BigDecimal("10.55"));
    SAMPLE_VALUES.put(Type.EMBEDDED, new JSONObject());
    SAMPLE_VALUES.put(Type.DATETIME_MICROS, LocalDateTime.now());
    SAMPLE_VALUES.put(Type.DATETIME_NANOS, LocalDateTime.now());
    SAMPLE_VALUES.put(Type.DATETIME_SECOND, LocalDateTime.now());
    SAMPLE_VALUES.put(Type.ARRAY_OF_SHORTS, new short[] { 1, 2 });
    SAMPLE_VALUES.put(Type.ARRAY_OF_INTEGERS, new int[] { 1, 2 });
    SAMPLE_VALUES.put(Type.ARRAY_OF_LONGS, new long[] { 1L, 2L });
    SAMPLE_VALUES.put(Type.ARRAY_OF_FLOATS, new float[] { 1.5f, 2.5f });
    SAMPLE_VALUES.put(Type.ARRAY_OF_DOUBLES, new double[] { 1.5d, 2.5d });
  }

  /**
   * Types whose two paths are known to disagree, each pending a fix that is not a missing switch case:
   * <ul>
   *   <li>SHORT/BYTE: the value path widens Short/Byte to INTEGER, because PostgresType has no int2[] to pair
   *       with it; reconciling on SMALLINT changes the OID existing clients already receive for populated rows.
   *   <li>DATETIME: java.util.Date is the default Java type of both DATE and DATETIME, so the value path cannot
   *       tell them apart and answers DATE for either.
   *   <li>DECIMAL: neither DOUBLE (lossy) nor VARCHAR is right; the fix is a NUMERIC (OID 1700) entry with a
   *       binary encoder.
   *   <li>BINARY: neither VARCHAR nor _char is right; the fix is a BYTEA (OID 17) entry.
   * </ul>
   * Removing an entry here is the intended way to land one of those fixes.
   */
  private static final Map<Type, PathDisagreement> KNOWN_DISAGREEMENTS = new EnumMap<>(Type.class);

  static {
    KNOWN_DISAGREEMENTS.put(Type.SHORT, new PathDisagreement(PostgresType.SMALLINT, PostgresType.INTEGER));
    KNOWN_DISAGREEMENTS.put(Type.BYTE, new PathDisagreement(PostgresType.SMALLINT, PostgresType.INTEGER));
    KNOWN_DISAGREEMENTS.put(Type.DATETIME, new PathDisagreement(PostgresType.TIMESTAMP, PostgresType.DATE));
    KNOWN_DISAGREEMENTS.put(Type.DECIMAL, new PathDisagreement(PostgresType.DOUBLE, PostgresType.VARCHAR));
    KNOWN_DISAGREEMENTS.put(Type.BINARY, new PathDisagreement(PostgresType.VARCHAR, PostgresType.ARRAY_CHAR));
  }

  private record PathDisagreement(PostgresType schemaPath, PostgresType valuePath) {
  }

  @Test
  void everyTypeHasASampleValue() {
    // Guards the table below against silently skipping a Type added to the enum later.
    assertThat(SAMPLE_VALUES.keySet()).containsExactlyInAnyOrder(Type.values());
  }

  @Test
  void schemaPathAgreesWithValuePathForEveryType() {
    for (final Type arcadeType : Type.values()) {
      if (KNOWN_DISAGREEMENTS.containsKey(arcadeType))
        continue;

      final PostgresType fromSchema = PostgresType.getTypeFromArcade(arcadeType);
      final PostgresType fromValue = PostgresType.getTypeForValue(SAMPLE_VALUES.get(arcadeType));

      assertThat(fromSchema)
          .as("%s resolves to %s from a sample value but to %s from the schema, so its column OID would depend "
              + "on whether the result set is empty", arcadeType, fromValue, fromSchema)
          .isEqualTo(fromValue);
    }
  }

  @Test
  void knownDisagreementsStillDisagree() {
    // Pins the documented exceptions so that fixing one forces its entry to be removed above rather than leaving
    // a stale exemption that would mask a regression.
    KNOWN_DISAGREEMENTS.forEach((arcadeType, expected) -> {
      assertThat(PostgresType.getTypeFromArcade(arcadeType)).as("schema path of %s", arcadeType).isEqualTo(expected.schemaPath());
      assertThat(PostgresType.getTypeForValue(SAMPLE_VALUES.get(arcadeType))).as("value path of %s", arcadeType)
          .isEqualTo(expected.valuePath());
    });
  }

  @Test
  void valuePathTypesShortArrays() {
    // Type.ARRAY_OF_SHORTS declares short[] as its default Java type, so a plain property declaration reaches
    // the value path with one: it used to hit the switch's default and abort the whole query.
    assertThat(PostgresType.getTypeForValue(new short[] { 1, 2 })).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getTypeForValue(new Short[] { 1, 2 })).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void schemaPathTypesArrayTypesAsArrays() {
    assertThat(PostgresType.getTypeFromArcade(Type.ARRAY_OF_SHORTS)).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getTypeFromArcade(Type.ARRAY_OF_INTEGERS)).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getTypeFromArcade(Type.ARRAY_OF_LONGS)).isEqualTo(PostgresType.ARRAY_LONG);
    assertThat(PostgresType.getTypeFromArcade(Type.ARRAY_OF_FLOATS)).isEqualTo(PostgresType.ARRAY_REAL);
    assertThat(PostgresType.getTypeFromArcade(Type.ARRAY_OF_DOUBLES)).isEqualTo(PostgresType.ARRAY_DOUBLE);
  }

  @Test
  void schemaPathTypesSubSecondDatetimesAsTimestamps() {
    assertThat(PostgresType.getTypeFromArcade(Type.DATETIME_MICROS)).isEqualTo(PostgresType.TIMESTAMP);
    assertThat(PostgresType.getTypeFromArcade(Type.DATETIME_NANOS)).isEqualTo(PostgresType.TIMESTAMP);
    assertThat(PostgresType.getTypeFromArcade(Type.DATETIME_SECOND)).isEqualTo(PostgresType.TIMESTAMP);
  }

  @Test
  void arrayTypesAreAdvertisedAsArrays() {
    // The five ARRAY_OF_* types used to collapse to a scalar varchar on the schema path.
    for (final Type arrayType : EnumSet.of(Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_LONGS,
        Type.ARRAY_OF_FLOATS, Type.ARRAY_OF_DOUBLES))
      assertThat(PostgresType.getTypeFromArcade(arrayType).isArrayType()).as("%s is an array type", arrayType).isTrue();
  }
}
