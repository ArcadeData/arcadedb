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
import java.time.LocalDate;
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
   * record in a database left at its default configuration. DATE and DATETIME are configurable per database
   * (GlobalConfiguration.DATE_IMPLEMENTATION/DATE_TIME_IMPLEMENTATION) and default to LocalDate/LocalDateTime,
   * not java.util.Date (issue #6447) - {@link #datetimeValuePathDisagreesWithSchemaWhenConfiguredForJavaUtilDate}
   * covers the non-default configuration separately, since a single sample per Type can only model one of them.
   */
  private static final Map<Type, Object> SAMPLE_VALUES = new EnumMap<>(Type.class);

  static {
    SAMPLE_VALUES.put(Type.BOOLEAN, Boolean.TRUE);
    SAMPLE_VALUES.put(Type.INTEGER, 42);
    SAMPLE_VALUES.put(Type.SHORT, (short) 10);
    SAMPLE_VALUES.put(Type.LONG, 100L);
    SAMPLE_VALUES.put(Type.FLOAT, 1.5f);
    SAMPLE_VALUES.put(Type.DOUBLE, 2.5d);
    SAMPLE_VALUES.put(Type.DATETIME, LocalDateTime.now());
    SAMPLE_VALUES.put(Type.STRING, "text");
    SAMPLE_VALUES.put(Type.BINARY, new byte[] { 1, 2 });
    SAMPLE_VALUES.put(Type.LIST, List.of("a", "b"));
    SAMPLE_VALUES.put(Type.MAP, Map.of("k", "v"));
    SAMPLE_VALUES.put(Type.LINK, new RID(1, 1));
    SAMPLE_VALUES.put(Type.BYTE, (byte) 5);
    SAMPLE_VALUES.put(Type.DATE, LocalDate.now());
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

  @Test
  void everyTypeHasASampleValue() {
    // Guards the table below against silently skipping a Type added to the enum later.
    assertThat(SAMPLE_VALUES.keySet()).containsExactlyInAnyOrder(Type.values());
  }

  @Test
  void schemaPathAgreesWithValuePathForEveryType() {
    // No skips: every Type's two paths now agree for the sample a default-configured database would actually
    // produce (issue #6447). The one remaining disagreement only a non-default configuration reaches - see
    // datetimeValuePathDisagreesWithSchemaWhenConfiguredForJavaUtilDate below.
    for (final Type arcadeType : Type.values()) {
      final PostgresType fromSchema = PostgresType.getTypeFromArcade(arcadeType);
      final PostgresType fromValue = PostgresType.getTypeForValue(SAMPLE_VALUES.get(arcadeType));

      assertThat(fromSchema)
          .as("%s resolves to %s from a sample value but to %s from the schema, so its column OID would depend "
              + "on whether the result set is empty", arcadeType, fromValue, fromSchema)
          .isEqualTo(fromValue);
    }
  }

  @Test
  void datetimeValuePathDisagreesWithSchemaWhenConfiguredForJavaUtilDate() {
    // Type.DATETIME's runtime representation is configurable per database (GlobalConfiguration.
    // DATE_TIME_IMPLEMENTATION) and java.util.Date is one of the supported alternatives to the LocalDateTime
    // default. java.util.Date is ALSO Type.DATE's default representation, so a sampled Date value cannot tell
    // the two apart on its own and getTypeForValue always answers DATE - disagreeing with the schema path's
    // TIMESTAMP for a DATETIME column. This is not client-visible for a real column, though:
    // PostgresNetworkExecutor.getColumns() resolves the ambiguity from the schema - either the row's own
    // element or, for a narrow column projection whose rows carry no element, the query's FROM-target type
    // (isDeclaredAsDatetime/getDeclaredProperty/resolveQueryTargetType) - the same mechanism already used to
    // type an empty LIST column from its declared element type (issue #5289). Pinned here, rather than fixed,
    // because the two *pure* functions have no schema to consult and so are not meant to converge for this one.
    assertThat(PostgresType.getTypeFromArcade(Type.DATETIME)).isEqualTo(PostgresType.TIMESTAMP);
    assertThat(PostgresType.getTypeForValue(new Date())).isEqualTo(PostgresType.DATE);
  }

  @Test
  void valuePathTypesShortArrays() {
    // Type.ARRAY_OF_SHORTS declares short[] as its default Java type, so a plain property declaration reaches
    // the value path with one: it used to hit the switch's default and abort the whole query.
    assertThat(PostgresType.getTypeForValue(new short[] { 1, 2 })).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getTypeForValue(new Short[] { 1, 2 })).isEqualTo(PostgresType.ARRAY_INT);
  }

  @Test
  void valuePathTypesBoxedByteArraysAsIntArrays() {
    // Only the primitive byte[] carries Type.BINARY's blob meaning; a Byte[] is an array of small integers, and
    // Byte elements already resolve to int4 everywhere else (getTypeForValue, getArrayTypeForElementType).
    assertThat(PostgresType.getTypeForValue(new Byte[] { 1, 2 })).isEqualTo(PostgresType.ARRAY_INT);
    assertThat(PostgresType.getTypeForValue(new byte[] { 1, 2 })).isEqualTo(PostgresType.BYTEA);
  }

  @Test
  void valuePathTypesEveryPrimitiveAndWrapperArrayWithoutThrowing() {
    // SQL passes these through to the wire un-converted rather than boxing them into a collection (see
    // InputParameter.isPrimitiveOrWrapperArray), so each one reaches the value path's array switch. Any type
    // missing a case there hits its throwing default and fails the whole query, which is what #5311 reported
    // for short[]. Assert the family is covered rather than only the reported member.
    final List<Object> arrays = List.of(
        new byte[] { 1 }, new Byte[] { 1 },
        new short[] { 1 }, new Short[] { 1 },
        new int[] { 1 }, new Integer[] { 1 },
        new long[] { 1L }, new Long[] { 1L },
        new float[] { 1f }, new Float[] { 1f },
        new double[] { 1d }, new Double[] { 1d },
        new boolean[] { true }, new Boolean[] { true },
        new char[] { 'a' }, new Character[] { 'a' },
        new String[] { "a" });

    for (final Object array : arrays)
      assertThat(PostgresType.getTypeForValue(array)).as("value path of %s", array.getClass().getSimpleName()).isNotNull();
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
