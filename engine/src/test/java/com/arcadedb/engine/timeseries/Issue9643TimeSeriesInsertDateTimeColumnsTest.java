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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9643: {@code INSERT INTO} a TIMESERIES type threw a raw {@code ClassCastException} for every
 * {@code DATETIME}, {@code DATE}, {@code DATETIME_SECOND}, {@code DATETIME_MICROS} and {@code DATETIME_NANOS}
 * column, as a FIELD or as a TAG, whatever the literal form. The document layer had already turned the property
 * into a {@code LocalDateTime} / {@code LocalDate}, and the SQL write path handed that object to the engine, whose
 * row source stores these columns as an epoch count and casts the value to {@code Number}.
 * <p>
 * The invariant pinned here: an INSERT stores the same epoch count, in the column's own unit, that the binary
 * serializer stores for the same value on a document property of the same type. For {@code DATETIME} (epoch millis)
 * and {@code DATE} (epoch days) that makes a numeric literal round-trip unchanged, exactly as
 * {@link TimeSeriesEngine#appendSamples(long[], Object[][])} stores it.
 */
class Issue9643TimeSeriesInsertDateTimeColumnsTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  private int documentTypes;

  /**
   * Declared type, a numeric literal and the epoch count the column must hold for it. A number assigned to a
   * datetime property is read by the document layer as epoch MILLIS whatever the declared precision, and a number
   * assigned to a DATE as epoch DAYS (issue #9324), so {@code DATETIME} and {@code DATE} hold the literal itself and
   * the other precisions hold the same instant in their own unit - what a document property of that type stores.
   */
  private static final Object[][] NUMERIC_LITERALS = {
      { "DATETIME", 1_700_000_123_456L, 1_700_000_123_456L },
      { "DATE", 19_675L, 19_675L },
      { "DATETIME_SECOND", 1_700_000_123_456L, 1_700_000_123L },
      { "DATETIME_MICROS", 1_700_000_123_456L, 1_700_000_123_456_000L },
      { "DATETIME_NANOS", 1_700_000_123_456L, 1_700_000_123_456_000_000L },
  };

  @Test
  void aNumericLiteralIntoADateTimeFieldIsStoredOnBothLayers() throws Exception {
    for (final Object[] spec : NUMERIC_LITERALS) {
      final String declared = (String) spec[0];
      final long literal = (Long) spec[1];
      final long expected = (Long) spec[2];
      final String typeName = "Field_" + declared;

      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared
          + ", tail DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS + ", v = " + literal + ", tail = 2.5");

      assertStored(typeName, declared, expected, 2.5d);
      assertThat(expected).as("%s stores what a document property stores", declared)
          .isEqualTo(documentEncoding(declared, String.valueOf(literal)));
    }
  }

  @Test
  void aNumericLiteralIntoADateTimeTagIsStoredAndFiltersBack() throws Exception {
    for (final Object[] spec : NUMERIC_LITERALS) {
      final String declared = (String) spec[0];
      final long literal = (Long) spec[1];
      final long expected = (Long) spec[2];
      // one day later in every unit the literal can be read in, so the two rows never share a tag value
      final long otherLiteral = "DATE".equals(declared) ? literal + 1 : literal + 86_400_000L;
      final String typeName = "Tag_" + declared;

      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (k " + declared
          + ") FIELDS (tail DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS + ", k = " + literal + ", tail = 2.5");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + (BASE_TS + 1) + ", k = " + otherLiteral
          + ", tail = 3.5");

      final TimeSeriesEngine engine = engineOf(typeName);
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).as("%s tag rows", declared).hasSize(2);
      assertThat(rows.get(0)[1]).as("%s tag value", declared).isEqualTo(expected);

      // the tag push-down converts a literal as INSERT did (issue #9724), so the literal that wrote the row finds it - unless the
      // column's precision dropped part of it, as a DATETIME_SECOND drops the milliseconds, which a document property does too
      if (!"DATETIME_SECOND".equals(declared))
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE k = " + literal)) {
          final List<Result> found = rs.stream().toList();
          assertThat(found).as("%s tag filter", declared).hasSize(1);
          assertThat(found.getFirst().<Object>getProperty("tail")).isEqualTo(2.5d);
        }
      // and the value a row reads back as finds that row whatever the precision (issue #9725)
      final Object readBack;
      try (final ResultSet rs = database.query("sql", "SELECT k FROM " + typeName + " WHERE tail = 2.5")) {
        readBack = rs.next().getProperty("k");
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE k = ?", readBack)) {
        final List<Result> found = rs.stream().toList();
        assertThat(found).as("%s tag filter by the value read back", declared).hasSize(1);
        assertThat(found.getFirst().<Object>getProperty("tail")).isEqualTo(2.5d);
      }

      engine.compactAll();
      final List<Object[]> sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(sealed.get(0)[1]).as("%s tag value after compaction", declared).isEqualTo(expected);
    }
  }

  /**
   * A string literal is parsed by the document layer exactly as it is for a document property, so the stored
   * epoch must be what the binary serializer would store for that same property.
   */
  @Test
  void aStringLiteralIsStoredAsTheEpochADocumentPropertyWouldHold() throws Exception {
    final Object[][] stringLiterals = {
        { "DATETIME", "2023-11-14 22:15:23" },
        { "DATE", "2023-11-14" },
        { "DATETIME_SECOND", "2023-11-14 22:15:23" },
        { "DATETIME_MICROS", "2023-11-14 22:15:23.456789" },
        { "DATETIME_NANOS", "2023-11-14 22:15:23.456789123" },
    };

    for (final Object[] spec : stringLiterals) {
      final String declared = (String) spec[0];
      final String literal = (String) spec[1];

      final long expected = documentEncoding(declared, "'" + literal + "'");

      final String typeName = "Str_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared
          + ", tail DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS + ", v = '" + literal + "', tail = 2.5");

      assertStored(typeName, declared, expected, 2.5d);
    }
  }

  /**
   * The SQL path and the engine API must agree: the issue's control case, an engine append of the same epoch.
   */
  @Test
  void theInsertStoresWhatTheEngineAppendStores() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE ViaSql TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE ViaEngine TIMESTAMP ts FIELDS (v DATETIME) SHARDS 1");

    database.command("sql", "INSERT INTO ViaSql SET ts = " + BASE_TS + ", v = 1700000123456");
    engineOf("ViaEngine").appendSamples(new long[] { BASE_TS }, new Object[] { 1_700_000_123_456L });

    assertThat(engineOf("ViaSql").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst())
        .containsExactly(engineOf("ViaEngine").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst());
  }

  @Test
  void aMissingDateTimeFieldStillInserts() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Sparse TIMESTAMP ts FIELDS (v DATETIME, tail DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Sparse SET ts = " + BASE_TS + ", tail = 2.5");

    final List<Object[]> rows = engineOf("Sparse").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[2]).isEqualTo(2.5d);
  }

  @Test
  void anExplicitNullIntoADateTimeFieldStillInserts() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Nulls TIMESTAMP ts FIELDS (v DATETIME_MICROS, d DATE, tail DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Nulls SET ts = " + BASE_TS + ", v = null, d = null, tail = 2.5");

    final List<Object[]> rows = engineOf("Nulls").query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[3]).isEqualTo(2.5d);
  }

  /**
   * A value no datetime encoding can read is refused naming the column, not cast to {@code Number} by the engine and
   * not stored as the absent marker. Through SQL the document layer refuses it first, when the property is set; the
   * refusal in {@code SaveElementStep.convertValue} is the second line for a value that reaches it unconverted.
   */
  @Test
  void aValueNoDateTimeEncodingCanReadIsRefusedNamingTheColumn() throws Exception {
    for (final String declared : new String[] { "DATETIME", "DATETIME_NANOS", "DATE" }) {
      final String typeName = "Bad_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (observed " + declared
          + ") SHARDS 1");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO " + typeName + " SET ts = " + BASE_TS
          + ", observed = [1, 2]"))
          .as("%s refuses a list", declared)
          .isInstanceOfAny(IllegalArgumentException.class, CommandExecutionException.class)
          .hasMessageContaining("'observed'")
          .hasMessageContaining(declared);
      assertThat(engineOf(typeName).query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).as("%s stored nothing", declared)
          .isEmpty();
    }
  }

  private void assertStored(final String typeName, final String declared, final long expected, final double tail)
      throws Exception {
    final TimeSeriesEngine engine = engineOf(typeName);

    final List<Object[]> mutable = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(mutable).as("%s row count", declared).hasSize(1);
    assertThat(mutable.getFirst()[1]).as("%s value from the mutable layer", declared).isEqualTo(expected);
    assertThat(mutable.getFirst()[2]).as("%s neighbour from the mutable layer", declared).isEqualTo(tail);

    engine.compactAll();

    final List<Object[]> sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(sealed.getFirst()[1]).as("%s value from the sealed layer", declared).isEqualTo(expected);
    assertThat(sealed.getFirst()[2]).as("%s neighbour from the sealed layer", declared).isEqualTo(tail);
  }

  /**
   * The epoch count the binary serializer stores for {@code sqlLiteral} assigned to a document property declared
   * {@code declared}: the reference every INSERT into a TimeSeries column of that type must agree with.
   */
  private long documentEncoding(final String declared, final String sqlLiteral) {
    final String docType = "Doc_" + declared + "_" + (++documentTypes);
    database.command("sql", "CREATE DOCUMENT TYPE " + docType);
    database.command("sql", "CREATE PROPERTY " + docType + ".v " + declared);
    final Object[] holder = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "INSERT INTO " + docType + " SET v = " + sqlLiteral)) {
        final Document doc = rs.next().getElement().orElseThrow();
        holder[0] = doc.get("v");
      }
    });
    return "DATE".equals(declared) ?
        DateUtils.dateToEpochDays(holder[0]) :
        DateUtils.dateTimeToTimestamp(holder[0], DateUtils.getPrecisionFromType(Type.valueOf(declared)));
  }

  private TimeSeriesEngine engineOf(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }
}
