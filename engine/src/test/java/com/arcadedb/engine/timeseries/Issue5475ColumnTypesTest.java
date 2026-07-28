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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5475: the mutable row format and the sealed block format disagreed about what a TimeSeries
 * column's declared type means.
 * <ul>
 *   <li>{@code DATE} and the sub-millisecond {@code DATETIME_*} variants had no case in the row
 *       writer or reader, so the writer advanced the row cursor by 8 bytes while the reader advanced
 *       by 2. Everything after such a column in the row was read from the wrong offset.</li>
 *   <li>The sealed layer handed back a different Java type than the mutable layer for
 *       {@code SHORT}, {@code BYTE}, {@code FLOAT}, {@code BOOLEAN} and the datetime family, so the
 *       type a query returned depended on whether the sample had been compacted yet.</li>
 *   <li>Types with no possible fixed-stride encoding ({@code DECIMAL}, {@code BINARY}, containers)
 *       were accepted at {@code CREATE} and then corrupted the row.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5475ColumnTypesTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  /**
   * The declared-type families that a TimeSeries row can store, with a representative value and the
   * Java type both storage layers must hand back.
   */
  private static final Object[][] SUPPORTED = {
      { "DOUBLE", 1.5d, Double.class },
      { "FLOAT", 1.5f, Float.class },
      { "LONG", 42L, Long.class },
      { "INTEGER", 42, Integer.class },
      { "SHORT", (short) 42, Short.class },
      { "BYTE", (byte) 42, Byte.class },
      { "BOOLEAN", Boolean.TRUE, Boolean.class },
      { "DATETIME", 1_700_000_123_456L, Long.class },
      { "DATE", 1_700_000_123_456L, Long.class },
      { "DATETIME_SECOND", 1_700_000_123L, Long.class },
      { "DATETIME_MICROS", 1_700_000_123_456_789L, Long.class },
      { "DATETIME_NANOS", 1_700_000_123_456_789L, Long.class },
      { "STRING", "hello", String.class },
  };

  /**
   * A column whose declared type has no case in the row writer used to leave the reader 6 bytes
   * behind for the rest of the row, so the value AND every column after it came back wrong.
   */
  @Test
  void everySupportedTypeRoundTripsWithoutDisturbingTheNextColumn() throws IOException {
    for (final Object[] spec : SUPPORTED) {
      final String declared = (String) spec[0];
      final Object value = spec[1];

      final String typeName = "Row_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName
          + " TIMESTAMP ts FIELDS (v " + declared + ", tail DOUBLE) SHARDS 1");

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
      engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1 },
          new Object[] { value, value }, new Object[] { 1.5d, 2.5d });

      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).as("%s row count", declared).hasSize(2);
      assertThat(rows.get(0)[1]).as("%s value", declared).isEqualTo(value);
      assertThat(rows.get(0)[2]).as("%s neighbour on row 0", declared).isEqualTo(1.5d);
      assertThat(rows.get(1)[2]).as("%s neighbour on row 1", declared).isEqualTo(2.5d);
    }
  }

  /**
   * The Java type a query returns must not depend on whether the sample has been compacted yet.
   */
  @Test
  void theSealedLayerReturnsTheDeclaredType() throws IOException {
    for (final Object[] spec : SUPPORTED) {
      final String declared = (String) spec[0];
      final Object value = spec[1];
      final Class<?> expected = (Class<?>) spec[2];

      final String typeName = "Sealed_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared + ") SHARDS 1");

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
      engine.appendSamples(new long[] { BASE_TS }, new Object[] { value });

      final Object mutable = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst()[1];
      assertThat(mutable).as("%s from the mutable layer", declared).isInstanceOf(expected).isEqualTo(value);

      engine.compactAll();

      final Object sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst()[1];
      assertThat(sealed).as("%s from the sealed layer", declared).isInstanceOf(expected).isEqualTo(value);
    }
  }

  /**
   * A datetime field is high-cardinality by nature, so a text dictionary is the wrong sealed encoding
   * for it: every instant is written out as digits and the dictionary itself is stored alongside. It
   * belongs on the integer codec, like {@code LONG}.
   * <p>
   * Measured against the same instants declared as {@code STRING}, which is exactly the encoding a
   * datetime field used to get.
   */
  @Test
  void aDateTimeFieldSealsAsIntegersNotText() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Instants TIMESTAMP ts FIELDS (observed DATETIME_MICROS) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE InstantsAsText TIMESTAMP ts FIELDS (observed STRING) SHARDS 1");

    final TimeSeriesEngine instants = ((LocalTimeSeriesType) database.getSchema().getType("Instants")).getEngine();
    final TimeSeriesEngine asText = ((LocalTimeSeriesType) database.getSchema().getType("InstantsAsText")).getEngine();

    final int rows = 50_000;
    final TimeSeriesBatch batch = instants.newBatch(rows);
    final long[] timestamps = new long[rows];
    final Object[] text = new Object[rows];
    for (int i = 0; i < rows; i++) {
      final long observed = 1_700_000_000_000_000L + i;
      batch.setLong(batch.addRow(BASE_TS + i), 0, observed);
      timestamps[i] = BASE_TS + i;
      text[i] = Long.toString(observed);
    }

    instants.appendSamples(batch);
    asText.appendSamples(timestamps, text);
    instants.compactAll();
    asText.compactAll();

    // The declared type decides the codec, and the codec is what the sealed store actually used.
    final long instantsBytes = instants.getShard(0).getSealedStore().getFileSizeBytes();
    final long textBytes = asText.getShard(0).getSealedStore().getFileSizeBytes();
    assertThat(instantsBytes)
        .as("delta-packed integers (%d bytes) must be far smaller than a text dictionary (%d bytes)",
            instantsBytes, textBytes)
        .isLessThan(textBytes / 2);

    final List<Object[]> result = instants.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(result).hasSize(rows);
    for (int i = 0; i < rows; i++)
      assertThat(result.get(i)[1]).as("row %d", i).isEqualTo(1_700_000_000_000_000L + i);
  }

  /**
   * A type that cannot be stored in a fixed-stride row must be refused when the type is declared,
   * not silently corrupt the row later.
   */
  @Test
  void unstorableColumnTypesAreRefusedAtCreate() {
    for (final String declared : new String[] { "DECIMAL", "BINARY", "LIST", "MAP", "LINK", "EMBEDDED" }) {
      assertThatThrownBy(() -> database.command("sql",
          "CREATE TIMESERIES TYPE Bad_" + declared + " TIMESTAMP ts FIELDS (v " + declared + ") SHARDS 1"))
          .as("%s must be refused", declared)
          .hasMessageContaining(declared)
          .hasMessageContaining("TIMESERIES");

      assertThat(database.getSchema().existsType("Bad_" + declared)).as("%s type must not exist", declared).isFalse();
    }
  }

  /**
   * A tag column carries the same restriction: it is stored in the same row.
   */
  @Test
  void unstorableTagTypesAreRefusedAtCreate() {
    assertThatThrownBy(() -> database.command("sql",
        "CREATE TIMESERIES TYPE BadTag TIMESTAMP ts TAGS (t DECIMAL) FIELDS (v DOUBLE) SHARDS 1"))
        .hasMessageContaining("DECIMAL");

    assertThat(database.getSchema().existsType("BadTag")).isFalse();
  }

  /**
   * A datetime column used as a tag stays on the dictionary codec (tags are low-cardinality by
   * definition and the tag filter compares them as strings), and must still round-trip.
   */
  @Test
  void aDateTimeTagRoundTripsThroughBothLayers() throws IOException {
    database.command("sql",
        "CREATE TIMESERIES TYPE TaggedInstant TIMESTAMP ts TAGS (day DATE) FIELDS (v DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("TaggedInstant")).getEngine();
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1 },
        new Object[] { 1_700_000_000_000L, 1_700_086_400_000L }, new Object[] { 1.5d, 2.5d });

    List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo(1_700_000_000_000L);
    assertThat(rows.get(0)[2]).isEqualTo(1.5d);

    engine.compactAll();

    rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo(1_700_000_000_000L);
    assertThat(rows.get(1)[1]).isEqualTo(1_700_086_400_000L);
  }

  /**
   * The codec is not recorded inside a sealed block, it is resolved from the schema when the store is
   * opened. So a block written by a build whose codec table differed must still be decoded with that
   * build's table: the codec is now persisted per column, and a schema that predates it falls back to
   * {@link ColumnDefinition#legacyCodecFor}.
   * <p>
   * Here a {@code DATETIME} and a {@code BOOLEAN} field are sealed with their pre-#5475 codec
   * ({@code DICTIONARY}, i.e. stored as text) and must still come back as {@code Long} and
   * {@code Boolean}.
   */
  @Test
  void sealedBlocksWrittenWithTheLegacyCodecStillDecodeToTheDeclaredType() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP,
            ColumnDefinition.legacyCodecFor(Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP)),
        new ColumnDefinition("observed", Type.DATETIME, ColumnDefinition.ColumnRole.FIELD,
            ColumnDefinition.legacyCodecFor(Type.DATETIME, ColumnDefinition.ColumnRole.FIELD)),
        new ColumnDefinition("ok", Type.BOOLEAN, ColumnDefinition.ColumnRole.FIELD,
            ColumnDefinition.legacyCodecFor(Type.BOOLEAN, ColumnDefinition.ColumnRole.FIELD)),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD,
            ColumnDefinition.legacyCodecFor(Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)));

    // The pre-#5475 table put both of these on the text dictionary; that is the premise of this test.
    assertThat(columns.get(1).getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
    assertThat(columns.get(2).getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);

    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard((DatabaseInternal) database, "legacy_codec_shard", 0, columns);
    shard.appendSamples(new long[] { BASE_TS, BASE_TS + 1 },
        new Object[] { 1_700_000_123_456L, 1_700_000_123_457L },
        new Object[] { Boolean.TRUE, Boolean.FALSE },
        new Object[] { 1.5d, 2.5d });
    database.commit();

    shard.compact();

    database.begin();
    final List<Object[]> rows = shard.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    database.commit();

    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo(1_700_000_123_456L);
    assertThat(rows.get(0)[2]).isEqualTo(Boolean.TRUE);
    assertThat(rows.get(0)[3]).isEqualTo(1.5d);
    assertThat(rows.get(1)[1]).isEqualTo(1_700_000_123_457L);
    assertThat(rows.get(1)[2]).isEqualTo(Boolean.FALSE);
    assertThat(rows.get(1)[3]).isEqualTo(2.5d);

    shard.close();
  }

  /**
   * The codec must survive a schema round-trip, and a schema entry without one must resolve to the
   * pre-#5475 table rather than to the current default.
   */
  @Test
  void theCodecIsPersistedAndFallsBackToTheLegacyTableWhenAbsent() {
    database.command("sql", "CREATE TIMESERIES TYPE Persisted TIMESTAMP ts TAGS (host STRING) "
        + "FIELDS (observed DATETIME, ok BOOLEAN, value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Persisted");
    final JSONObject json = type.toJSON();
    final JSONArray cols = json.getJSONArray("tsColumns");

    for (int i = 0; i < cols.length(); i++) {
      final JSONObject col = cols.getJSONObject(i);
      assertThat(col.getString("compression", null)).as("column %s", col.getString("name")).isNotNull();
    }

    // A DATETIME field is on the integer codec now; a schema without the entry must still say what the
    // build that wrote its blocks said.
    assertThat(type.getTsColumns().get(2).getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
    assertThat(ColumnDefinition.legacyCodecFor(Type.DATETIME, ColumnDefinition.ColumnRole.FIELD))
        .isEqualTo(TimeSeriesCodec.DICTIONARY);

    for (int i = 0; i < cols.length(); i++)
      cols.getJSONObject(i).remove("compression");

    type.fromJSON(json);
    assertThat(type.getTsColumns().get(2).getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
    assertThat(type.getTsColumns().get(3).getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
  }

  /**
   * Restoring the declared type on the sealed read path would have broken tag filtering: the SQL
   * planner stringified every literal, and it happened to match only because the sealed layer also
   * handed back the dictionary text. A tag filter must now match from both layers and whichever form
   * the literal arrives in.
   */
  @Test
  void aNonStringTagFiltersFromBothLayersAndInEitherForm() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Filtered TIMESTAMP ts TAGS (ok BOOLEAN, day DATE) "
        + "FIELDS (v DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Filtered")).getEngine();

    // Two sealed rows and one still in the mutable bucket, so a single query spans both layers.
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1 },
        new Object[] { Boolean.TRUE, Boolean.FALSE },
        new Object[] { 1_700_000_000_000L, 1_700_086_400_000L },
        new Object[] { 1.5d, 2.5d });
    engine.compactAll();
    engine.appendSamples(new long[] { BASE_TS + 2 },
        new Object[] { Boolean.TRUE }, new Object[] { 1_700_000_000_000L }, new Object[] { 3.5d });

    // Typed literal and text literal must agree, on a boolean tag and on a datetime tag.
    for (final Object trueLiteral : new Object[] { Boolean.TRUE, "true" })
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, trueLiteral)))
          .as("ok = %s (%s)", trueLiteral, trueLiteral.getClass().getSimpleName())
          .hasSize(2);

    for (final Object dayLiteral : new Object[] { 1_700_000_000_000L, "1700000000000" })
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(1, dayLiteral)))
          .as("day = %s (%s)", dayLiteral, dayLiteral.getClass().getSimpleName())
          .hasSize(2);

    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, Boolean.FALSE))).hasSize(1);

    // And through SQL, which is what actually reaches a user.
    assertThat(database.query("sql", "SELECT FROM Filtered WHERE ok = true").stream().count()).isEqualTo(2);
    assertThat(database.query("sql", "SELECT FROM Filtered WHERE ok = false").stream().count()).isEqualTo(1);
  }

  /**
   * A STRING tag must keep matching exactly, including a value that happens to look like a number or a
   * boolean: the extra match forms a filter carries can only ever equal a value of that other type.
   */
  @Test
  void aStringTagIsNotConfusedByNumericOrBooleanLookingValues() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE StringTags TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("StringTags")).getEngine();
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1, BASE_TS + 2 },
        new Object[] { "1", "true", "hostA" }, new Object[] { 1.5d, 2.5d, 3.5d });
    engine.compactAll();

    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "1"))).hasSize(1);
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "true"))).hasSize(1);
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "hostA"))).hasSize(1);
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "nope"))).isEmpty();

    assertThat(database.query("sql", "SELECT FROM StringTags WHERE host = 'hostA'").stream().count()).isEqualTo(1);
  }

  /**
   * A null on a supported column must read back as the zero of that column's type from both layers,
   * and must not shift the columns after it.
   */
  @Test
  void nullsDoNotShiftTheRow() throws IOException {
    database.command("sql",
        "CREATE TIMESERIES TYPE Nulls TIMESTAMP ts FIELDS (a DATE, b BOOLEAN, c DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Nulls")).getEngine();
    engine.appendSamples(new long[] { BASE_TS },
        new Object[] { null }, new Object[] { null }, new Object[] { 7.5d });

    final Object[] mutable = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst();
    assertThat(mutable[1]).isEqualTo(0L);
    assertThat(mutable[2]).isEqualTo(Boolean.FALSE);
    assertThat(mutable[3]).isEqualTo(7.5d);

    engine.compactAll();

    final Object[] sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst();
    assertThat(sealed[1]).isEqualTo(0L);
    assertThat(sealed[2]).isEqualTo(Boolean.FALSE);
    assertThat(sealed[3]).isEqualTo(7.5d);
  }
}
