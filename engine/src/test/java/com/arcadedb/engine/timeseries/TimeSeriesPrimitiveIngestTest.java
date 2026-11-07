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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Primitive batch and async ingest of time series samples: storable values, constraints, column types and tag dictionary encoding.
 */
class TimeSeriesPrimitiveIngestTest extends TestHelper {
  private static final long BASE_TS = 1_700_000_000_000L;

  // Issue #5474: the only ingest API was typed on {@code Object[]} per column, so a caller holding
  // primitive samples had to box every value into a {@code Double}/{@code Long} that the engine
  // unboxed a few frames later to write its raw bits. The boxed object never survived the call.
  // <p>
  // {@link TimeSeriesRowSource} and its array-backed implementation {@link TimeSeriesBatch} carry the
  // already-encoded raw bits, so the fast path never materialises an object per sample. These tests
  // pin both halves of that: the primitive path must store exactly what the boxed path stored, and it
  // must allocate dramatically less doing it.
  // #5474
  @Test
  void primitiveBatchStoresExactlyWhatTheBoxedPathStores() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Boxed TIMESTAMP ts TAGS (host STRING) "
        + "FIELDS (d DOUBLE, f FLOAT, l LONG, i INTEGER, s SHORT, b BOOLEAN) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE Primitive TIMESTAMP ts TAGS (host STRING) "
        + "FIELDS (d DOUBLE, f FLOAT, l LONG, i INTEGER, s SHORT, b BOOLEAN) SHARDS 1");

    final TimeSeriesEngine boxed = ((LocalTimeSeriesType) database.getSchema().getType("Boxed")).getEngine();
    final TimeSeriesEngine primitive = ((LocalTimeSeriesType) database.getSchema().getType("Primitive")).getEngine();

    final int rows = 500;

    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] doubles = new Object[rows];
    final Object[] floats = new Object[rows];
    final Object[] longs = new Object[rows];
    final Object[] ints = new Object[rows];
    final Object[] shorts = new Object[rows];
    final Object[] booleans = new Object[rows];

    final TimeSeriesBatch batch = primitive.newBatch(rows);

    for (int i = 0; i < rows; i++) {
      final long ts = BASE_TS + i;
      timestamps[i] = ts;
      hosts[i] = "host_" + (i % 7);
      doubles[i] = i * 1.5d;
      floats[i] = i * 0.25f;
      longs[i] = (long) i * 1_000_000_000L;
      ints[i] = i * 3;
      shorts[i] = (short) (i % 100);
      booleans[i] = i % 2 == 0;

      final int row = batch.addRow(ts);
      batch.setString(row, 0, (String) hosts[i]);
      batch.setDouble(row, 1, (Double) doubles[i]);
      batch.setFloat(row, 2, (Float) floats[i]);
      batch.setLong(row, 3, (Long) longs[i]);
      batch.setInt(row, 4, (Integer) ints[i]);
      batch.setShort(row, 5, (Short) shorts[i]);
      batch.setBoolean(row, 6, (Boolean) booleans[i]);
    }

    boxed.appendSamples(timestamps, hosts, doubles, floats, longs, ints, shorts, booleans);
    primitive.appendSamples(batch);

    final List<Object[]> boxedRows = boxed.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    final List<Object[]> primitiveRows = primitive.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);

    assertThat(primitiveRows).hasSize(rows);
    assertThat(boxedRows).hasSize(rows);

    for (int i = 0; i < rows; i++)
      assertThat(primitiveRows.get(i)).as("row %d", i).isEqualTo(boxedRows.get(i));
  }

  // #5474
  @Test
  void primitiveBatchSurvivesCompaction() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Sealed TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sealed")).getEngine();

    final int rows = 5_000;
    final TimeSeriesBatch batch = engine.newBatch(rows);
    for (int i = 0; i < rows; i++) {
      final int row = batch.addRow(BASE_TS + i * 1_000L);
      batch.setString(row, 0, "host_" + (i % 4));
      batch.setDouble(row, 1, i * 0.5d);
    }

    engine.appendBatch(batch);
    engine.compactAll();

    final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(result).hasSize(rows);
    for (int i = 0; i < rows; i++) {
      assertThat((long) result.get(i)[0]).isEqualTo(BASE_TS + i * 1_000L);
      assertThat(result.get(i)[1]).isEqualTo("host_" + (i % 4));
      assertThat(((Number) result.get(i)[2]).doubleValue()).isEqualTo(i * 0.5d);
    }
  }

  // #5474
  /**
   * A BYTE field reserved 1 byte in the fixed row stride but the writer fell through to the default
   * branch and wrote 8, so the value overran the neighbouring columns and the reader handed back
   * {@code null}. The primitive path shares one width table with the boxed path, which is what makes
   * the mismatch visible.
   */
  @Test
  void byteColumnRoundTrips() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE WithByte TIMESTAMP ts FIELDS (flags BYTE, value DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("WithByte")).getEngine();

    final int rows = 64;
    final TimeSeriesBatch batch = engine.newBatch(rows);
    for (int i = 0; i < rows; i++) {
      final int row = batch.addRow(BASE_TS + i);
      batch.setByte(row, 0, (byte) (i - 32));
      batch.setDouble(row, 1, i * 2.5d);
    }
    engine.appendSamples(batch);

    final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(result).hasSize(rows);
    for (int i = 0; i < rows; i++) {
      assertThat(((Number) result.get(i)[1]).byteValue()).as("byte row %d", i).isEqualTo((byte) (i - 32));
      assertThat(((Number) result.get(i)[2]).doubleValue()).as("double row %d", i).isEqualTo(i * 2.5d);
    }
  }

  // #5474
  /**
   * A column the caller never sets must land as the zero the {@code Object[][]} path writes for a
   * {@code null}, and a batch reused after {@link TimeSeriesBatch#clear()} must not leak the previous
   * fill's values into the skipped columns.
   */
  @Test
  void untouchedColumnsMatchNullAndAReusedBatchDoesNotLeak() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Sparse TIMESTAMP ts TAGS (host STRING) FIELDS (a DOUBLE, b LONG) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sparse")).getEngine();

    // First fill: every column set.
    final TimeSeriesBatch batch = engine.newBatch(2);
    int row = batch.addRow(BASE_TS);
    batch.setString(row, 0, "host_a");
    batch.setDouble(row, 1, 42.5d);
    batch.setLong(row, 2, 7L);
    row = batch.addRow(BASE_TS + 1);
    batch.setString(row, 0, "host_b");
    batch.setDouble(row, 1, 43.5d);
    batch.setLong(row, 2, 8L);
    engine.appendSamples(batch);

    // Second fill on the same buffer: only the tag is set, so a and b read back as "no measurement" - the absent
    // marker on the DOUBLE column (issue #7743) and null on the LONG column, whose row carries a null bitmap (issue #9721).
    batch.clear();
    row = batch.addRow(BASE_TS + 2);
    batch.setString(row, 0, "host_c");
    engine.appendSamples(batch);

    // Same shape through the boxed path, with explicit nulls.
    engine.appendSamples(new long[] { BASE_TS + 3 },
        new Object[] { "host_d" }, new Object[] { null }, new Object[] { null });

    final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(result).hasSize(4);

    assertThat(result.get(2)[1]).isEqualTo("host_c");
    assertThat(((Number) result.get(2)[2]).doubleValue()).as("a DOUBLE nobody measured is absent, not zero").isNaN();
    assertThat(result.get(2)[3]).as("a LONG nobody measured is absent, not zero").isNull();

    // The point of the test, and it survives #7743: a column left unset and a column set to null are the same
    // thing, whatever that thing reads back as.
    assertThat(result.get(3)[1]).isEqualTo("host_d");
    assertThat(((Number) result.get(3)[2]).doubleValue()).isNaN();
    assertThat(result.get(3)[3]).isEqualTo(result.get(2)[3]);
  }

  // #5474
  /**
   * A batch filled past its declared capacity must grow without losing or reordering samples.
   */
  @Test
  void batchGrowsBeyondItsInitialCapacity() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Grown TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Grown")).getEngine();

    final int rows = 1_000;
    final TimeSeriesBatch batch = engine.newBatch(1);
    for (int i = 0; i < rows; i++) {
      final int row = batch.addRow(BASE_TS + i);
      batch.setString(row, 0, "host_" + i);
      batch.setDouble(row, 1, i * 1.25d);
    }
    assertThat(batch.size()).isEqualTo(rows);

    engine.appendSamples(batch);

    final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(result).hasSize(rows);
    for (int i = 0; i < rows; i++) {
      assertThat(result.get(i)[1]).as("tag row %d", i).isEqualTo("host_" + i);
      assertThat(((Number) result.get(i)[2]).doubleValue()).as("value row %d", i).isEqualTo(i * 1.25d);
    }
  }

  // #5474
  /**
   * The multi-shard split must route the primitive batch exactly as the boxed batch: same shard for
   * the same sample position, so a mixed-API workload keeps a stable distribution.
   */
  @Test
  void multiShardSplitRoutesTheSameSamplesToTheSameShards() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE ShardBoxed TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 4");
    database.command("sql", "CREATE TIMESERIES TYPE ShardPrim TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 4");

    final TimeSeriesEngine boxed = ((LocalTimeSeriesType) database.getSchema().getType("ShardBoxed")).getEngine();
    final TimeSeriesEngine primitive = ((LocalTimeSeriesType) database.getSchema().getType("ShardPrim")).getEngine();

    final int rows = 997; // deliberately not a multiple of the shard count
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[1][rows];
    final TimeSeriesBatch batch = primitive.newBatch(rows);
    for (int i = 0; i < rows; i++) {
      timestamps[i] = BASE_TS + i;
      columns[0][i] = i * 1.0d;
      batch.setDouble(batch.addRow(BASE_TS + i), 0, i * 1.0d);
    }

    boxed.appendBatch(timestamps, columns);
    primitive.appendBatch(batch);

    int routed = 0;
    for (int s = 0; s < 4; s++) {
      final List<Object[]> boxedShard = boxed.getShard(s).scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      final List<Object[]> primitiveShard = primitive.getShard(s).scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);

      assertThat(primitiveShard).as("shard %d row count", s).hasSameSizeAs(boxedShard);
      assertThat(primitiveShard).as("shard %d must not be empty", s).isNotEmpty();
      for (int i = 0; i < boxedShard.size(); i++)
        assertThat(primitiveShard.get(i)).as("shard %d row %d", s, i).isEqualTo(boxedShard.get(i));
      routed += primitiveShard.size();
    }
    assertThat(routed).isEqualTo(rows);
  }

  // #5474
  /**
   * The async ingest API accepts the primitive batch too, since that is the path documented for
   * contention-free writes.
   */
  @Test
  void asyncAppendAcceptsAPrimitiveBatch() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Async TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Async")).getEngine();

    final int batches = 8;
    final int perBatch = 250;
    for (int b = 0; b < batches; b++) {
      final TimeSeriesBatch batch = engine.newBatch(perBatch);
      for (int i = 0; i < perBatch; i++) {
        final int sample = b * perBatch + i;
        final int row = batch.addRow(BASE_TS + sample);
        batch.setString(row, 0, "host_" + (sample % 3));
        batch.setDouble(row, 1, sample * 0.75d);
      }
      database.async().appendSamples("Async", batch);
    }
    database.async().waitCompletion();

    final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(result).hasSize(batches * perBatch);
    for (int i = 0; i < result.size(); i++) {
      assertThat((long) result.get(i)[0]).isEqualTo(BASE_TS + i);
      assertThat(result.get(i)[1]).isEqualTo("host_" + (i % 3));
      assertThat(((Number) result.get(i)[2]).doubleValue()).isEqualTo(i * 0.75d);
    }
  }

  // #5474
  /**
   * The point of the issue: with primitive samples in hand, ingest must not allocate one object per
   * value. Measured on the calling thread so the assertion covers the marshalling the caller pays
   * for, not just the engine internals.
   */
  @Test
  void primitiveIngestAllocatesFarLessThanTheBoxedPath() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE AllocBoxed TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE, c DOUBLE) SHARDS 1");
    database.command("sql", "CREATE TIMESERIES TYPE AllocPrim TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE, c DOUBLE) SHARDS 1");

    final TimeSeriesEngine boxed = ((LocalTimeSeriesType) database.getSchema().getType("AllocBoxed")).getEngine();
    final TimeSeriesEngine primitive = ((LocalTimeSeriesType) database.getSchema().getType("AllocPrim")).getEngine();

    final int rows = 50_000;

    // Warm-up so class loading and JIT do not land inside the measured windows.
    appendBoxed(boxed, 256);
    appendPrimitive(primitive, 256);

    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    final long boxedBefore = threads.getCurrentThreadAllocatedBytes();
    appendBoxed(boxed, rows);
    final long boxedAllocated = threads.getCurrentThreadAllocatedBytes() - boxedBefore;

    final long primitiveBefore = threads.getCurrentThreadAllocatedBytes();
    appendPrimitive(primitive, rows);
    final long primitiveAllocated = threads.getCurrentThreadAllocatedBytes() - primitiveBefore;

    // 3 DOUBLE columns x `rows` samples: the boxed path allocates one Double (16 bytes on a 64-bit
    // JVM) per value on top of the reference slot. The primitive path stores 8 bytes per value with
    // no object at all, so the saving is at least 12 bytes per value.
    final long values = (long) rows * 3;
    assertThat(boxedAllocated - primitiveAllocated)
        .as("boxed=%d bytes, primitive=%d bytes for %d values", boxedAllocated, primitiveAllocated, values)
        .isGreaterThan(values * 12);

    assertThat(boxed.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(rows + 256);
    assertThat(primitive.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(rows + 256);
  }

  private static void appendBoxed(final TimeSeriesEngine engine, final int rows) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[3][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = BASE_TS + i;
      columns[0][i] = i * 1.0d;
      columns[1][i] = i * 2.0d;
      columns[2][i] = i * 3.0d;
    }
    engine.appendSamples(timestamps, columns);
  }

  private static void appendPrimitive(final TimeSeriesEngine engine, final int rows) throws IOException {
    final TimeSeriesBatch batch = engine.newBatch(rows);
    for (int i = 0; i < rows; i++) {
      final int row = batch.addRow(BASE_TS + i);
      batch.setDouble(row, 0, i * 1.0d);
      batch.setDouble(row, 1, i * 2.0d);
      batch.setDouble(row, 2, i * 3.0d);
    }
    engine.appendSamples(batch);
  }

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
      { "DATETIME_NANOS", 1_700_000_123_456_789_000L, Long.class },
      { "STRING", "hello", String.class },
  };

  // Issue #5475: the mutable row format and the sealed block format disagreed about what a TimeSeries
  // column's declared type means.
  // <ul>
  //   <li>{@code DATE} and the sub-millisecond {@code DATETIME_*} variants had no case in the row
  //       writer or reader, so the writer advanced the row cursor by 8 bytes while the reader advanced
  //       by 2. Everything after such a column in the row was read from the wrong offset.</li>
  //   <li>The sealed layer handed back a different Java type than the mutable layer for
  //       {@code SHORT}, {@code BYTE}, {@code FLOAT}, {@code BOOLEAN} and the datetime family, so the
  //       type a query returned depended on whether the sample had been compacted yet.</li>
  //   <li>Types with no possible fixed-stride encoding ({@code DECIMAL}, {@code BINARY}, containers)
  //       were accepted at {@code CREATE} and then corrupted the row.</li>
  // </ul>
  // #5475
  /**
   * A column whose declared type has no case in the row writer used to leave the reader 6 bytes
   * behind for the rest of the row, so the value AND every column after it came back wrong.
   */
  @Test
  void everySupportedTypeRoundTripsWithoutDisturbingTheNextColumn() throws Exception {
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

  // #5475
  /**
   * The Java type a query returns must not depend on whether the sample has been compacted yet.
   */
  @Test
  void theSealedLayerReturnsTheDeclaredType() throws Exception {
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

  // #5475
  /**
   * A datetime field is high-cardinality by nature, so a text dictionary is the wrong sealed encoding
   * for it: every instant is written out as digits and the dictionary itself is stored alongside. It
   * belongs on the integer codec, like {@code LONG}.
   * <p>
   * Measured against the same instants declared as {@code STRING}, which is exactly the encoding a
   * datetime field used to get.
   */
  @Test
  void aDateTimeFieldSealsAsIntegersNotText() throws Exception {
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

  // #5475
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

  // #5475
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

  // #5475
  /**
   * A datetime column used as a tag stays on the dictionary codec (tags are low-cardinality by
   * definition and the tag filter compares them as strings), and must still round-trip.
   */
  @Test
  void aDateTimeTagRoundTripsThroughBothLayers() throws Exception {
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

  // #5475
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

  // #5475
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

  // #5475
  /**
   * Restoring the declared type on the sealed read path would have broken tag filtering: the SQL
   * planner stringified every literal, and it happened to match only because the sealed layer also
   * handed back the dictionary text. A tag filter must now match from both layers and whichever form
   * the literal arrives in.
   */
  @Test
  void aNonStringTagFiltersFromBothLayersAndInEitherForm() throws Exception {
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

  // #5475
  /**
   * A STRING tag must keep matching exactly, including a value that happens to look like a number or a
   * boolean: the extra match forms a filter carries can only ever equal a value of that other type.
   */
  @Test
  void aStringTagIsNotConfusedByNumericOrBooleanLookingValues() throws Exception {
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

  // #5475
  /**
   * A null on a supported column must read back as the zero of that column's type from both layers,
   * and must not shift the columns after it.
   */
  @Test
  void nullsDoNotShiftTheRow() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Nulls TIMESTAMP ts FIELDS (a DATE, b BOOLEAN, c DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Nulls")).getEngine();
    engine.appendSamples(new long[] { BASE_TS },
        new Object[] { null }, new Object[] { null }, new Object[] { 7.5d });

    // a null reads back as null on both layers since issue #9721, and the column after it is still where it was
    final Object[] mutable = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst();
    assertThat(mutable[1]).isNull();
    assertThat(mutable[2]).isNull();
    assertThat(mutable[3]).isEqualTo(7.5d);

    engine.compactAll();

    final Object[] sealed = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).getFirst();
    assertThat(sealed[1]).isNull();
    assertThat(sealed[2]).isNull();
    assertThat(sealed[3]).isEqualTo(7.5d);
  }

  /**
   * The schema {@code cpu_influx.lp} actually ships: ten tags, plus fields.
   */
  private static final String[] TSBS_TAGS = { "hostname", "region", "datacenter", "rack", "os", "arch", "team", "service",
      "service_version", "service_environment" };

  private TimeSeriesEngine createTsbsType(final String typeName, final int shards) {
    final StringBuilder tags = new StringBuilder();
    for (final String tag : TSBS_TAGS) {
      if (!tags.isEmpty())
        tags.append(", ");
      tags.append(tag).append(" STRING");
    }
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (" + tags
        + ") FIELDS (usage_user DOUBLE, usage_system DOUBLE, usage_idle DOUBLE) SHARDS " + shards);
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  /**
   * Appends {@code rows} samples whose tag values cycle over {@code hosts} distinct hostnames.
   */
  private void appendRows(final TimeSeriesEngine engine, final int rows, final int hosts) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[TSBS_TAGS.length + 3][rows];

    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % hosts);
      columns[1][i] = "eu-west-1";
      columns[2][i] = "dc_" + (i % 4);
      columns[3][i] = "rack_" + (i % 8);
      columns[4][i] = "Ubuntu16";
      columns[5][i] = "x86";
      columns[6][i] = "SF";
      columns[7][i] = "7";
      columns[8][i] = "1";
      columns[9][i] = "production";
      columns[10][i] = (double) i;
      columns[11][i] = i * 2.0;
      columns[12][i] = i * 3.0;
    }
    engine.appendBatch(timestamps, columns);
  }

  // Issue #5519: {@code TimeSeriesBucket.calculateRowSize()} reserved {@code 2 + MAX_STRING_BYTES} for
  // every STRING column, so the TSBS cpu-only schema - ten tags and three fields - paid a 2612-byte
  // stride to store ~110 bytes of payload. That fits 25 rows in a 64 KB page, a 4% fill, and since
  // {@code MutablePage.MAX_MODIFIED_RANGES} is 8, the scattered writes collapsed to the page hull and
  // the WAL shipped whole pages: 131 MB written to store 5.5 MB.
  // <p>
  // TAG STRING columns are now dictionary-encoded into a 4-byte id, which is what the sealed layer
  // already did for the same columns. The tests below pin the new stride and, more importantly, that
  // nothing observable changed: the same values come back, through the same readers, before and after
  // compaction and across a reopen.
  // #5519
  /**
   * The defect, stated as a number. Before: {@code 8 + 10 * 258 + 3 * 8 = 2612} bytes and 25 rows per
   * 64 KB page. After: {@code 8 + 10 * 4 + 3 * 8 = 72}.
   */
  @Test
  void tenTagStrideCollapsesToFourBytesPerTag() {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();

    assertThat(bucket.getRowSize()).isEqualTo(8 + TSBS_TAGS.length * 4 + 3 * 8);
    assertThat(bucket.getRowSize()).isEqualTo(72);

    // 25 rows per page was the reported symptom; the page now carries an order of magnitude more.
    assertThat(bucket.getMaxSamplesPerPage()).isGreaterThan(900);
  }

  // #5519
  /**
   * The shape {@code cpu_influx.lp} actually ships - ten tags and ten fields, not the ten-and-three
   * the issue's own table used. Reported on #5519 by @tae898, whose measurement put it at a 2,668-byte
   * stride and 24 rows per page, the worst of the three arms they ran.
   */
  @Test
  void theShippingTsbsShapeOfTenTagsAndTenFields() {
    final StringBuilder tags = new StringBuilder();
    for (final String tag : TSBS_TAGS) {
      if (!tags.isEmpty())
        tags.append(", ");
      tags.append(tag).append(" STRING");
    }
    final StringBuilder fields = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      if (!fields.isEmpty())
        fields.append(", ");
      fields.append("f").append(i).append(" DOUBLE");
    }
    database.command("sql",
        "CREATE TIMESERIES TYPE CpuFull TIMESTAMP ts TAGS (" + tags + ") FIELDS (" + fields + ") SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("CpuFull")).getEngine();
    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();

    // Was 8 + 10 * 258 + 10 * 8 = 2668 bytes and 24 rows per 64KB page
    assertThat(bucket.getRowSize()).isEqualTo(8 + 10 * 4 + 10 * 8);
    assertThat(bucket.getRowSize()).isEqualTo(128);
    assertThat(bucket.getMaxSamplesPerPage()).isEqualTo(511);
  }

  // #5519
  @Test
  void tagValuesRoundTripThroughTheDictionary() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 2_000, 7);

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(2_000);

    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    for (int i = 0; i < 2_000; i++) {
      final Object[] row = rows.get(i);
      assertThat(row[0]).isEqualTo(1_700_000_000_000L + i * 1_000L);
      assertThat(row[1]).isEqualTo("host_" + (i % 7));
      assertThat(row[2]).isEqualTo("eu-west-1");
      assertThat(row[3]).isEqualTo("dc_" + (i % 4));
      assertThat(row[4]).isEqualTo("rack_" + (i % 8));
      assertThat(row[10]).isEqualTo("production");
      assertThat(row[11]).isEqualTo((double) i);
      assertThat(row[13]).isEqualTo(i * 3.0);
    }
  }

  // #5519
  /**
   * Id 0 is reserved so a null tag still reads back as {@code ""}, which is what the inline encoding
   * did with its zero-length payload.
   */
  @Test
  void nullTagStillReadsBackAsEmptyString() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);

    final Object[][] columns = new Object[TSBS_TAGS.length + 3][1];
    for (int c = 0; c < TSBS_TAGS.length; c++)
      columns[c][0] = null;
    columns[10][0] = 1.0;
    columns[11][0] = 2.0;
    columns[12][0] = 3.0;
    engine.appendBatch(new long[] { 1_700_000_000_000L }, columns);

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    for (int c = 1; c <= TSBS_TAGS.length; c++)
      assertThat(rows.getFirst()[c]).isEqualTo("");
  }

  // #5519
  @Test
  void tagFilterMatchesThroughTheDictionary() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 1_000, 5);

    final List<Object[]> matching = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_3"));
    assertThat(matching).hasSize(200);
    for (final Object[] row : matching)
      assertThat(row[1]).isEqualTo("host_3");

    // Newest-first path evaluates the filter straight off the page, now as an int compare.
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_3"), 3, null);
    assertThat(newest).hasSize(3);
    for (final Object[] row : newest)
      assertThat(row[1]).isEqualTo("host_3");
    assertThat((long) newest.getFirst()[0]).isGreaterThan((long) newest.get(1)[0]);
  }

  // #5519
  /**
   * A filter value the dictionary has never seen cannot appear in any row, so the condition is
   * unsatisfiable and the scan short-circuits without decoding a page.
   */
  @Test
  void filterOnANeverStoredTagValueMatchesNothing() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 500, 3);

    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_does_not_exist"))).isEmpty();
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_does_not_exist"), 10,
        null)).isEmpty();
  }

  // #5519
  /**
   * A STRING FIELD is where high-cardinality text belongs, so it is not interned: it keeps the inline
   * encoding and the wide reservation that goes with it.
   */
  @Test
  void stringFieldsAreNotDictionaryEncoded() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Logs TIMESTAMP ts TAGS (host STRING) FIELDS (message STRING, level INTEGER) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Logs")).getEngine();

    // 8 (ts) + 1 (null bitmap for level, issue #9721) + 4 (dictionary-encoded host) + 258 (inline message) + 4 (level)
    assertThat(engine.getShard(0).getMutableBucket().getRowSize())
        .isEqualTo(8 + 1 + 4 + (2 + TimeSeriesBucket.MAX_STRING_BYTES) + 4);
  }

  // #5519
  @Test
  void stringFieldValuesStillRoundTrip() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Logs TIMESTAMP ts TAGS (host STRING) FIELDS (message STRING, level INTEGER) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Logs")).getEngine();

    engine.appendBatch(new long[] { 1_000L, 2_000L },
        new Object[][] { { "web01", "web02" }, { "started", "stopped" }, { 1, 2 } });

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    assertThat(rows.get(0)[1]).isEqualTo("web01");
    assertThat(rows.get(0)[2]).isEqualTo("started");
    assertThat(rows.get(1)[1]).isEqualTo("web02");
    assertThat(rows.get(1)[2]).isEqualTo("stopped");
  }

  // #5519
  /**
   * The dictionary is per type: every shard resolves a value to the same id, and one file backs them
   * all.
   */
  @Test
  void allShardsShareOneDictionary() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 4);
    appendRows(engine, 2_000, 11);

    final TimeSeriesTagDictionary dictionary = engine.getTagDictionary();
    assertThat(dictionary).isNotNull();
    for (int i = 0; i < 4; i++)
      assertThat(engine.getShard(i).getMutableBucket().getTagDictionary()).isSameAs(dictionary);

    // 11 hostnames + 1 region + 4 datacenters + 8 racks + 6 single-valued tags
    assertThat(dictionary.size()).isEqualTo(11 + 1 + 4 + 8 + 6);

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_9"));
    assertThat(rows).hasSize(2_000 / 11 + (2_000 % 11 > 9 ? 1 : 0));
  }

  // #5519
  @Test
  void tagValuesSurviveCompaction() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 3_000, 6);

    engine.getShard(0).compact();
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isPositive();

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_4"));
    assertThat(rows).hasSize(500);
    for (final Object[] row : rows) {
      assertThat(row[1]).isEqualTo("host_4");
      assertThat(row[10]).isEqualTo("production");
    }
  }

  // #5519
  @Test
  void tagValuesSurviveAReopen() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 2);
    appendRows(engine, 1_500, 9);

    reopenDatabase();

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Cpu");
    final TimeSeriesEngine reopened = type.getEngine();
    assertThat(reopened.getTagDictionary()).isNotNull();
    assertThat(reopened.getTagDictionary().size()).isEqualTo(9 + 1 + 4 + 8 + 6);

    final List<Object[]> rows = reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1_500);

    rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
    for (int i = 0; i < 1_500; i++) {
      assertThat(rows.get(i)[1]).isEqualTo("host_" + (i % 9));
      assertThat(rows.get(i)[4]).isEqualTo("rack_" + (i % 8));
    }

    // And an append after the reopen keeps assigning ids from where the reload left off
    appendRows(reopened, 100, 20);
    assertThat(reopened.getTagDictionary().size()).isEqualTo(20 + 1 + 4 + 8 + 6);
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_15"))).isNotEmpty();
  }

  // #5519
  /**
   * The dictionary file is now discovered on open like any other component, so dropping the type has
   * to take it with it. A leftover would be re-adopted by a type recreated under the same name and
   * hand back tag values that belong to the dropped one.
   */
  @Test
  void droppingTheTypeRemovesTheDictionaryFile() throws Exception {
    final TimeSeriesEngine engine = createTsbsType("Cpu", 1);
    appendRows(engine, 200, 5);
    assertThat(listTagDictionaryFiles()).hasSize(1);

    database.command("sql", "DROP TYPE Cpu");
    assertThat(listTagDictionaryFiles()).isEmpty();

    // Recreating under the same name starts from an empty id space, and survives a reopen
    final TimeSeriesEngine recreated = createTsbsType("Cpu", 1);
    assertThat(recreated.getTagDictionary().size()).isZero();
    appendRows(recreated, 100, 3);

    reopenDatabase();
    final TimeSeriesEngine reopened = ((LocalTimeSeriesType) database.getSchema().getType("Cpu")).getEngine();
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(100);
    assertThat(reopened.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_2"))).isNotEmpty();
  }

  private List<String> listTagDictionaryFiles() {
    final List<String> found = new ArrayList<>();
    final java.io.File[] files = new java.io.File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final java.io.File file : files)
        if (file.getName().endsWith("." + TimeSeriesTagDictionary.DICT_EXT))
          found.add(file.getName());
    return found;
  }

  // #5519
  /**
   * Backward compatibility: a bucket built without a dictionary is a format-version-0 bucket. It
   * keeps the inline tag encoding, the wide stride that comes with it, and reads back the same
   * values, which is what lets a database written before this change open unchanged.
   */
  @Test
  void aBucketWithoutADictionaryKeepsTheInlineFormat() throws Exception {
    final List<ColumnDefinition> columns = new ArrayList<>();
    columns.add(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP));
    columns.add(new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG));
    columns.add(new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final DatabaseInternal db = (DatabaseInternal) database;
    database.begin();
    final TimeSeriesBucket bucket =
        new TimeSeriesBucket(db, "legacy_inline", db.getDatabasePath() + "/legacy_inline", columns);
    ((LocalSchema) db.getSchema()).registerFile(bucket);
    bucket.initHeaderPage();

    assertThat(bucket.getTagDictionary()).isNull();
    assertThat(bucket.getRowSize()).isEqualTo(8 + (2 + TimeSeriesBucket.MAX_STRING_BYTES) + 8);

    bucket.appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { "web01", null },
        new Object[] { 1.5, 2.5 });
    database.commit();

    database.begin();
    final List<Object[]> rows = bucket.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo("web01");
    assertThat(rows.get(0)[2]).isEqualTo(1.5);
    // A null tag reads back as "" in both formats
    assertThat(rows.get(1)[1]).isEqualTo("");
    database.commit();
  }

  // #5519
  /**
   * The compatibility claim end to end, through a real open rather than through a null dictionary: a
   * database whose stored schema says mutable format 0 keeps the inline layout when this build opens
   * it, and never adopts the dictionary. Simulated by relabelling the type on disk before the pages
   * hold anything, since a v1 type whose rows already carry 4-byte ids could not honestly be called v0.
   */
  @Test
  void aTypeStoredAsVersion0OpensWithTheInlineFormat() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Legacy TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    final String databasePath = database.getDatabasePath();
    database.close();

    // Rewrite the stored schema as a pre-#5519 one: mutable format 0, and no dictionary component
    final File schemaFile = new File(databasePath + "/schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject legacy = schema.getJSONObject("types").getJSONObject("Legacy");
    assertThat(legacy.getInt("mutableFormatVersion", -1)).isEqualTo(TimeSeriesBucket.CURRENT_VERSION);
    legacy.put("mutableFormatVersion", 0);
    Files.writeString(schemaFile.toPath(), schema.toString(), StandardCharsets.UTF_8);

    final File[] dictFiles = new File(databasePath).listFiles((dir, fileName) -> fileName.startsWith("Legacy_tags."));
    if (dictFiles != null)
      for (final File dictFile : dictFiles)
        assertThat(dictFile.delete()).isTrue();

    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("Legacy");
    final TimeSeriesEngine engine = reopened.getEngine();

    // No dictionary, and the row is back to the reserved inline slot
    assertThat(engine.getTagDictionary()).isNull();
    assertThat(engine.getShard(0).getMutableBucket().getRowSize())
        .isEqualTo(8 + (2 + TimeSeriesBucket.MAX_STRING_BYTES) + 8);

    // ...and it still reads and writes tags correctly through the normal path
    database.begin();
    engine.appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { "web01", "web02" }, new Object[] { 1.5, 2.5 });
    database.commit();

    database.begin();
    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo("web01");
    assertThat(rows.get(1)[1]).isEqualTo("web02");
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "web02"))).hasSize(1);
    database.commit();

    // Writing it back out must not relabel it as current, or the next open would misread these rows
    assertThat(reopened.toJSON().getInt("mutableFormatVersion", -1)).isZero();
  }

  /**
   * Issue #10487: {@code database.async().appendSamples(type, TimeSeriesRowSource)} promised that a primitive source "holds values its
   * columns can store by construction", and it does not. A TAG or STRING text over the 256 UTF-8 bytes a row can hold was accepted by
   * the call and then failed inside the async worker, taking the whole batch (the conforming rows too) with it, with nobody to tell. And
   * {@link TimeSeriesBatch#setValue}, documented to store "exactly what the {@code Object[][]} path would have stored", narrowed a long
   * past the INTEGER range modulo 2^32, turned any text of a BOOLEAN column into {@code false} and threw a bare
   * {@code ClassCastException} for numeric text, which the SQL INSERT converts. The door now refuses an unstorable text on the calling
   * thread, as the boxed overload does, and {@code setValue} converts and refuses like {@link ColumnDefinition#storableValue}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PrimitiveAppendStorable {
    private static final int MAX_BYTES = TimeSeriesBucket.MAX_STRING_BYTES;

    @BeforeEach
    void createType() {
      database.command("sql",
          "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER, l LONG, d DOUBLE, s STRING, b BOOLEAN) SHARDS 2");
    }

    // #10487
    @Test
    void aTagOverTheLimitIsRefusedToTheCallerAndNothingOfTheBatchIsLost() throws Exception {
      final AtomicInteger workerErrors = new AtomicInteger();
      database.async().onError(e -> workerErrors.incrementAndGet());

      final TimeSeriesBatch batch = engine().newBatch(3);
      fill(batch, 1000L, "h1", 10);
      fill(batch, 2000L, "h".repeat(3000), 10);
      fill(batch, 3000L, "h3", 10);

      assertThatThrownBy(() -> database.async().appendSamples("M", batch)).isInstanceOf(ValidationException.class)
          .hasMessageContaining("M.host").hasMessageContaining(String.valueOf(MAX_BYTES)).hasMessageContaining("1 of 3 rows");
      database.async().waitCompletion();

      assertThat(count()).isZero();
      assertThat(workerErrors.get()).as("the worker never saw the batch").isZero();
    }

    // #10487
    @Test
    void aStringFieldOverTheLimitIsRefusedToTheCaller() {
      final TimeSeriesBatch batch = engine().newBatch(2);
      int row = batch.addRow(1000L);
      batch.setString(row, 0, "h");
      batch.setString(row, 4, "s".repeat(MAX_BYTES + 1));
      row = batch.addRow(2000L);
      batch.setString(row, 0, "h");
      batch.setString(row, 4, "ok");

      assertThatThrownBy(() -> database.async().appendSamples("M", batch)).isInstanceOf(ValidationException.class)
          .hasMessageContaining("M.s").hasMessageContaining("1 of 2 rows");
      database.async().waitCompletion();
      assertThat(count()).isZero();
    }

    // #10487
    /** The limit counts UTF-8 bytes, not chars: 86 three-byte characters are 258 bytes, 85 are 255. */
    @Test
    void theLimitCountsUtf8BytesAndAtTheLimitIsStored() {
      final String threeBytes = "€"; // EURO SIGN, 3 UTF-8 bytes

      final TimeSeriesBatch over = engine().newBatch(1);
      fill(over, 1000L, threeBytes.repeat(86), 1);
      assertThatThrownBy(() -> database.async().appendSamples("M", over)).isInstanceOf(ValidationException.class)
          .hasMessageContaining("M.host");

      final TimeSeriesBatch within = engine().newBatch(2);
      fill(within, 2000L, threeBytes.repeat(85), 1);
      fill(within, 3000L, "x".repeat(MAX_BYTES), 2);
      assertThatCode(() -> database.async().appendSamples("M", within)).doesNotThrowAnyException();
      database.async().waitCompletion();

      assertThat(count()).isEqualTo(2);
    }

    // #10487
    /** The type has constraints too: the length is still checked, together with them, and the whole batch is refused. */
    @Test
    void aTypeWithConstraintsRefusesBothTheConstraintAndTheLengthInOneAnswer() {
      database.command("sql", "ALTER PROPERTY M.a MAX 50");

      final TimeSeriesBatch batch = engine().newBatch(3);
      fill(batch, 1000L, "h1", 10);
      fill(batch, 2000L, "h".repeat(3000), 10);
      fill(batch, 3000L, "h3", 999);

      assertThatThrownBy(() -> database.async().appendSamples("M", batch)).isInstanceOf(ValidationException.class)
          .hasMessageContaining("M.host").hasMessageContaining("M.a").hasMessageContaining("2 of 3 rows");
      database.async().waitCompletion();
      assertThat(count()).isZero();
    }

    // #10487
    @Test
    void aConformingBatchIsStoredAsBefore() {
      final TimeSeriesBatch batch = engine().newBatch(2);
      fill(batch, 1000L, "h1", 10);
      fill(batch, 2000L, "h2", 20);
      database.async().appendSamples("M", batch);
      database.async().waitCompletion();

      assertThat(count()).isEqualTo(2);
    }

    // #10487
    @Test
    void aNullTagIsNotATextOverTheLimit() {
      final TimeSeriesBatch batch = engine().newBatch(1);
      fill(batch, 1000L, null, 10);
      assertThatCode(() -> database.async().appendSamples("M", batch)).doesNotThrowAnyException();
      database.async().waitCompletion();
      assertThat(count()).isEqualTo(1);
    }

    // #10487
    // ------------------------------------------------------------------------------------------------------------------------------
    // TimeSeriesBatch.setValue converts like ColumnDefinition.storableValue, i.e. like the INSERT

    @Test
    void setValueRefusesALongPastTheIntegerRangeInsteadOfStoringItModulo2To32() {
      final TimeSeriesBatch batch = engine().newBatch(1);
      final int row = batch.addRow(1000L);
      assertThatThrownBy(() -> batch.setValue(row, 1, 4294967316L)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("INTEGER column 'a'").hasMessageContaining("out of range");
      // the INSERT refuses the same value
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 1000, host = 'h', a = 4294967316")).isNotNull();
    }

    // #10487
    @Test
    void setValueDoesNotTurnTextOrANumberIntoFalseForABooleanColumn() {
      final TimeSeriesBatch batch = engine().newBatch(1);
      final int row = batch.addRow(1000L);
      final int booleanColumn = 5;
      assertThatThrownBy(() -> batch.setValue(row, booleanColumn, "maybe")).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("BOOLEAN column 'b'");
      assertThatThrownBy(() -> batch.setValue(row, booleanColumn, 1)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("BOOLEAN column 'b'");

      batch.setValue(row, booleanColumn, "true");
      assertThat(batch.getRawValue(row, booleanColumn)).isEqualTo(1L);
      batch.setValue(row, booleanColumn, "FALSE");
      assertThat(batch.getRawValue(row, booleanColumn)).isZero();
      batch.setValue(row, booleanColumn, Boolean.TRUE);
      assertThat(batch.getRawValue(row, booleanColumn)).isEqualTo(1L);
    }

    // #10487
    @Test
    void setValueConvertsNumericTextLikeTheInsert() {
      final TimeSeriesBatch batch = engine().newBatch(1);
      final int row = batch.addRow(1000L);
      batch.setString(row, 0, "h");
      batch.setValue(row, 1, "20");
      batch.setValue(row, 2, "123456789012");
      batch.setValue(row, 3, "1.5");
      assertThat(batch.getRawValue(row, 1)).isEqualTo(20L);
      assertThat(batch.getRawValue(row, 2)).isEqualTo(123456789012L);
      assertThat(Double.longBitsToDouble(batch.getRawValue(row, 3))).isEqualTo(1.5d);

      assertThatThrownBy(() -> batch.setValue(row, 1, "abc")).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("INTEGER column 'a'");
    }

    // #10487
    @Test
    void setValueRefusesATextOverTheLimitForAStringColumnAndANonTextForIt() {
      final TimeSeriesBatch batch = engine().newBatch(1);
      final int row = batch.addRow(1000L);
      assertThatThrownBy(() -> batch.setValue(row, 0, "h".repeat(MAX_BYTES + 1))).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("host");
      assertThatThrownBy(() -> batch.setValue(row, 4, 42)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("STRING column 's'");
    }

    // #10487
    /** What setValue stores is what the SQL INSERT stores, value for value. */
    @Test
    void aBatchFilledWithSetValueStoresWhatTheInsertStores() throws Exception {
      database.command("sql", "INSERT INTO M SET ts = 1000, host = 'sql', a = '20', l = '123456789012', d = '1.5', s = 'x', b = 'true'");

      final TimeSeriesBatch batch = engine().newBatch(1);
      final int row = batch.addRow(1000L);
      batch.setValue(row, 0, "batch");
      batch.setValue(row, 1, "20");
      batch.setValue(row, 2, "123456789012");
      batch.setValue(row, 3, "1.5");
      batch.setValue(row, 4, "x");
      batch.setValue(row, 5, "true");
      database.async().appendSamples("M", batch);
      database.async().waitCompletion();

      final Result viaSql = row("sql");
      final Result viaBatch = row("batch");
      for (final String column : new String[] { "a", "l", "d", "s", "b" })
        assertThat(viaBatch.<Object>getProperty(column)).as(column).isEqualTo(viaSql.<Object>getProperty(column));
    }

    // #10487
    @Test
    void setValueKeepsTheNullAndTheNumberPaths() {
      final TimeSeriesBatch batch = engine().newBatch(1);
      final int row = batch.addRow(1000L);
      batch.setValue(row, 1, null);
      assertThat(batch.isNull(row, 1)).isTrue();
      batch.setValue(row, 1, 7);
      assertThat(batch.isNull(row, 1)).isFalse();
      assertThat(batch.getRawValue(row, 1)).isEqualTo(7L);
      batch.setValue(row, 3, 7.9d);
      assertThat(Double.longBitsToDouble(batch.getRawValue(row, 3))).isEqualTo(7.9d);
      batch.setValue(row, 5, null);
      assertThat(batch.isNull(row, 5)).isTrue();
    }

    // #10487
    /**
     * A batch built without a database (the public constructor) reads date text with the default formats and the JVM's time zone, as
     * {@code ColumnDefinition.storableValue} does with no database; one made by the engine reads it as the INSERT does, so the two name
     * the same instant only when the JVM's zone is UTC.
     */
    @Test
    void setValueOnABatchWithoutADatabaseReadsDateTextWithTheDefaultFormats() {
      database.command("sql", "CREATE TIMESERIES TYPE D TIMESTAMP ts TAGS (host STRING) FIELDS (t DATETIME) SHARDS 1");
      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("D");
      final LocalDateTime text = LocalDateTime.of(2020, 1, 2, 3, 4, 5);

      final TimeSeriesBatch withoutDatabase = new TimeSeriesBatch(type.getTsColumns(), 1);
      final int row = withoutDatabase.addRow(1000L);
      withoutDatabase.setValue(row, 1, "2020-01-02 03:04:05");
      assertThat(withoutDatabase.getRawValue(row, 1)).isEqualTo(text.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

      final TimeSeriesBatch withDatabase = type.getEngine().newBatch(1);
      final int other = withDatabase.addRow(1000L);
      withDatabase.setValue(other, 1, "2020-01-02 03:04:05");
      // the text names the same instant the INSERT stores for it: the wall clock of the text read as UTC
      assertThat(withDatabase.getRawValue(other, 1)).isEqualTo(text.toInstant(ZoneOffset.UTC).toEpochMilli());

      // a number is a count in the column's unit, whatever the constructor
      withoutDatabase.setValue(row, 1, 1_577_934_246_000L);
      assertThat(withoutDatabase.getRawValue(row, 1)).isEqualTo(1_577_934_246_000L);
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
    }

    private static void fill(final TimeSeriesBatch batch, final long ts, final String host, final int a) {
      final int row = batch.addRow(ts);
      batch.setString(row, 0, host);
      batch.setInt(row, 1, a);
    }

    private Result row(final String host) {
      try (final ResultSet rs = database.query("sql", "SELECT a, l, d, s, b FROM M WHERE host = ?", host)) {
        assertThat(rs.hasNext()).as("a row for host " + host).isTrue();
        return rs.next();
      }
    }

    private long count() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM M")) {
        return rs.next().<Number>getProperty("n").longValue();
      }
    }
  }

  /**
   * Issue #10381: {@code database.async().appendSamples()} handed the engine a grid of its own, so a TIMESERIES sample that
   * MANDATORY / MIN / MAX refuse everywhere else was stored (and the type was then wedged: the constraint already in force could not
   * be declared again, and the sample could not be deleted), while a value the column cannot store at all threw inside the async
   * worker and the batch was silently lost. Both overloads now convert and check on the calling thread, with the code every other
   * surface uses, and refuse the whole batch.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncAppendSamplesConstraints {
    @BeforeEach
    void createType() {
      database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER) SHARDS 1");
      database.command("sql", "ALTER PROPERTY M.host MANDATORY true");
      database.command("sql", "ALTER PROPERTY M.a MIN 5");
      database.command("sql", "ALTER PROPERTY M.a MAX 50");
    }

    // #10381
    @Test
    void sqlRefusesTheSampleTheAsyncApiUsedToStore() {
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 3000, a = 999")).isInstanceOf(ValidationException.class);
    }

    // #10381
    @Test
    void aNullMandatoryTagIsRefusedToTheCaller() {
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { null }, new Object[] { 10 }))
          .isInstanceOf(ValidationException.class).hasMessageContaining("M.host").hasMessageContaining("MANDATORY");
      database.async().waitCompletion();
      assertThat(count()).isZero();
    }

    // #10381
    @Test
    void aFieldPastItsMaxIsRefusedToTheCaller() {
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }, new Object[] { 999 }))
          .isInstanceOf(ValidationException.class).hasMessageContaining("M.a").hasMessageContaining("MAX 50");
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }, new Object[] { 1 }))
          .isInstanceOf(ValidationException.class).hasMessageContaining("M.a").hasMessageContaining("MIN 5");
      database.async().waitCompletion();
      assertThat(count()).isZero();
    }

    // #10381
    /** The type is not wedged any more: the constraint in force can be declared again, because nothing violating it was stored. */
    @Test
    void theTypeStaysAlterable() {
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }, new Object[] { 999 }))
          .isInstanceOf(ValidationException.class);
      database.async().waitCompletion();

      assertThatCode(() -> database.command("sql", "ALTER PROPERTY M.a MAX 50")).doesNotThrowAnyException();
    }

    // #10381
    /** A value no column can store was lost inside the worker, with waitCompletion() returning normally. */
    @Test
    void aValueTheColumnCannotStoreIsRefusedToTheCaller() {
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 2000L }, new Object[] { "h" }, new Object[] { "not-a-number" }))
          .isInstanceOf(ValidationException.class).hasMessageContaining("INTEGER column 'a'");
      database.async().waitCompletion();
      assertThat(count()).isZero();
    }

    // #10381
    /** All or nothing: one refused row keeps the conforming ones of the same call out too, and the message counts them. */
    @Test
    void aBatchWithOneRefusedRowAppendsNothing() {
      assertThatThrownBy(() -> database.async()
          .appendSamples("M", new long[] { 1000L, 2000L, 3000L }, new Object[] { "h", "h", "h" }, new Object[] { 10, 999, 20 }))
          .isInstanceOf(ValidationException.class).hasMessageContaining("1 of 3 rows");
      database.async().waitCompletion();
      assertThat(count()).isZero();
    }

    // #10381
    @Test
    void aConformingBatchIsStoredAndTextForANumericColumnIsConvertedLikeEverySurface() {
      final long[] timestamps = { 1000L, 2000L };
      final Object[] hosts = { "h1", "h2" };
      final Object[] values = { 10, "20" };
      database.async().appendSamples("M", timestamps, hosts, values);
      database.async().waitCompletion();

      assertThat(count()).isEqualTo(2);
      try (final ResultSet rs = database.query("sql", "SELECT a FROM M WHERE host = 'h2'")) {
        assertThat(rs.next().<Number>getProperty("a").intValue()).isEqualTo(20);
      }
      // the caller's arrays are its own again
      hosts[0] = "changed";
      values[0] = 999;
      assertThat(count()).isEqualTo(2);
    }

    // #10381
    @Test
    void aShapeThatIsNotTheTypesIsRefused() {
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("2 value columns");
      assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L, 2000L }, new Object[] { "h" }, new Object[] { 10 }))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("2 timestamps");
    }

    // #10381
    @Test
    void aPrimitiveSourceIsHeldToTheConstraintsToo() throws Exception {
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();

      final TimeSeriesBatch violating = engine.newBatch(2);
      int row = violating.addRow(1000L);
      violating.setString(row, 0, "h");
      violating.setInt(row, 1, 10);
      row = violating.addRow(2000L);
      violating.setString(row, 0, "h");
      violating.setInt(row, 1, 999);
      assertThatThrownBy(() -> database.async().appendSamples("M", violating)).isInstanceOf(ValidationException.class)
          .hasMessageContaining("M.a").hasMessageContaining("MAX 50").hasMessageContaining("1 of 2 rows");

      final TimeSeriesBatch noTag = engine.newBatch(1);
      row = noTag.addRow(3000L);
      noTag.setString(row, 0, null);
      noTag.setInt(row, 1, 10);
      assertThatThrownBy(() -> database.async().appendSamples("M", noTag)).isInstanceOf(ValidationException.class)
          .hasMessageContaining("M.host");

      database.async().waitCompletion();
      assertThat(count()).isZero();

      final TimeSeriesBatch conforming = engine.newBatch(1);
      row = conforming.addRow(4000L);
      conforming.setString(row, 0, "h");
      conforming.setInt(row, 1, 10);
      database.async().appendSamples("M", conforming);
      database.async().waitCompletion();
      assertThat(count()).isEqualTo(1);
    }

    // #10381
    /** A type with no constraint on any column pays nothing and refuses nothing it did not before. */
    @Test
    void aTypeWithoutConstraintsIsUnchanged() {
      database.command("sql", "CREATE TIMESERIES TYPE Plain TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER) SHARDS 1");
      database.async().appendSamples("Plain", new long[] { 1000L }, new Object[] { null }, new Object[] { 999 });
      database.async().waitCompletion();

      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Plain")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
      }
    }

    private long count() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM M")) {
        return rs.next().<Number>getProperty("n").longValue();
      }
    }
  }
}
