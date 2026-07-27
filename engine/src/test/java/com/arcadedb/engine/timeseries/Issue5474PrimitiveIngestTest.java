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
import com.arcadedb.schema.LocalTimeSeriesType;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5474: the only ingest API was typed on {@code Object[]} per column, so a caller holding
 * primitive samples had to box every value into a {@code Double}/{@code Long} that the engine
 * unboxed a few frames later to write its raw bits. The boxed object never survived the call.
 * <p>
 * {@link TimeSeriesRowSource} and its array-backed implementation {@link TimeSeriesBatch} carry the
 * already-encoded raw bits, so the fast path never materialises an object per sample. These tests
 * pin both halves of that: the primitive path must store exactly what the boxed path stored, and it
 * must allocate dramatically less doing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5474PrimitiveIngestTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  @Test
  void primitiveBatchStoresExactlyWhatTheBoxedPathStores() throws IOException {
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

  @Test
  void primitiveBatchSurvivesCompaction() throws IOException {
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

  /**
   * A BYTE field reserved 1 byte in the fixed row stride but the writer fell through to the default
   * branch and wrote 8, so the value overran the neighbouring columns and the reader handed back
   * {@code null}. The primitive path shares one width table with the boxed path, which is what makes
   * the mismatch visible.
   */
  @Test
  void byteColumnRoundTrips() throws IOException {
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

  /**
   * A column the caller never sets must land as the zero the {@code Object[][]} path writes for a
   * {@code null}, and a batch reused after {@link TimeSeriesBatch#clear()} must not leak the previous
   * fill's values into the skipped columns.
   */
  @Test
  void untouchedColumnsMatchNullAndAReusedBatchDoesNotLeak() throws IOException {
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

    // Second fill on the same buffer: only the tag is set, so a and b must read back as zero.
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
    assertThat(((Number) result.get(2)[2]).doubleValue()).isEqualTo(0.0d);
    assertThat(((Number) result.get(2)[3]).longValue()).isEqualTo(0L);

    assertThat(result.get(3)[1]).isEqualTo("host_d");
    assertThat(result.get(3)[2]).isEqualTo(result.get(2)[2]);
    assertThat(result.get(3)[3]).isEqualTo(result.get(2)[3]);
  }

  /**
   * A batch filled past its declared capacity must grow without losing or reordering samples.
   */
  @Test
  void batchGrowsBeyondItsInitialCapacity() throws IOException {
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

  /**
   * The multi-shard split must route the primitive batch exactly as the boxed batch: same shard for
   * the same sample position, so a mixed-API workload keeps a stable distribution.
   */
  @Test
  void multiShardSplitRoutesTheSameSamplesToTheSameShards() throws IOException {
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

  /**
   * The async ingest API accepts the primitive batch too, since that is the path documented for
   * contention-free writes.
   */
  @Test
  void asyncAppendAcceptsAPrimitiveBatch() throws IOException {
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

  /**
   * The point of the issue: with primitive samples in hand, ingest must not allocate one object per
   * value. Measured on the calling thread so the assertion covers the marshalling the caller pays
   * for, not just the engine internals.
   */
  @Test
  void primitiveIngestAllocatesFarLessThanTheBoxedPath() throws IOException {
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
}
