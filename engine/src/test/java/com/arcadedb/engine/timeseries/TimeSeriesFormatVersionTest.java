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

import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for TimeSeries disk format versioning and CRC32 integrity checks.
 */
class TimeSeriesFormatVersionTest {

  private static final String TEST_DIR  = "target/databases/TimeSeriesFormatVersionTest";
  private static final String TEST_PATH = TEST_DIR + "/sealed";

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(TEST_DIR));
    new File(TEST_DIR).mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_DIR));
  }

  @Test
  void testSealedStoreHeaderHasVersionByte() throws Exception {
    final long[] timestamps = { 1000L, 2000L, 3000L };
    final double[] values = { 10.0, 20.0, 30.0 };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(timestamps),
          GorillaXORCodec.encode(values)
      }, new double[] { Double.NaN, 10.0 }, new double[] { Double.NaN, 30.0 }, new double[] { Double.NaN, 60.0 }, null);
    }

    // Read raw file bytes and verify version byte at offset 4
    try (final RandomAccessFile raf = new RandomAccessFile(TEST_PATH + ".ts.sealed", "r")) {
      // Magic: bytes 0-3
      final int magic = raf.readInt();
      assertThat(magic).isEqualTo(0x54534958); // "TSIX"

      // Format version: byte 4
      final byte version = raf.readByte();
      assertThat(version).isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);

      // Column count: bytes 5-6
      final short colCount = raf.readShort();
      assertThat(colCount).isEqualTo((short) 2);

      // Block count: bytes 7-10
      final int blockCount = raf.readInt();
      assertThat(blockCount).isEqualTo(1);
    }
  }

  @Test
  void testSealedStoreRejectsNewerVersion() throws Exception {
    // Create a valid file first
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(1, 1000L, 1000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L }),
          GorillaXORCodec.encode(new double[] { 10.0 })
      }, new double[] { Double.NaN, 10.0 }, new double[] { Double.NaN, 10.0 }, new double[] { Double.NaN, 10.0 }, null);
    }

    // Corrupt the version byte to 99
    try (final RandomAccessFile raf = new RandomAccessFile(TEST_PATH + ".ts.sealed", "rw")) {
      raf.seek(4); // version byte offset
      raf.writeByte(99);
    }

    // Opening should fail
    assertThatThrownBy(() -> new TimeSeriesSealedStore(TEST_PATH, columns))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("version");
  }

  @Test
  void testBlockCRC32DetectsCorruption() throws Exception {
    final long[] timestamps = { 1000L, 2000L, 3000L };
    final double[] values = { 10.0, 20.0, 30.0 };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(timestamps),
          GorillaXORCodec.encode(values)
      }, new double[] { Double.NaN, 10.0 }, new double[] { Double.NaN, 30.0 }, new double[] { Double.NaN, 60.0 }, null);
    }

    // Flip a byte in the compressed data region (somewhere after the header + block meta)
    final File sealedFile = new File(TEST_PATH + ".ts.sealed");
    final long fileLen = sealedFile.length();
    try (final RandomAccessFile raf = new RandomAccessFile(sealedFile, "rw")) {
      // The CRC is the last 4 bytes of the file. Corrupt a byte just before it.
      final long corruptOffset = fileLen - 8; // well inside compressed data, before CRC
      raf.seek(corruptOffset);
      final byte original = raf.readByte();
      raf.seek(corruptOffset);
      raf.writeByte(original ^ 0xFF);
    }

    // First read should fail with CRC mismatch (CRC validated lazily on block access)
    assertThatThrownBy(() -> {
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
        store.scanRange(1000L, 3000L, null, null);
      }
    }).isInstanceOf(IOException.class)
        .hasMessageContaining("CRC");
  }

  @Test
  void testStatsWithoutColIndex() throws Exception {
    final long[] timestamps = { 1000L, 2000L, 3000L };
    final double[] values = { 10.0, 20.0, 30.0 };

    final double[] mins = { Double.NaN, 10.0 };
    final double[] maxs = { Double.NaN, 30.0 };
    final double[] sums = { Double.NaN, 60.0 };

    // Write and read back — stats should round-trip without colIdx
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(timestamps),
          GorillaXORCodec.encode(values)
      }, mins, maxs, sums, null);
    }

    // Reload and verify data + stats-based aggregation both work
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(1);

      final List<Object[]> results = store.scanRange(1000L, 3000L, null, null);
      assertThat(results).hasSize(3);
      assertThat((double) results.get(0)[1]).isEqualTo(10.0);
      assertThat((double) results.get(2)[1]).isEqualTo(30.0);

      // Verify aggregation still uses block stats correctly
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_val"),
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min_val"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max_val")
      );

      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(1000L, 3000L, requests, 3600000L, result, null, null);

      final long bucket = result.getBucketTimestamps().get(0);
      assertThat(result.getValue(bucket, 0)).isEqualTo(60.0);  // SUM
      assertThat(result.getValue(bucket, 1)).isEqualTo(10.0);  // MIN
      assertThat(result.getValue(bucket, 2)).isEqualTo(30.0);  // MAX
    }
  }

  @Test
  void testSchemaJsonFormatVersionRoundTrip() {
    final JSONObject json = new JSONObject();
    json.put("timestampColumn", "ts");
    json.put("shardCount", 1);
    json.put("retentionMs", 0L);
    json.put("sealedFormatVersion", 0);
    json.put("mutableFormatVersion", 0);
    json.put("tsColumns", new com.arcadedb.serializer.json.JSONArray());

    // Simulate fromJSON
    final int sealedVersion = json.getInt("sealedFormatVersion", 0);
    final int mutableVersion = json.getInt("mutableFormatVersion", 0);

    assertThat(sealedVersion).isEqualTo(0);
    assertThat(mutableVersion).isEqualTo(0);

    // Verify a JSON without the fields defaults to 0
    final JSONObject legacyJson = new JSONObject();
    legacyJson.put("timestampColumn", "ts");
    legacyJson.put("shardCount", 1);
    legacyJson.put("retentionMs", 0L);
    legacyJson.put("tsColumns", new com.arcadedb.serializer.json.JSONArray());

    assertThat(legacyJson.getInt("sealedFormatVersion", 0)).isEqualTo(0);
    assertThat(legacyJson.getInt("mutableFormatVersion", 0)).isEqualTo(0);
  }
}
