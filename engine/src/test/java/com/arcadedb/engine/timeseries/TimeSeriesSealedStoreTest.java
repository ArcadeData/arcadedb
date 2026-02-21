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
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesSealedStoreTest {

  private static final String TEST_PATH = "target/databases/TimeSeriesSealedStoreTest/sealed";
  private List<ColumnDefinition> columns;

  // Stats arrays: ts(NaN), sensor_id(NaN), temperature(has stats)
  private static final double[] NO_MINS = { Double.NaN, Double.NaN, Double.NaN };
  private static final double[] NO_MAXS = { Double.NaN, Double.NaN, Double.NaN };
  private static final double[] NO_SUMS = { Double.NaN, Double.NaN, Double.NaN };

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File("target/databases/TimeSeriesSealedStoreTest"));
    new File("target/databases/TimeSeriesSealedStoreTest").mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File("target/databases/TimeSeriesSealedStoreTest"));
  }

  @Test
  void testCreateEmptyStore() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(0);
    }
  }

  @Test
  void testAppendAndReadBlock() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
      final String[] sensorIds = { "A", "B", "A", "C", "B" };
      final double[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

      final byte[][] compressed = {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      };

      store.appendBlock(5, 1000L, 5000L, compressed,
          new double[] { Double.NaN, Double.NaN, 19.5 },
          new double[] { Double.NaN, Double.NaN, 23.0 },
          new double[] { Double.NaN, Double.NaN, 106.0 });

      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1000L);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(5000L);

      // Read back
      final List<Object[]> results = store.scanRange(1000L, 5000L, null);
      assertThat(results).hasSize(5);

      assertThat((long) results.get(0)[0]).isEqualTo(1000L);
      assertThat((String) results.get(0)[1]).isEqualTo("A");
      assertThat((double) results.get(0)[2]).isEqualTo(20.0);

      assertThat((long) results.get(4)[0]).isEqualTo(5000L);
      assertThat((String) results.get(4)[1]).isEqualTo("B");
      assertThat((double) results.get(4)[2]).isEqualTo(23.0);
    }
  }

  @Test
  void testRangeFilter() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
      final String[] sensorIds = { "A", "B", "A", "C", "B" };
      final double[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

      final byte[][] compressed = {
          DeltaOfDeltaCodec.encode(timestamps),
          DictionaryCodec.encode(sensorIds),
          GorillaXORCodec.encode(temperatures)
      };
      store.appendBlock(5, 1000L, 5000L, compressed, NO_MINS, NO_MAXS, NO_SUMS);

      // Query subset
      final List<Object[]> results = store.scanRange(2000L, 4000L, null);
      assertThat(results).hasSize(3);
      assertThat((long) results.get(0)[0]).isEqualTo(2000L);
      assertThat((long) results.get(2)[0]).isEqualTo(4000L);
    }
  }

  @Test
  void testMultipleBlocks() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      // Block 1: timestamps 1000-3000
      store.appendBlock(3, 1000L, 3000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L, 3000L }),
          DictionaryCodec.encode(new String[] { "A", "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0, 12.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS);

      // Block 2: timestamps 4000-6000
      store.appendBlock(3, 4000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 4000L, 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0, 22.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS);

      assertThat(store.getBlockCount()).isEqualTo(2);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1000L);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(6000L);

      // Query spanning both blocks
      final List<Object[]> results = store.scanRange(2000L, 5000L, null);
      assertThat(results).hasSize(4);
    }
  }

  @Test
  void testBlockSkipping() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS);

      store.appendBlock(2, 5000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS);

      // Query only block 2
      final List<Object[]> results = store.scanRange(5000L, 6000L, null);
      assertThat(results).hasSize(2);
      assertThat((String) results.get(0)[1]).isEqualTo("B");
    }
  }

  @Test
  void testTruncateBefore() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
          DictionaryCodec.encode(new String[] { "A", "A" }),
          GorillaXORCodec.encode(new double[] { 10.0, 11.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS);

      store.appendBlock(2, 5000L, 6000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
          DictionaryCodec.encode(new String[] { "B", "B" }),
          GorillaXORCodec.encode(new double[] { 20.0, 21.0 })
      }, NO_MINS, NO_MAXS, NO_SUMS);

      // Truncate old data
      store.truncateBefore(3000L);
      assertThat(store.getBlockCount()).isEqualTo(1);

      final List<Object[]> results = store.scanRange(0L, 10000L, null);
      assertThat(results).hasSize(2);
      assertThat((long) results.get(0)[0]).isEqualTo(5000L);
    }
  }
}
