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

import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockEntry;
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8793: reopening a sealed store decoded every block's declared tag values into fresh Strings, so the heap held
 * (blocks x distinct values) copies of the same few thousand strings: 188 MB for 1.3M samples in 5,648 blocks, against
 * 2-3 MB for the same samples in 24 blocks. After a reopen all blocks must share ONE instance of each value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8793SharedTagValuesOnLoadTest {
  private static final String BASE_DIR   = "target/databases/Issue8793SharedTagValuesOnLoadTest";
  private static final String STORE_PATH = BASE_DIR + "/store";
  private static final int    BLOCKS     = 300;

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();
    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  @Test
  void blocksShareTheTagValuesTheyDeclareAfterAReopen() throws Exception {
    final String[] ids = { "entity-0001", "entity-0002", "entity-0003", "entity-0004" };
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, columns)) {
      for (int b = 0; b < BLOCKS; b++) {
        final long[] ts = { b * 10L, b * 10L + 1, b * 10L + 2, b * 10L + 3 };
        final double[] values = { 1.0, 2.0, 3.0, 4.0 };
        // every other block declares the set in another order, so sharing the ARRAY cannot be what makes this pass
        final String[] declared = b % 2 == 0 ? ids.clone() : new String[] { ids[3], ids[2], ids[1], ids[0] };
        store.appendBlock(4, ts[0], ts[3],
            new byte[][] { DeltaOfDeltaCodec.encode(ts), DictionaryCodec.encode(ids), GorillaXORCodec.encode(values) },
            new double[] { Double.NaN, Double.NaN, 1.0 }, new double[] { Double.NaN, Double.NaN, 4.0 },
            new double[] { Double.NaN, Double.NaN, 10.0 }, new long[] { 0, 0, 4 }, new String[][] { null, declared, null });
      }
    }

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, columns)) {
      final List<BlockEntry> blocks = store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks();
      assertThat(blocks).hasSize(BLOCKS);

      final Set<String> instances = Collections.newSetFromMap(new IdentityHashMap<>());
      for (final BlockEntry block : blocks) {
        assertThat(block.tagDistinctValues[1]).hasSize(4);
        for (final String v : block.tagDistinctValues[1])
          instances.add(v);
      }
      assertThat(instances).as("distinct String instances held for 4 distinct values across " + BLOCKS + " blocks").hasSize(4);
      assertThat(store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(BLOCKS * 4);
    }
  }
}
