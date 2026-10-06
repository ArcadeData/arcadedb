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
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8813 at the level of the sealed store: blocks appended out of timestamp order (what a compaction does with the
 * late-arriving samples it finds after merging the rest) must be served in timestamp order, survive a reopen, and be
 * handled by the checks that walk the file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8813SealedStoreDirectoryOrderTest {
  private static final String ROOT      = "target/databases/Issue8813SealedStoreDirectoryOrderTest";
  private static final String BASE_PATH = ROOT + "/sealed";
  private static final long   T0        = 1_700_000_000_000L;

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(ROOT));
    new File(ROOT).mkdirs();
    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("usage", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(ROOT));
  }

  private static void appendBlock(final TimeSeriesSealedStore store, final long fromTs, final long toTs) throws IOException {
    final int samples = (int) (toTs - fromTs + 1);
    final long[] timestamps = new long[samples];
    final double[] usage = new double[samples];
    double sum = 0;
    for (int i = 0; i < samples; i++) {
      timestamps[i] = fromTs + i;
      usage[i] = i;
      sum += i;
    }
    store.appendBlock(samples, fromTs, toTs, new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(usage) },
        new double[] { Double.NaN, 0 }, new double[] { Double.NaN, samples - 1 }, new double[] { Double.NaN, sum },
        new long[] { 0, samples }, new String[][] { null, null });
  }

  /** A wide late block first on disk, then narrow ones: appended in the order a compaction's Phase 4 would. */
  private void load(final TimeSeriesSealedStore store) throws IOException {
    appendBlock(store, T0 + 1_000, T0 + 1_099);
    appendBlock(store, T0 + 2_000, T0 + 2_099);
    appendBlock(store, T0 + 3_000, T0 + 3_099);
    // late-arriving samples: older than everything above, written after it
    appendBlock(store, T0, T0 + 9);
    // and one that starts early and ends late, so maxTimestamp is not monotonic along the directory
    appendBlock(store, T0 + 500, T0 + 3_500);
  }

  private static long oldest(final TimeSeriesSealedStore store, final long fromTs, final int limit) throws IOException {
    final List<Object[]> rows = store.scanRangeAscending(fromTs, Long.MAX_VALUE, null, null, limit, null);
    return rows.isEmpty() ? -1 : (long) rows.get(0)[0];
  }

  private void assertServedInOrder(final TimeSeriesSealedStore store) throws IOException {
    assertThat(oldest(store, Long.MIN_VALUE, 1)).isEqualTo(T0);
    // a lower bound inside the wide block, past blocks that end before it: only a running max finds the wide one
    assertThat(oldest(store, T0 + 3_200, 1)).isEqualTo(T0 + 3_200);
    assertThat(store.scanRange(T0 + 3_200, T0 + 3_300, null, null)).hasSize(101);
    assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty();
  }

  @Test
  void outOfOrderBlocksAreServedInTimestampOrderAndSurviveReopen() throws IOException {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
      load(store);
      assertServedInOrder(store);
    }
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
      assertServedInOrder(store);
    }
  }

  @Test
  void truncateToBlockCountKeepsTheFirstBlocksInFileOrder() throws IOException {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
      load(store);
      // the first three blocks WRITTEN are the T0+1000, T0+2000 and T0+3000 ones, not the three oldest
      store.truncateToBlockCount(3);
      assertThat(store.getBlockCount()).isEqualTo(3);
      assertThat(oldest(store, Long.MIN_VALUE, 1)).isEqualTo(T0 + 1_000);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty();
    }
  }
}
