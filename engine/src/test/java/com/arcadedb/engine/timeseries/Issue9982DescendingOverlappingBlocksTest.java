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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #9982: {@link TimeSeriesSealedStore#scanRangeDescending} walked the block directory as if {@code maxTimestamp} were
 * monotonic along it. The directory is ordered by {@code minTimestamp} and its blocks overlap (#8813), so one block
 * ending before the lower bound - or before the oldest row retained - stopped the walk while an earlier block still
 * held newer rows. The guarantee is the differential one: for any range and limit, the descending scan answers the
 * newest rows of what {@link TimeSeriesSealedStore#scanRange} returns, over directories with overlapping blocks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9982DescendingOverlappingBlocksTest {
  private static final String ROOT      = "target/databases/Issue9982DescendingOverlappingBlocksTest";
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

  /** The directory shape of {@code Issue8813SealedStoreDirectoryOrderTest}: a wide block that starts early and ends last. */
  private static void loadReportedShape(final TimeSeriesSealedStore store) throws IOException {
    appendBlock(store, T0 + 1_000, T0 + 1_099);
    appendBlock(store, T0 + 2_000, T0 + 2_099);
    appendBlock(store, T0 + 3_000, T0 + 3_099);
    appendBlock(store, T0, T0 + 9);
    appendBlock(store, T0 + 500, T0 + 3_500);
  }

  private static long[] timestamps(final List<Object[]> rows) {
    final long[] ts = new long[rows.size()];
    for (int i = 0; i < ts.length; i++)
      ts[i] = (long) rows.get(i)[0];
    return ts;
  }

  /** The newest {@code limit} timestamps of {@code scanRange} over the same range, newest first. */
  private static long[] expectedDescending(final TimeSeriesSealedStore store, final long fromTs, final long toTs, final int limit)
      throws IOException {
    final long[] all = timestamps(store.scanRange(fromTs, toTs, null, null));
    Arrays.sort(all);
    final int n = limit > 0 ? Math.min(limit, all.length) : all.length;
    final long[] expected = new long[n];
    for (int i = 0; i < n; i++)
      expected[i] = all[all.length - 1 - i];
    return expected;
  }

  private static void assertDescendingMirrorsScanRange(final TimeSeriesSealedStore store, final long fromTs, final long toTs,
      final int limit) throws IOException {
    final long[] actual = timestamps(store.scanRangeDescending(fromTs, toTs, null, null, limit, null));
    assertThat(actual).as("descending [%d..%d] limit %d", fromTs - T0, toTs - T0, limit)
        .containsExactly(expectedDescending(store, fromTs, toTs, limit));
  }

  @Test
  void aRangeInsideTheWideBlockIsNotCutShortByANarrowOneEndingBeforeIt() throws IOException {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
      loadReportedShape(store);

      assertThat(store.scanRange(T0 + 3_200, T0 + 3_300, null, null)).hasSize(101);
      assertThat(store.scanRangeDescending(T0 + 3_200, T0 + 3_300, null, null, 0, null)).hasSize(101);
      assertDescendingMirrorsScanRange(store, T0 + 3_200, T0 + 3_300, 0);
      assertDescendingMirrorsScanRange(store, T0 + 3_200, T0 + 3_300, 10);
    }
  }

  @Test
  void theLatestSampleIsTheNewestOneInTheStore() throws IOException {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
      loadReportedShape(store);

      final List<Object[]> latest = store.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, null, 1, null);
      assertThat(latest).hasSize(1);
      assertThat((long) latest.getFirst()[0]).isEqualTo(T0 + 3_500);
      assertThat((long) latest.getFirst()[0]).isEqualTo(store.getGlobalMaxTimestamp());

      for (final int limit : new int[] { 1, 2, 50, 401, 1_000, 0 })
        assertDescendingMirrorsScanRange(store, Long.MIN_VALUE, Long.MAX_VALUE, limit);
    }
  }

  @Test
  void descendingMirrorsScanRangeOverRandomOverlappingDirectories() throws IOException {
    final Random rnd = new Random(9982);
    for (int round = 0; round < 20; round++) {
      FileUtils.deleteRecursively(new File(ROOT));
      new File(ROOT).mkdirs();
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(BASE_PATH, columns)) {
        final int blocks = 2 + rnd.nextInt(12);
        for (int b = 0; b < blocks; b++) {
          final long from = T0 + rnd.nextInt(5_000);
          // mostly narrow blocks, a few wide ones that overlap many others
          final long length = rnd.nextInt(10) == 0 ? 500 + rnd.nextInt(4_000) : 1 + rnd.nextInt(200);
          appendBlock(store, from, from + length - 1);
        }

        for (int q = 0; q < 30; q++) {
          final long a = T0 - 100 + rnd.nextInt(9_500);
          final long b = T0 - 100 + rnd.nextInt(9_500);
          final int limit = switch (rnd.nextInt(4)) {
            case 0 -> 0;
            case 1 -> 1;
            case 2 -> 1 + rnd.nextInt(20);
            default -> 1 + rnd.nextInt(2_000);
          };
          assertDescendingMirrorsScanRange(store, Math.min(a, b), Math.max(a, b), limit);
        }
        assertDescendingMirrorsScanRange(store, Long.MIN_VALUE, Long.MAX_VALUE, 1);
      }
    }
  }
}
