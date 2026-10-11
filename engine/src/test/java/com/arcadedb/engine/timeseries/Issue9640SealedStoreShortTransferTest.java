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
import com.arcadedb.utility.ShortTransferFileChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9640: {@link TimeSeriesSealedStore} wrote its 27-byte header with one unlooped positional
 * {@code FileChannel.write} and read it back with one unchecked {@code FileChannel.read}, and its directory scan took
 * any short read of a block record for the end of the file. A {@link java.nio.channels.FileChannel} may legally
 * transfer fewer bytes than asked for, so every one of those calls has to loop.
 * <p>
 * A local-disk channel essentially never returns short, so the bug cannot show against one: every channel the store
 * opens is routed here through {@link ShortTransferFileChannel}, which never moves more than a few bytes per call.
 * Every path that writes or reads a header - create, append, reopen, compaction (including the append-to-temp step
 * that re-reads the temp header), truncation by time and by block count - then has to produce a file that a plain
 * channel reads back whole.
 */
class Issue9640SealedStoreShortTransferTest {

  private static final String ROOT      = "target/databases/Issue9640SealedStoreShortTransferTest";
  private static final String BASE_PATH = ROOT + "/sealed";
  private static final long   T0        = 1_700_000_000_000L;
  private static final int    SAMPLES   = 10;

  private       List<ColumnDefinition>         columns;
  /** Every channel the store opened while the decorator was installed, to prove the short path was taken. */
  private final List<ShortTransferFileChannel> channels = new CopyOnWriteArrayList<>();

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(ROOT));
    new File(ROOT).mkdirs();
    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("usage", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    TimeSeriesSealedStore.TEST_CHANNEL_DECORATOR = channel -> {
      final ShortTransferFileChannel shortChannel = new ShortTransferFileChannel(channel, 3);
      channels.add(shortChannel);
      return shortChannel;
    };
  }

  @AfterEach
  void tearDown() {
    TimeSeriesSealedStore.TEST_CHANNEL_DECORATOR = null;
    FileUtils.deleteRecursively(new File(ROOT));
  }

  private TimeSeriesSealedStore open() throws IOException {
    return new TimeSeriesSealedStore(BASE_PATH, columns);
  }

  /** Block {@code b} covers {@code [T0 + b * 100_000, T0 + b * 100_000 + 9_000]}, ten samples, hosts by block. */
  private static long blockMinTs(final int b) {
    return T0 + b * 100_000L;
  }

  private static long blockMaxTs(final int b) {
    return blockMinTs(b) + (SAMPLES - 1) * 1_000L;
  }

  private static String[] hostsOf(final int b) {
    return new String[] { "host_" + b + "_a", "host_" + b + "_b" };
  }

  private static byte[][] compressedBlock(final int b) {
    final long[] timestamps = new long[SAMPLES];
    final String[] hosts = new String[SAMPLES];
    final double[] usage = new double[SAMPLES];
    final String[] declared = hostsOf(b);
    for (int i = 0; i < SAMPLES; i++) {
      timestamps[i] = blockMinTs(b) + i * 1_000L;
      hosts[i] = declared[i % 2];
      usage[i] = b * 100 + i;
    }
    return new byte[][] { DeltaOfDeltaCodec.encode(timestamps), DictionaryCodec.encode(hosts),
        GorillaXORCodec.encode(usage) };
  }

  private static double usageSum(final int b) {
    double sum = 0;
    for (int i = 0; i < SAMPLES; i++)
      sum += b * 100 + i;
    return sum;
  }

  private static double[] mins(final int b) {
    return new double[] { Double.NaN, Double.NaN, b * 100 };
  }

  private static double[] maxs(final int b) {
    return new double[] { Double.NaN, Double.NaN, b * 100 + SAMPLES - 1 };
  }

  private static double[] sums(final int b) {
    return new double[] { Double.NaN, Double.NaN, usageSum(b) };
  }

  private static long[] counts() {
    return new long[] { 0, 0, SAMPLES };
  }

  private static String[][] tagValues(final int b) {
    return new String[][] { null, hostsOf(b), null };
  }

  private static void appendBlock(final TimeSeriesSealedStore store, final int b) throws IOException {
    store.appendBlock(SAMPLES, blockMinTs(b), blockMaxTs(b), compressedBlock(b), mins(b), maxs(b), sums(b), counts(),
        tagValues(b));
  }

  /**
   * The header as the file holds it, read with a plain stream: what the next process would decode, independent of
   * the store's own reader.
   */
  private static long[] headerOnDisk() throws IOException {
    final ByteBuffer header = ByteBuffer.wrap(Files.readAllBytes(new File(BASE_PATH + ".ts.sealed").toPath()));
    assertThat(header.getInt()).as("magic").isEqualTo(0x54534958); // "TSIX"
    header.get(); // version
    assertThat(header.getShort()).as("column count").isEqualTo((short) 3);
    return new long[] { header.getInt(), header.getLong(), header.getLong() };
  }

  /** Reopens the store (through the short channel too) and checks it holds exactly blocks {@code expected}. */
  private void assertReopensHolding(final int... expected) throws IOException {
    final long[] header = headerOnDisk();
    assertThat(header[0]).as("block count in the header").isEqualTo(expected.length);
    assertThat(header[1]).as("min timestamp in the header").isEqualTo(blockMinTs(expected[0]));
    assertThat(header[2]).as("max timestamp in the header").isEqualTo(blockMaxTs(expected[expected.length - 1]));

    try (final TimeSeriesSealedStore store = open()) {
      assertThat(store.getBlockCount()).as("blocks the directory scan found").isEqualTo(expected.length);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(blockMinTs(expected[0]));
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(blockMaxTs(expected[expected.length - 1]));

      final List<Object[]> rows = store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(expected.length * SAMPLES);

      final Set<String> hosts = new TreeSet<>();
      for (final Object[] row : rows)
        hosts.add((String) row[1]);
      final Set<String> expectedHosts = new TreeSet<>();
      for (final int b : expected)
        expectedHosts.addAll(List.of(hostsOf(b)));
      assertThat(hosts).isEqualTo(expectedHosts);

      // The tag metadata section, read value by value through the short channel by the directory scan
      for (final TimeSeriesSealedStore.BlockEntry entry : store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
        assertThat(entry.tagDistinctValues[1]).hasSize(2);
    }
  }

  @Test
  void createAppendAndReopenRoundTripTheHeaderThroughShortTransfers() throws IOException {
    try (final TimeSeriesSealedStore store = open()) {
      for (int b = 0; b < 3; b++)
        appendBlock(store, b);
    }

    assertReopensHolding(0, 1, 2);
    assertThat(channels).as("every channel the store opened went through the short-transfer decorator").isNotEmpty();
    assertThat(channels.stream().mapToInt(c -> c.shortTransfers.get()).sum())
        .as("the short channel must actually have shortened transfers").isPositive();
  }

  @Test
  void anEmptyStoreHeaderSurvivesShortTransfers() throws IOException {
    try (final TimeSeriesSealedStore store = open()) {
      assertThat(store.getBlockCount()).isZero();
    }
    assertThat(new File(BASE_PATH + ".ts.sealed").length()).as("the whole empty header").isEqualTo(27);

    try (final TimeSeriesSealedStore store = open()) {
      assertThat(store.getBlockCount()).isZero();
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(Long.MAX_VALUE);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(Long.MIN_VALUE);
    }
  }

  @Test
  void compactionWritesAndRereadsTheTempHeaderThroughShortTransfers() throws IOException {
    try (final TimeSeriesSealedStore store = open()) {
      appendBlock(store, 0);
      appendBlock(store, 1);

      final List<TimeSeriesSealedStore.BlockEntry> directory = store.writeTempCompactionFile(List.<byte[][]>of(compressedBlock(2)),
          List.of(new long[] { blockMinTs(2), blockMaxTs(2), SAMPLES }),
          List.of(new TimeSeriesSealedStore.BlockStats(mins(2), maxs(2), sums(2), counts())), List.<String[][]>of(tagValues(2)));
      // Re-reads the temp file's header, then patches its count and range in place
      store.appendBlocksToTempFile(List.<byte[][]>of(compressedBlock(3)),
          List.of(new long[] { blockMinTs(3), blockMaxTs(3), SAMPLES }),
          List.of(new TimeSeriesSealedStore.BlockStats(mins(3), maxs(3), sums(3), counts())), List.<String[][]>of(tagValues(3)),
          directory);
      store.commitTempCompactionFile(directory);
    }

    assertReopensHolding(0, 1, 2, 3);
  }

  @Test
  void truncationRewritesTheHeaderThroughShortTransfers() throws IOException {
    try (final TimeSeriesSealedStore store = open()) {
      for (int b = 0; b < 4; b++)
        appendBlock(store, b);
      store.truncateBefore(blockMinTs(1));
    }
    assertReopensHolding(1, 2, 3);

    try (final TimeSeriesSealedStore store = open()) {
      store.truncateToBlockCount(2);
    }
    assertReopensHolding(1, 2);
  }
}
