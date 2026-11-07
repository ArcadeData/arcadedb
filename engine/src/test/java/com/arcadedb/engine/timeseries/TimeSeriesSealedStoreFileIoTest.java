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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.ShortTransferFileChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Sealed store file handling: channel reopen, short transfers, rewrite leftovers and exclusion, file ownership,
 * stray files at create, schema reload and wide integer compaction. Each nested group keeps its own fixture and
 * carries the issue number it regression-tests.
 */
class TimeSeriesSealedStoreFileIoTest {

  /**
   * Issue #9621: the sealed store keeps one {@code FileChannel} for its whole life, and an interrupt landing on any thread
   * blocked in a read on it ({@code ClosedByInterruptException}) closes it for every thread. Before the fix nothing reopened
   * it, so every later read, {@code getFileSizeBytes} and {@code checkIntegrity} failed with {@code ClosedChannelException}
   * or "Stream Closed" until a compaction happened to swap the file, while the bytes on disk were intact.
   * <p>
   * The decoded-column cache is disabled (budget 0) so every read below really goes to the channel.
   * <p>
   * The channel is left interruptible (issue #10372): the store now protects it from interrupts where the JVM allows it, and
   * these tests are about the recovery a JVM without {@code --add-opens java.base/java.nio.channels.spi} still depends on.
   */
  @Nested
  class SealedStoreChannelReopen {

    private static final String ROOT      = "target/databases/Issue9621SealedStoreChannelReopenTest";
    private static final String BASE_PATH = ROOT + "/sealed";
    private static final int    BLOCKS    = 4;
    private static final int    SAMPLES   = 64;
    private static final long   BASE_TS   = 1_700_000_000_000L;

    private List<ColumnDefinition> columns;
    private TimeSeriesSealedStore  store;

    @BeforeEach
    void setUp() throws IOException {
      FileUtils.deleteRecursively(new File(ROOT));
      new File(ROOT).mkdirs();
      TimeSeriesSealedStore.TEST_INTERRUPTIBLE_CHANNEL = true;

      columns = List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
          new ColumnDefinition("usage", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

      store = open();
      for (int b = 0; b < BLOCKS; b++)
        appendBlock(store, BASE_TS + (long) b * SAMPLES * 1_000L);
      store.flushHeader();
    }

    @AfterEach
    void tearDown() {
      TimeSeriesSealedStore.TEST_INTERRUPTIBLE_CHANNEL = false;
      if (store != null)
        try {
          store.close();
        } catch (final IOException ignored) {
          // ALREADY CLOSED OR DROPPED BY THE TEST
        }
      FileUtils.deleteRecursively(new File(ROOT));
    }

    private TimeSeriesSealedStore open() throws IOException {
      return new TimeSeriesSealedStore(BASE_PATH, columns, 0L);
    }

    private static void appendBlock(final TimeSeriesSealedStore target, final long firstTs) throws IOException {
      final long[] timestamps = new long[SAMPLES];
      final String[] hosts = new String[SAMPLES];
      final double[] usage = new double[SAMPLES];
      double sum = 0;
      for (int i = 0; i < SAMPLES; i++) {
        timestamps[i] = firstTs + i * 1_000L;
        hosts[i] = "host_" + (i % 3);
        usage[i] = i * 0.5;
        sum += usage[i];
      }
      final byte[][] compressed = { DeltaOfDeltaCodec.encode(timestamps), DictionaryCodec.encode(hosts), GorillaXORCodec.encode(usage) };
      target.appendBlock(SAMPLES, timestamps[0], timestamps[SAMPLES - 1], compressed,
          new double[] { Double.NaN, Double.NaN, usage[0] },
          new double[] { Double.NaN, Double.NaN, usage[SAMPLES - 1] },
          new double[] { Double.NaN, Double.NaN, sum },
          new long[] { 0, 0, SAMPLES },
          new String[][] { null, { "host_0", "host_1", "host_2" }, null });
    }

    private static int countRows(final TimeSeriesSealedStore target) throws IOException {
      return target.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null).size();
    }

    /**
     * The repro of the issue: a reader whose interrupt flag is already set closes the channel on its first positioned read.
     * The reader itself must still get its rows and keep its interrupt flag, and every thread after it must read normally.
     */
    // #9621
    @Test
    void anInterruptedReaderDoesNotPoisonTheStoreForEveryoneElse() throws Exception {
      final AtomicInteger interruptedReaderRows = new AtomicInteger(-1);
      final AtomicBoolean flagKept = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread reader = new Thread(() -> {
        Thread.currentThread().interrupt();
        try {
          interruptedReaderRows.set(countRows(store));
        } catch (final Throwable t) {
          failure.set(t);
        }
        flagKept.set(Thread.currentThread().isInterrupted());
      });
      reader.start();
      reader.join();

      assertThat(failure.get()).isNull();
      assertThat(interruptedReaderRows.get()).isEqualTo(BLOCKS * SAMPLES);
      assertThat(flagKept.get()).isTrue();
      assertThat(store.getReopenCountForTest()).isPositive();

      assertThat(Thread.currentThread().isInterrupted()).isFalse();
      for (int attempt = 0; attempt < 3; attempt++)
        assertThat(countRows(store)).isEqualTo(BLOCKS * SAMPLES);
      assertThat(store.getFileSizeBytes()).isEqualTo(new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION).length());
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    /**
     * The channel left closed by ANOTHER thread's interrupt (simulated by closing it directly, which is what NIO does to
     * every user of the channel). Every entry point that touches the file must reopen it and work: reads, the file size,
     * the integrity check, the HA whole-file read, an append and the header flush that follows it.
     */
    // #9621
    @Test
    void everyEntryPointRecoversAChannelAnotherThreadLeftClosed() throws Exception {
      store.getIndexChannelForTest().close();
      assertThat(countRows(store)).isEqualTo(BLOCKS * SAMPLES);

      store.getIndexChannelForTest().close();
      assertThat(store.getFileSizeBytes()).isEqualTo(new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION).length());

      store.getIndexChannelForTest().close();
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();

      store.getIndexChannelForTest().close();
      assertThat(store.readWholeSealedFile()).hasSize((int) new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION).length());

      store.getIndexChannelForTest().close();
      appendBlock(store, BASE_TS + (long) BLOCKS * SAMPLES * 1_000L);

      store.getIndexChannelForTest().close();
      store.flushHeader();

      store.close();
      store = open();
      assertThat(store.getBlockCount()).isEqualTo(BLOCKS + 1);
      assertThat(countRows(store)).isEqualTo((BLOCKS + 1) * SAMPLES);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    /**
     * Interrupts landing MID-OPERATION, on readers sharing the channel: each interrupt closes the channel under every other
     * reader in the middle of its scan. Every scan must still return every row, and the store must be healthy afterwards.
     */
    // #9621
    @Test
    void readersInterruptedMidScanAllGetTheirRows() throws Exception {
      final int readers = 4;
      final int scansPerReader = 50;
      final Thread[] threads = new Thread[readers];
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicInteger wrongCounts = new AtomicInteger();
      final AtomicBoolean done = new AtomicBoolean();
      final CountDownLatch scansDone = new CountDownLatch(readers);

      // Each reader scans at least scansPerReader times, and on until an interrupt has landed on a read in flight: a scan
      // is fast enough that a fixed number of them can all slip between two interrupts, which proved nothing
      for (int r = 0; r < readers; r++) {
        threads[r] = new Thread(() -> {
          try {
            for (int i = 0; i < scansPerReader || !done.get(); i++) {
              Thread.interrupted(); // A CANCELLED CALLER WOULD STOP; HERE EACH SCAN STARTS CLEAN AND IS INTERRUPTED MID-WAY
              if (countRows(store) != BLOCKS * SAMPLES)
                wrongCounts.incrementAndGet();
              if (i == scansPerReader - 1)
                scansDone.countDown();
            }
          } catch (final Throwable t) {
            failure.compareAndSet(null, t);
            scansDone.countDown();
          }
        });
        threads[r].start();
      }

      final Thread interrupter = new Thread(() -> {
        int next = 0;
        while (!done.get()) {
          threads[next++ % readers].interrupt();
          LockSupport.parkNanos(200_000L);
        }
      });
      interrupter.start();

      // A hang detector, not a latency bound: an interrupt lands on a read within milliseconds
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      final boolean quotaScanned;
      try {
        quotaScanned = scansDone.await(60, TimeUnit.SECONDS);
        while (quotaScanned && store.getReopenCountForTest() == 0 && failure.get() == null && System.nanoTime() < deadline)
          LockSupport.parkNanos(1_000_000L);
      } finally {
        // Stopped and joined on every path, a timed-out wait included: no reader may outlive the test into its teardown
        done.set(true);
        for (final Thread t : threads)
          t.join();
        interrupter.join();
      }
      assertThat(quotaScanned).as("every reader scanned its quota within the hang detector").isTrue();

      assertThat(failure.get()).isNull();
      assertThat(wrongCounts.get()).isZero();
      // THE INTERRUPTS DID LAND ON READS IN FLIGHT: OTHERWISE THIS TEST WOULD PROVE NOTHING
      assertThat(store.getReopenCountForTest()).isPositive();
      assertThat(countRows(store)).isEqualTo(BLOCKS * SAMPLES);
      assertThat(store.getIndexChannelForTest().isOpen()).isTrue();
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    /**
     * Other threads' interrupts keep closing the shared channel while one reader recovers (#10223). A reader that retried on
     * that same channel lost the race whenever a close landed between its reopen and its read, five times in a row, and
     * surfaced a {@code ClosedChannelException} from a scan that no interrupt of its own had touched.
     * <p>
     * The closer here is the worst case of that: it closes the channel again the moment it is open, so a read through the
     * shared channel never succeeds. Nothing but a recovery that does not depend on the shared channel survives it, and the
     * outcome does not depend on how fast the machine is or how the threads are scheduled.
     * <p>
     * Nor does the proof: twenty scans take a millisecond or two, which a closer just started on a loaded runner can spend
     * entirely waiting for a CPU, so the scans ran on a channel nobody closed and the test proved nothing. The reader starts
     * only once the closer has closed the channel, and scans on until the closer has also closed, several times, a channel
     * the reader itself reopened: only the reader reopens it, so every close after the first is one of those.
     */
    // #9621
    @Test
    void aReaderIsNotFailedByOtherThreadsClosingTheChannelItJustReopened() throws Exception {
      final int minScans = 20;
      final long minClosesOfAReopenedChannel = 10;
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong closes = new AtomicLong();
      final CountDownLatch firstClose = new CountDownLatch(1);
      final Thread closer = new Thread(() -> {
        while (!stop.get()) {
          Thread.onSpinWait();
          final FileChannel channel = store.getIndexChannelForTest();
          if (channel.isOpen())
            try {
              channel.close();
              closes.incrementAndGet();
              firstClose.countDown();
            } catch (final IOException ignored) {
              // THE CLOSE OF A CHANNEL THAT IS ALREADY GONE
            }
        }
      }, "issue10223-closer");
      closer.start();

      final long closesDuringScans;
      try {
        // Hang detectors, not latency bounds: a spinning closer closes a channel within microseconds of being scheduled
        assertThat(firstClose.await(60, TimeUnit.SECONDS)).as("the closer closed the channel before the first scan").isTrue();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        for (int scan = 0; scan < minScans || (closes.get() <= minClosesOfAReopenedChannel && System.nanoTime() < deadline); scan++)
          assertThat(countRows(store)).as("scan %d", scan).isEqualTo(BLOCKS * SAMPLES);
        closesDuringScans = closes.get();
      } finally {
        stop.set(true);
        closer.join();
      }

      assertThat(closesDuringScans).as("the channel the reader reopened really was closed under it").isGreaterThan(minClosesOfAReopenedChannel);
      assertThat(store.getReopenCountForTest()).isGreaterThanOrEqualTo(minClosesOfAReopenedChannel);
      // THE STORE IS HEALTHY ONCE THE CLOSES STOP
      assertThat(countRows(store)).isEqualTo(BLOCKS * SAMPLES);
      assertThat(store.getIndexChannelForTest().isOpen()).isTrue();
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    /**
     * The rule #9306 settled for the page file: only an ACCIDENTAL close is recovered. A store closed on purpose must not be
     * resurrected by a late reader.
     */
    // #9621
    @Test
    void aStoreClosedOnPurposeIsNotReopened() throws Exception {
      final TimeSeriesSealedStore closedStore = store;
      closedStore.close();
      store = null;

      assertThatThrownBy(() -> countRows(closedStore)).isInstanceOf(IOException.class);
      assertThat(closedStore.getIndexChannelForTest().isOpen()).isFalse();
    }

    /**
     * Nor is a store whose file is gone (dropped): reopening with "rw" would re-create an empty ghost file.
     */
    // #9621
    @Test
    void aChannelWhoseFileIsGoneIsNotReopenedIntoAGhostFile() throws Exception {
      final File sealed = new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION);
      store.getIndexChannelForTest().close();
      assertThat(sealed.delete()).isTrue();

      assertThatThrownBy(() -> countRows(store)).isInstanceOf(IOException.class);
      assertThatThrownBy(() -> store.getFileSizeBytes()).isInstanceOf(IOException.class);
      assertThat(sealed.exists()).isFalse();
    }
  }

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
  @Nested
  class SealedStoreShortTransfer {

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

    // #9640
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

    // #9640
    @Test
    void anEmptyStoreHeaderSurvivesShortTransfers() throws IOException {
      try (final TimeSeriesSealedStore store = open()) {
        assertThat(store.getBlockCount()).isZero();
      }
      // 27 bytes every version shares plus the owner of version 4 (issue #11274)
      assertThat(new File(BASE_PATH + ".ts.sealed").length()).as("the whole empty header").isEqualTo(35);

      try (final TimeSeriesSealedStore store = open()) {
        assertThat(store.getBlockCount()).isZero();
        assertThat(store.getGlobalMinTimestamp()).isEqualTo(Long.MAX_VALUE);
        assertThat(store.getGlobalMaxTimestamp()).isEqualTo(Long.MIN_VALUE);
      }
    }

    // #9640
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

    // #9640
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

  /**
   * #9770: a rewrite of the sealed file that reuses a fixed temporary path owns that file, so it must truncate it before it
   * writes ({@code RandomAccessFile} in {@code "rw"} mode neither truncates on open nor shrinks on write). Five of the six
   * writers did; {@code truncateBefore}, the retention pass, did not. Worse than a surviving tail, the block writer positions
   * every block at {@code tempFile.length()}, so a leftover of ANY size past the header was entirely preserved and the retained
   * blocks were appended after it: the file swapped over the live store held every block of the abandoned attempt, the blocks
   * retention was told to drop came back on the next open, and the whole bloated file is what HA ships to the followers.
   * <p>
   * The test asserts the RULE and not the cell: for every rewrite entry point, whatever an abandoned rewrite left at the path,
   * the store that is reopened afterwards is the one the same rewrite produces without a leftover.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SealedRewriteLeftoverTemp {
    private static final String ROOT      = "target/databases/Issue9770SealedRewriteLeftoverTempTest";
    private static final long   T0        = 1_700_000_000_000L;
    private static final int    BLOCKS    = 6;
    private static final int    PER_BLOCK = 100;

    private List<ColumnDefinition> columns;

    /** The rewrite entry points that write a temp file and swap it over the live sealed store. */
    enum Rewrite {
      TRUNCATE_BEFORE {
        @Override
        void apply(final TimeSeriesSealedStore store) throws IOException {
          store.truncateBefore(T0 + 4_000);
        }
      },
      TRUNCATE_TO_BLOCK_COUNT {
        @Override
        void apply(final TimeSeriesSealedStore store) throws IOException {
          store.truncateToBlockCount(3);
        }
      },
      DOWNSAMPLE {
        @Override
        void apply(final TimeSeriesSealedStore store) throws IOException {
          store.downsampleBlocks(T0 + 4_000, 10L, 0, List.of(), List.of(1));
        }
      },
      COMPACTION_SWAP {
        @Override
        void apply(final TimeSeriesSealedStore store) throws IOException {
          final long fromTs = T0 + 7_000;
          final List<byte[][]> compressed = new ArrayList<>();
          final List<long[]> meta = new ArrayList<>();
          final List<TimeSeriesSealedStore.BlockStats> stats = new ArrayList<>();
          final long[] timestamps = new long[PER_BLOCK];
          final double[] usage = new double[PER_BLOCK];
          for (int i = 0; i < PER_BLOCK; i++) {
            timestamps[i] = fromTs + i;
            usage[i] = i;
          }
          compressed.add(new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(usage) });
          meta.add(new long[] { fromTs, fromTs + PER_BLOCK - 1, PER_BLOCK });
          stats.add(new TimeSeriesSealedStore.BlockStats(new double[] { Double.NaN, 0 }, new double[] { Double.NaN, PER_BLOCK - 1 },
              new double[] { Double.NaN, 4_950 }, new long[] { 0, PER_BLOCK }));
          final List<TimeSeriesSealedStore.BlockEntry> directory = store.writeTempCompactionFile(compressed, meta, stats, null);
          store.commitTempCompactionFile(directory);
        }
      },
      MERGE {
        @Override
        void apply(final TimeSeriesSealedStore store) throws IOException {
          final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(1_000, 0L, 1);
          assertThat(plan).as("the six small blocks must be mergeable").isNotNull();
          assertThat(store.commitMerge(plan)).isTrue();
        }
      };

      abstract void apply(TimeSeriesSealedStore store) throws IOException;
    }

    /** What an abandoned rewrite can leave behind at the temp path. */
    enum Leftover {
      NONE,
      /** What the store's own Phase 3 writer leaves when the compaction that started it never committed. */
      ABANDONED_PHASE_3,
      /** Garbage longer than anything the rewrite writes. */
      LONG_GARBAGE,
      /** A stub shorter than a header. */
      SHORT_STUB
    }

    private record Outcome(int blocks, long samples, long fileSize, List<Long> timestamps, List<String> problems) {
    }

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

    private static void appendBlock(final TimeSeriesSealedStore store, final long fromTs) throws IOException {
      final long[] timestamps = new long[PER_BLOCK];
      final double[] usage = new double[PER_BLOCK];
      double sum = 0;
      for (int i = 0; i < PER_BLOCK; i++) {
        timestamps[i] = fromTs + i;
        usage[i] = i;
        sum += i;
      }
      store.appendBlock(PER_BLOCK, fromTs, fromTs + PER_BLOCK - 1,
          new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(usage) }, new double[] { Double.NaN, 0 },
          new double[] { Double.NaN, PER_BLOCK - 1 }, new double[] { Double.NaN, sum }, new long[] { 0, PER_BLOCK },
          new String[][] { null, null });
    }

    private void plant(final TimeSeriesSealedStore store, final String basePath, final Leftover leftover) throws IOException {
      final File temp = new File(basePath + ".ts.sealed.tmp");
      switch (leftover) {
      case NONE -> {
      }
      case ABANDONED_PHASE_3 ->
          // the real writer, left uncommitted: exactly the file a compaction that died after Phase 3 leaves behind
          store.writeTempCompactionFile(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null);
      case LONG_GARBAGE -> {
        final byte[] garbage = new byte[8_192];
        Arrays.fill(garbage, (byte) 0x5A);
        Files.write(temp.toPath(), garbage);
      }
      case SHORT_STUB -> Files.write(temp.toPath(), new byte[] { 1, 2, 3 });
      }
      if (leftover != Leftover.NONE) {
        assertThat(temp).as("the leftover must be in place").exists();
        // the merge of small blocks writes to a path of its own: give it the same leftover
        Files.copy(temp.toPath(), new File(basePath + TimeSeriesSealedStore.MERGE_TEMP_SUFFIX).toPath());
      }
    }

    private Outcome run(final Rewrite rewrite, final Leftover leftover) throws IOException {
      final String basePath = ROOT + "/" + rewrite + "_" + leftover + "/sealed";
      new File(basePath).getParentFile().mkdirs();

      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(basePath, columns)) {
        for (int k = 1; k <= BLOCKS; k++)
          appendBlock(store, T0 + k * 1_000L);
        plant(store, basePath, leftover);

        rewrite.apply(store);

        // the rewrite's own temp file is consumed by the swap (the merge has a path of its own; the planted copy at the other one
        // is deleted by the next open)
        assertThat(new File(basePath + (rewrite == Rewrite.MERGE ? TimeSeriesSealedStore.MERGE_TEMP_SUFFIX : ".ts.sealed.tmp")))
            .as("the swap consumes the temp file").doesNotExist();
      }

      // what a restart sees: the directory is rebuilt by scanning the file, whatever its header says
      try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(basePath, columns)) {
        final List<Long> timestamps = new ArrayList<>();
        for (final Object[] row : reopened.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
          timestamps.add(((Number) row[0]).longValue());
        return new Outcome(reopened.getBlockCount(), reopened.getTotalSampleCount(), reopened.getFileSizeBytes(), timestamps,
            reopened.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems().stream().map(Object::toString).toList());
      }
    }

    // #9770
    @ParameterizedTest
    @EnumSource(Rewrite.class)
    void aLeftoverTempFileDoesNotChangeWhatARewriteProduces(final Rewrite rewrite) throws IOException {
      final Outcome control = run(rewrite, Leftover.NONE);
      assertThat(control.problems()).as("the control must be a sound store").isEmpty();
      assertThat(control.blocks()).isPositive();

      for (final Leftover leftover : Leftover.values()) {
        if (leftover == Leftover.NONE)
          continue;
        final Outcome outcome = run(rewrite, leftover);
        assertThat(outcome.problems()).as("%s over a %s leftover", rewrite, leftover).isEmpty();
        assertThat(outcome.blocks()).as("%s over a %s leftover: blocks", rewrite, leftover).isEqualTo(control.blocks());
        assertThat(outcome.samples()).as("%s over a %s leftover: samples", rewrite, leftover).isEqualTo(control.samples());
        assertThat(outcome.fileSize()).as("%s over a %s leftover: bytes on disk", rewrite, leftover).isEqualTo(control.fileSize());
        assertThat(outcome.timestamps()).as("%s over a %s leftover: rows served", rewrite, leftover).isEqualTo(control.timestamps());
      }
    }

    /** The reachable case: a retention pass over the temp file of a compaction that died after Phase 3. */
    // #9770
    @Test
    void retentionOverAnAbandonedCompactionDoesNotResurrectTheBlocksItDropped() throws IOException {
      final String basePath = ROOT + "/retention/sealed";
      new File(basePath).getParentFile().mkdirs();

      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(basePath, columns)) {
        for (int k = 1; k <= BLOCKS; k++)
          appendBlock(store, T0 + k * 1_000L);
        store.writeTempCompactionFile(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null);
        assertThat(new File(basePath + ".ts.sealed.tmp")).exists();

        store.truncateBefore(T0 + 4_000);

        assertThat(store.getBlockCount()).isEqualTo(3);
        assertThat(store.getTotalSampleCount()).isEqualTo(3L * PER_BLOCK);
        // the bytes the HA layer ships to every follower are the whole file: it must not carry the abandoned attempt
        assertThat(store.readWholeSealedFile()).hasSize((int) store.getFileSizeBytes());
      }

      try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(basePath, columns)) {
        assertThat(reopened.getBlockCount()).as("the blocks retention dropped must stay dropped").isEqualTo(3);
        assertThat(reopened.getTotalSampleCount()).isEqualTo(3L * PER_BLOCK);
        assertThat(reopened.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(3 * PER_BLOCK);
        assertThat(reopened.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty();
      }
    }

    /**
     * The signal when two rewrites collided used to be a JDK buffer bounds error out of a header read from an empty file that
     * {@code new RandomAccessFile(path, "rw")} had just re-created. The temp file Phase 4b appends to has to exist: a missing one
     * is refused, by name, and nothing is created in its place.
     */
    // #9770
    @Test
    void appendingToATempFileThatVanishedIsRefusedByNameAndCreatesNothing() throws IOException {
      final String basePath = ROOT + "/vanished/sealed";
      new File(basePath).getParentFile().mkdirs();

      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(basePath, columns)) {
        appendBlock(store, T0 + 1_000L);
        final long[] timestamps = { T0 + 5_000L };
        final List<byte[][]> compressed = new ArrayList<>();
        compressed.add(new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(new double[] { 1 }) });
        final List<long[]> meta = List.of(new long[] { T0 + 5_000L, T0 + 5_000L, 1 });
        final List<TimeSeriesSealedStore.BlockStats> stats = List.of(
            new TimeSeriesSealedStore.BlockStats(new double[] { Double.NaN, 1 }, new double[] { Double.NaN, 1 },
                new double[] { Double.NaN, 1 }, new long[] { 0, 1 }));

        assertThatThrownBy(() -> store.appendBlocksToTempFile(compressed, meta, stats, null, new ArrayList<>()))
            .isInstanceOf(IOException.class).hasMessageContaining("ts.sealed.tmp").hasMessageContaining("another rewrite");
        assertThat(new File(basePath + ".ts.sealed.tmp")).as("a refused append must not create the file it wanted").doesNotExist();
      }
    }
  }

  /**
   * Regression tests for #9771.
   */
  @Nested
  @Timeout(180)
  class SealedRewritesExcludeEachOther extends TestHelper {
    private static final String TYPE_NAME = "Issue9771Ts";
    private static final long   T0        = 1_700_000_000_000L;
    private static final long   STEP_MS   = 1_000L;
    private static final int    PER_BLOCK = 100;
    /** The sealed blocks are 100 samples a second apart, one per pass, 200 s apart. */
    private static final long   BLOCK_SPAN_MS = 200_000L;

    private enum SecondRewrite {
      RETENTION, DOWNSAMPLING, MERGE
    }

    @AfterEach
    void clearHooks() {
      TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
      TimeSeriesShard.TEST_PRE_PHASE4B_HOOK = null;
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
    }

    private void createType() {
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    }

    /** One pass worth of samples, starting at the pass's own slot so that passes never overlap. */
    private void feed(final TimeSeriesEngine engine, final int pass) throws Exception {
      final long[] timestamps = new long[PER_BLOCK];
      final Object[] tags = new Object[PER_BLOCK];
      final Object[] values = new Object[PER_BLOCK];
      for (int i = 0; i < PER_BLOCK; i++) {
        timestamps[i] = T0 + pass * BLOCK_SPAN_MS + i * STEP_MS;
        tags[i] = "s" + (i % 3);
        values[i] = (double) (pass * PER_BLOCK + i);
      }
      engine.appendSamples(timestamps, tags, values);
    }

    /** Six sealed blocks, then a seventh batch left in the mutable bucket for the compaction under test to seal. */
    private void sealSixBlocksAndLeaveOneBatch(final TimeSeriesEngine engine) throws Exception {
      for (int pass = 0; pass < 6; pass++) {
        feed(engine, pass);
        engine.compactAll();
      }
      feed(engine, 6);
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(6);
    }

    private static void runOn(final DatabaseInternal db, final Thread[] holder, final AtomicReference<Throwable> failure,
        final ThrowingRunnable work) {
      final Thread thread = new Thread(() -> {
        DatabaseContext.INSTANCE.init(db);
        try {
          work.run();
        } catch (final Throwable t) {
          failure.set(t);
        } finally {
          DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
        }
      }, "Issue9771-second-rewrite");
      holder[0] = thread;
      thread.start();
    }

    /**
     * Waits until the thread has finished or is queued on the shard's rewrite mutex, and says which. A thread that is merely parked
     * on something else would not do: the claim is that the second rewrite reached the mutex and waits there. The bound is a hang
     * detector, not a latency claim.
     */
    private static boolean awaitQueuedOnTheMutex(final TimeSeriesShard shard, final Thread thread) throws InterruptedException {
      final long deadline = System.nanoTime() + 20_000_000_000L;
      while (thread.isAlive() && System.nanoTime() < deadline) {
        if (shard.hasRewriteWaiting())
          return true;
        Thread.sleep(5);
      }
      return false;
    }

    private record Snapshot(int blocks, long samples, int rows, List<String> problems) {
    }

    private Snapshot snapshot() throws Exception {
      final TimeSeriesEngine engine = engine();
      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final List<String> problems = new ArrayList<>();
      sealed.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems().forEach(p -> problems.add(p.toString()));
      return new Snapshot(sealed.getBlockCount(), sealed.getTotalSampleCount(),
          engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).size(), problems);
    }

    // #9771
    @ParameterizedTest
    @EnumSource(SecondRewrite.class)
    void aRewriteStartedInsideACompactionWaitsForItAndLeavesASoundStore(final SecondRewrite second) throws Exception {
      createType();
      final TimeSeriesEngine engine = engine();
      sealSixBlocksAndLeaveOneBatch(engine);

      final Thread[] holder = new Thread[1];
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean queuedOnTheMutex = new AtomicBoolean();
      final DatabaseInternal db = (DatabaseInternal) database;
      TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
        TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
        runOn(db, holder, failure, () -> {
          switch (second) {
          case RETENTION -> engine.applyRetention(T0 + 3 * BLOCK_SPAN_MS);
          case DOWNSAMPLING -> engine.applyDownsampling(List.of(new DownsamplingTier(1L, 10_000L, 0L)), T0 + 100 * BLOCK_SPAN_MS);
          case MERGE -> engine.mergeSmallBlocks();
          }
        });
        try {
          queuedOnTheMutex.set(awaitQueuedOnTheMutex(engine.getShard(0), holder[0]));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      engine.compactAll();

      assertThat(holder[0]).as("the hook must have started the second rewrite").isNotNull();
      assertThat(queuedOnTheMutex.get()).as("the second rewrite must wait for the compaction on the shard's rewrite mutex").isTrue();
      holder[0].join(60_000);
      assertThat(holder[0].isAlive()).as("the second rewrite must finish once the compaction is done").isFalse();
      assertThat(failure.get()).as("the second rewrite must not fail").isNull();

      final Snapshot live = snapshot();
      assertThat(live.problems()).as("the live store must agree with its own file").isEmpty();
      switch (second) {
      case RETENTION -> {
        // 3 blocks retired, the compaction's own block added: the order the two ran in, not the one they interleaved in
        assertThat(live.blocks()).isEqualTo(4);
        assertThat(live.samples()).isEqualTo(4L * PER_BLOCK);
        assertThat(live.rows()).isEqualTo(4 * PER_BLOCK);
      }
      case DOWNSAMPLING -> assertThat(live.rows()).as("seven batches were sealed, then reduced").isLessThan(7 * PER_BLOCK);
      case MERGE -> assertThat(live.rows()).isEqualTo(7 * PER_BLOCK);
      }

      // the retention must not be undone by what the compaction installs, or by a restart
      reopenDatabase();
      final Snapshot reopened = snapshot();
      assertThat(reopened.problems()).isEmpty();
      assertThat(reopened.blocks()).as("a restart sees the store the live node was serving").isEqualTo(live.blocks());
      assertThat(reopened.samples()).isEqualTo(live.samples());
      assertThat(reopened.rows()).isEqualTo(live.rows());
    }

    /** The same rule for two retention passes: the second must not start writing while the first still owns the temp path. */
    // #9771
    @Test
    void concurrentMaintenancePassesLeaveASoundStore() throws Exception {
      createType();
      final TimeSeriesEngine engine = engine();
      for (int pass = 0; pass < 12; pass++) {
        feed(engine, pass);
        engine.compactAll();
      }

      final DatabaseInternal db = (DatabaseInternal) database;
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final List<Thread> threads = new ArrayList<>();
      final ThrowingRunnable[] works = {
          () -> engine.applyRetention(T0 + 2 * BLOCK_SPAN_MS),
          () -> engine.applyRetention(T0 + 4 * BLOCK_SPAN_MS),
          () -> engine.applyDownsampling(List.of(new DownsamplingTier(1L, 20_000L, 0L)), T0 + 100 * BLOCK_SPAN_MS),
          engine::mergeSmallBlocks,
          engine::compactAll };
      for (int round = 0; round < 20; round++)
        for (final ThrowingRunnable work : works) {
          final Thread[] holder = new Thread[1];
          runOn(db, holder, failure, work);
          threads.add(holder[0]);
        }
      for (final Thread thread : threads) {
        thread.join(120_000);
        assertThat(thread.isAlive()).isFalse();
      }
      assertThat(failure.get()).isNull();

      final Snapshot live = snapshot();
      assertThat(live.problems()).isEmpty();
      reopenDatabase();
      final Snapshot reopened = snapshot();
      assertThat(reopened.problems()).isEmpty();
      assertThat(reopened.blocks()).isEqualTo(live.blocks());
      assertThat(reopened.samples()).isEqualTo(live.samples());
      assertThat(reopened.rows()).isEqualTo(live.rows());
    }

    /**
     * The window that produced the leftover of #9770: a compaction that fails between writing its temp file and swapping it
     * (Phase 4a, Phase 4b) used to leave the file in place, with the crash-recovery flag set, for the next writer to find.
     */
    // #9771
    @Test
    void aCompactionFailingInPhase4bLeavesNoTempFileAndNoFlag() throws Exception {
      createType();
      final TimeSeriesEngine engine = engine();
      sealSixBlocksAndLeaveOneBatch(engine);
      final TimeSeriesShard shard = engine.getShard(0);
      final File temp = new File(getDatabasePath(), shard.getSealedStore().getSealedFileName() + ".tmp");

      TimeSeriesShard.TEST_PRE_PHASE4B_HOOK = () -> {
        assertThat(temp).as("the temp file exists once Phase 3 is done").exists();
        throw new IllegalStateException("injected Phase 4b failure");
      };
      assertThatThrownBy(engine::compactAll).isInstanceOf(IllegalStateException.class).hasMessageContaining("injected Phase 4b");
      TimeSeriesShard.TEST_PRE_PHASE4B_HOOK = null;

      assertThat(temp).as("a failed compaction leaves no temp file behind").doesNotExist();
      assertThat(shard.getMutableBucket().isCompactionInProgress()).as("and no crash-recovery flag").isFalse();

      // the next rewrite starts from a clean slate and the data is all still there
      engine.applyRetention(T0 + 3 * BLOCK_SPAN_MS);
      assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(3);
      engine.compactAll();
      assertThat(snapshot().problems()).isEmpty();
      assertThat(snapshot().rows()).isEqualTo(4 * PER_BLOCK);
    }

    /**
     * A read lock cannot be upgraded to the write lock a rewrite takes, so a rewrite started by a thread that holds the compaction
     * read lock would wait for itself for ever. It is refused up front, before the rewrite mutex is taken. The timeout is a hang
     * detector for the failure this guards against, not a latency claim.
     */
    // #9771
    @Test
    @Timeout(60)
    void aRewriteStartedWhileHoldingTheCompactionReadLockIsRefusedInsteadOfDeadlocking() throws Exception {
      createType();
      final TimeSeriesEngine engine = engine();
      final TimeSeriesShard shard = engine.getShard(0);

      shard.getCompactionLock().readLock().lock();
      try {
        assertThatThrownBy(() -> engine.applyRetention(T0)).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("compaction read lock");
      } finally {
        shard.getCompactionLock().readLock().unlock();
      }

      // and nothing was left held: the same call works once the lock is given back
      engine.applyRetention(T0);
      assertThat(shard.hasRewriteWaiting()).isFalse();
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
      void run() throws Exception;
    }
  }

  /**
   * Issue #11274: a sealed file recorded no owner, so the engine of a type adopted whatever file sat at the path it opens - through the
   * schema load above all, which no creation-time check reaches. A type now draws an owner id at creation, keeps it in the schema and in
   * the header of every sealed file of its shards, and refuses at open a file written for another type, another shard or another
   * database, leaving the type registered without an engine and the foreign file untouched.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SealedFileOwner extends TestHelper {

    // #11274
    @Test
    void aSealedFileOfAnotherTypeAtThePathIsNotAdoptedAtOpen() throws IOException {
      createSealedType("A", 1.0);
      createSealedType("B", 2.0);
      final LocalTimeSeriesType a = (LocalTimeSeriesType) database.getSchema().getType("A");
      final LocalTimeSeriesType b = (LocalTimeSeriesType) database.getSchema().getType("B");
      assertThat(a.getSealedOwner()).isNotEqualTo(TimeSeriesSealedStore.NO_OWNER).isNotEqualTo(b.getSealedOwner());
      assertThat(b.getSealedOwner()).isNotEqualTo(TimeSeriesSealedStore.NO_OWNER);
      assertThat(values("B")).containsExactly(2.0, 2.0, 2.0);

      final Path directory = Path.of(database.getDatabasePath());
      final Path aFile = directory.resolve(TimeSeriesSealedStore.sealedFileNameFor("A", 0));
      final Path bFile = directory.resolve(TimeSeriesSealedStore.sealedFileNameFor("B", 0));
      assertThat(aFile).isRegularFile();
      assertThat(bFile).isRegularFile();
      final Path bOriginal = directory.resolve("B_shard_0.original");

      // the shape of a file restored by hand: A's sealed file copied over B's, the schema left as it is
      database.close();
      Files.move(bFile, bOriginal);
      Files.copy(aFile, bFile);
      final byte[] foreign = Files.readAllBytes(bFile);
      database = factory.open();

      final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("B");
      assertThat(reopened.isEngineAvailable()).as("the engine of B must not adopt A's samples").isFalse();
      assertThat(reopened.getEngineUnavailableReason()).contains("is not read").contains("not for this shard");
      final Throwable refused = catchThrowable(() -> values("B"));
      assertThat(refused).isNotNull();
      assertThat(refused.getMessage()).contains("has no storage engine available");
      // A is untouched, and so is the foreign file: nothing was read from it or written to it
      assertThat(values("A")).containsExactly(1.0, 1.0, 1.0);
      database.close();
      assertThat(Files.readAllBytes(bFile)).isEqualTo(foreign);

      // B's own file back: B reads its own samples again
      Files.move(bOriginal, bFile, StandardCopyOption.REPLACE_EXISTING);
      database = factory.open();
      assertThat(((LocalTimeSeriesType) database.getSchema().getType("B")).isEngineAvailable()).isTrue();
      assertThat(values("B")).containsExactly(2.0, 2.0, 2.0);
    }

    // #11274
    @Test
    void theOwnerIsKeptInTheSchemaAndAcrossARename() {
      createSealedType("R", 3.0);
      final long owner = ((LocalTimeSeriesType) database.getSchema().getType("R")).getSealedOwner();
      assertThat(owner).isNotEqualTo(TimeSeriesSealedStore.NO_OWNER);

      database.command("sql", "ALTER TYPE R NAME R2");
      assertThat(((LocalTimeSeriesType) database.getSchema().getType("R2")).getSealedOwner()).isEqualTo(owner);
      reopenDatabase();

      final LocalTimeSeriesType renamed = (LocalTimeSeriesType) database.getSchema().getType("R2");
      assertThat(renamed.getSealedOwner()).isEqualTo(owner);
      assertThat(renamed.isEngineAvailable()).isTrue();
      assertThat(values("R2")).containsExactly(3.0, 3.0, 3.0);

      // and a further rewrite of the whole file keeps it: the samples are still read after a compaction and a reopen
      database.command("sql", "INSERT INTO R2 SET ts = 10000, value = 4.0");
      database.command("sql", "COMPACT TIMESERIES TYPE R2");
      reopenDatabase();
      assertThat(values("R2")).containsExactly(3.0, 3.0, 3.0, 4.0);
    }

    /**
     * A schema written before the owner existed has no {@code sealedOwner}: the type opens with no owner and reads its files as
     * before, whatever they record, and saving the schema again does not invent one.
     */
    // #11274
    @Test
    void aSchemaWithoutTheOwnerOpensItsTypeAsBefore() throws IOException {
      createSealedType("L", 5.0);
      final Path schemaFile = Path.of(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      database.close();

      final JSONObject schema = new JSONObject(Files.readString(schemaFile));
      final JSONObject type = schema.getJSONObject("types").getJSONObject("L");
      assertThat(type.has("sealedOwner")).isTrue();
      type.remove("sealedOwner");
      Files.writeString(schemaFile, schema.toString());

      database = factory.open();
      final LocalTimeSeriesType legacy = (LocalTimeSeriesType) database.getSchema().getType("L");
      assertThat(legacy.getSealedOwner()).isEqualTo(TimeSeriesSealedStore.NO_OWNER);
      assertThat(legacy.isEngineAvailable()).isTrue();
      assertThat(values("L")).containsExactly(5.0, 5.0, 5.0);

      // a schema change saves the schema: still no owner, and the samples are still read after a reopen
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      reopenDatabase();
      final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("L");
      assertThat(reopened.getSealedOwner()).isEqualTo(TimeSeriesSealedStore.NO_OWNER);
      assertThat(values("L")).containsExactly(5.0, 5.0, 5.0);
      assertThat(new JSONObject(Files.readString(schemaFile)).getJSONObject("types").getJSONObject("L").has("sealedOwner")).isFalse();
    }

    // #11274
    @Test
    void aStoreRefusesTheFileAndTheImageOfAnotherOwner() throws IOException {
      final String directory = database.getDatabasePath() + File.separator + "raw11274";
      new File(directory).mkdirs();
      final String path = directory + File.separator + "store";
      final List<ColumnDefinition> columns = List.of(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
      final long first = TimeSeriesSealedStore.newOwner();
      final long second = first + 1_000;

      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns, 1 << 20, first)) {
        appendBlock(store, 1_000L, 1.0);
        store.flushHeader();
      }
      try (final RandomAccessFile raf = new RandomAccessFile(path + TimeSeriesSealedStore.FILE_EXTENSION, "r")) {
        raf.seek(4);
        assertThat(raf.readByte()).isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);
        raf.seek(27);
        assertThat(raf.readLong()).as("the owner, after the 27 bytes every version shares").isEqualTo(first);
      }

      // another owner refuses it, at open
      final Throwable refused = catchThrowable(() -> new TimeSeriesSealedStore(path, columns, 1 << 20, second).close());
      assertThat(refused).isInstanceOf(IOException.class).hasMessageContaining(Long.toHexString(first))
          .hasMessageContaining(Long.toHexString(second));
      // its own owner, and a store told none, read it
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns, 1 << 20, first)) {
        assertThat(store.getBlockCount()).isEqualTo(1);
      }
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        assertThat(store.getBlockCount()).isEqualTo(1);
      }

      // an image written for another owner is refused before it replaces the file in place, which stays readable
      final String otherPath = directory + File.separator + "other";
      try (final TimeSeriesSealedStore other = new TimeSeriesSealedStore(otherPath, columns, 1 << 20, second)) {
        appendBlock(other, 5_000L, 9.0);
        appendBlock(other, 6_000L, 9.0);
        other.flushHeader();
      }
      final File image = new File(directory, "image.incoming");
      Files.copy(Path.of(otherPath + TimeSeriesSealedStore.FILE_EXTENSION), image.toPath());
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns, 1 << 20, first)) {
        final Throwable install = catchThrowable(() -> store.installSealedFile(image));
        assertThat(install).isInstanceOf(IOException.class).hasMessageContaining("is not read");
        assertThat(image).as("the image is left to the caller").exists();
        assertThat(store.getBlockCount()).isEqualTo(1);
        assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      }

      // the image itself is whole: a store told no owner reads it
      try (final TimeSeriesSealedStore anyOwner = new TimeSeriesSealedStore(otherPath, columns)) {
        assertThat(anyOwner.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
        assertThat(anyOwner.getBlockCount()).isEqualTo(2);
      }
      FileUtils.deleteRecursively(new File(directory));
    }

    // #11274
    @Test
    void aLegacyFileHeaderIsRewrittenInPlaceInItsOwnLayout() throws IOException {
      final String directory = database.getDatabasePath() + File.separator + "legacy11274";
      new File(directory).mkdirs();
      final String path = directory + File.separator + "store";
      final List<ColumnDefinition> columns = List.of(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

      // a version-3 file: the 27-byte header, its first block right after it
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        appendBlock(store, 1_000L, 1.0);
        store.flushHeader();
      }
      downgradeToVersion3(path + TimeSeriesSealedStore.FILE_EXTENSION);

      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        assertThat(store.getBlockCount()).isEqualTo(1);
        appendBlock(store, 2_000L, 2.0);
        store.flushHeader();
        assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      }
      try (final RandomAccessFile raf = new RandomAccessFile(path + TimeSeriesSealedStore.FILE_EXTENSION, "r")) {
        raf.seek(4);
        assertThat(raf.readByte()).as("an in-place header rewrite keeps the 27-byte layout").isEqualTo((byte) 3);
      }
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        assertThat(store.getBlockCount()).isEqualTo(2);
        // a rewrite of the whole file writes the current layout
        store.truncateBefore(1_500L);
        assertThat(store.getBlockCount()).isEqualTo(1);
      }
      try (final RandomAccessFile raf = new RandomAccessFile(path + TimeSeriesSealedStore.FILE_EXTENSION, "r")) {
        raf.seek(4);
        assertThat(raf.readByte()).isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);
      }
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        assertThat(store.getBlockCount()).isEqualTo(1);
        assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      }

      // a store told an owner refuses a file older than owners: its type wrote every file of the shard with one
      final Throwable refused = catchThrowable(() -> {
        downgradeToVersion3(path + TimeSeriesSealedStore.FILE_EXTENSION);
        new TimeSeriesSealedStore(path, columns, 1 << 20, TimeSeriesSealedStore.newOwner()).close();
      });
      assertThat(refused).isInstanceOf(IOException.class).hasMessageContaining("records no owner");
      FileUtils.deleteRecursively(new File(directory));
    }

    /** Rewrites a version-4 file as the version-3 file holding the same blocks: the owner taken out, version byte 3. */
    private static void downgradeToVersion3(final String file) throws IOException {
      final byte[] bytes = Files.readAllBytes(Path.of(file));
      final byte[] legacy = new byte[bytes.length - 8];
      System.arraycopy(bytes, 0, legacy, 0, 27);
      System.arraycopy(bytes, 35, legacy, 27, bytes.length - 35);
      legacy[4] = 3;
      Files.write(Path.of(file), legacy);
    }

    private static void appendBlock(final TimeSeriesSealedStore store, final long ts, final double value) throws IOException {
      store.appendBlock(1, ts, ts, new byte[][] { DeltaOfDeltaCodec.encode(new long[] { ts }), GorillaXORCodec.encode(new double[] { value }) },
          new double[] { Double.NaN, value }, new double[] { Double.NaN, value }, new double[] { Double.NaN, value }, new long[] { 0, 1 }, null);
    }

    private void createSealedType(final String name, final double value) {
      database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO " + name + " SET ts = ?, value = ?", 1000L * (i + 1), value);
      database.command("sql", "COMPACT TIMESERIES TYPE " + name);
    }

    private List<Double> values(final String type) {
      final List<Double> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT value FROM " + type + " ORDER BY ts")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(((Number) row.getProperty("value")).doubleValue());
        }
      }
      return values;
    }
  }

  /**
   * Issue #10925: CREATE TIMESERIES TYPE adopted any sealed file already sitting at its name, so a type created one statement ago
   * answered rows that belonged to a type dropped before it. The create now refuses it, as the rename already did.
   */
  @Nested
  class CreateRefusesStraySealedFile extends TestHelper {

    private static final String CREATE = "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1";

    // #10925
    @Test
    void createRefusesASealedFileLeftAtItsName() throws Exception {
      final File sealed = new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0));
      Files.write(sealed.toPath(), new byte[] { 1, 2, 3 });

      assertThatThrownBy(() -> database.command("sql", CREATE)).hasMessageContaining(sealed.getName());

      assertThat(database.getSchema().existsType("Ticker")).isFalse();
      assertThat(sealed).as("the stray file is left untouched").exists();
    }

    // #10925
    @Test
    void createSucceedsOnceTheStrayFileIsGone() throws Exception {
      final File sealed = new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0));
      Files.write(sealed.toPath(), new byte[] { 1, 2, 3 });
      assertThatThrownBy(() -> database.command("sql", CREATE)).isInstanceOf(SchemaException.class)
          .hasMessageContaining(sealed.getName());

      assertThat(sealed.delete()).isTrue();
      database.command("sql", CREATE);

      assertThat(database.getSchema().existsType("Ticker")).isTrue();
    }

    // #10925
    @Test
    void dropThenRecreateIsNotRefused() throws Exception {
      database.command("sql", CREATE);
      database.command("sql", "DROP TYPE Ticker");
      assertThat(new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0))).doesNotExist();

      database.command("sql", CREATE);

      assertThat(database.getSchema().existsType("Ticker")).isTrue();
    }

    // #10925
    @Test
    void ifNotExistsOnAnExistingTypeIsANoOp() throws Exception {
      database.command("sql", CREATE);

      database.command("sql", CREATE.replace("Ticker", "Ticker IF NOT EXISTS"));

      assertThat(database.getSchema().existsType("Ticker")).isTrue();
    }

    // #10925
    @Test
    void embeddedBuilderRefusesToo() throws Exception {
      final File sealed = new File(getDatabasePath(), TimeSeriesSealedStore.sealedFileNameFor("Ticker", 0));
      Files.write(sealed.toPath(), new byte[] { 1, 2, 3 });

      assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("Ticker").withTimestamp("ts")
          .withTag("name", Type.STRING).withField("f1", Type.DOUBLE).withShards(1).create())
          .isInstanceOf(SchemaException.class).hasMessageContaining(sealed.getName());
      assertThat(database.getSchema().existsType("Ticker")).isFalse();
    }
  }

  /**
   * Issue #9886: a schema reload rebuilds every TimeSeries type with a fresh engine, built on the SAME registered files
   * (the shards' mutable buckets and the type's tag dictionary), and then closed the engine it superseded - which closed
   * those shared files. The next write to the type failed with "Cannot write page N because the file
   * '..._tags...tstd' is closed". On an HA follower every applied schema entry reloads the schema, so the first
   * TimeSeries write after any DDL quarantined the database and forced a snapshot resync.
   */
  @Nested
  class SchemaReloadKeepsFilesOpen extends TestHelper {

    private static final String TYPE = "SensorReading";

    @Override
    protected void beginTest() {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE) SHARDS 1");
      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
    }

    // #9886
    @Test
    void aWriteAfterAFullSchemaLoadReachesTheTimeSeriesFiles() throws Exception {
      final LocalSchema schema = database.getSchema().getEmbedded();
      final TimeSeriesEngine before = engine();

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      assertThat(engine()).as("the reload must have built a new engine, or this test exercises nothing").isNotSameAs(before);
      assertFilesOpenAndWritable();
    }

    // #9886
    @Test
    void aWriteAfterAnIncrementalSchemaLoadReachesTheTimeSeriesFiles() throws Exception {
      final LocalSchema schema = database.getSchema().getEmbedded();
      final TimeSeriesEngine before = engine();

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, null, Set.of())).isTrue();

      assertThat(engine()).as("the reload must have built a new engine, or this test exercises nothing").isNotSameAs(before);
      assertFilesOpenAndWritable();
    }

    // #9886
    @Test
    void aWriteAfterCreatingAContinuousAggregateAndReloadingReachesTheTimeSeriesFiles() throws Exception {
      database.command("sql", "CREATE CONTINUOUS AGGREGATE hourly_temps AS SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, "
          + "avg(temperature) AS avg_temp FROM " + TYPE + " GROUP BY sensor_id, hour");

      database.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, true);

      assertFilesOpenAndWritable();
    }

    /** Closing the database still closes the TimeSeries files: the reload fix must not leak them. */
    // #9886
    @Test
    void closingTheDatabaseStillClosesTheTimeSeriesFiles() throws Exception {
      database.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, true);
      final PaginatedComponentFile dictionaryFile = engine().getTagDictionary().getComponentFile();
      final PaginatedComponentFile bucketFile = engine().getShard(0).getMutableBucket().getComponentFile();

      database.close();

      assertThat(dictionaryFile.isOpen()).isFalse();
      assertThat(bucketFile.isOpen()).isFalse();
    }

    private void assertFilesOpenAndWritable() {
      final TimeSeriesEngine engine = engine();
      assertThat(engine.getTagDictionary().getComponentFile().isOpen()).as("tag dictionary file").isTrue();
      assertThat(engine.getShard(0).getMutableBucket().getComponentFile().isOpen())
          .as("mutable bucket file").isTrue();

      // A tag value never seen before, so the write reaches the dictionary as well as the bucket
      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 2000, sensor_id = 'B', temperature = 23.5"));

      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
      }

      // Both tag values resolve through the replacement engine's dictionary: the one interned before the reload and the
      // one interned after it, so the two engines cannot have handed out diverging ids for them
      final List<String> tags = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT sensor_id FROM " + TYPE + " ORDER BY ts")) {
        while (rs.hasNext())
          tags.add(rs.next().getProperty("sensor_id"));
      }
      assertThat(tags).containsExactly("A", "B");
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
    }
  }

  /**
   * Issue #9309: an integral field wider than 60 bits (an epoch-nanosecond instant, a snowflake id, a 64-bit hash) was
   * accepted by the mutable layer and then made every compaction of the type fail, permanently.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WideIntegerCompaction extends TestHelper {

    private static final long BASE_TS = 1_700_000_000_000L;

    // #9309
    @Test
    void wideValuesSurviveCompaction() throws Exception {
      final long[] wide = { 1_700_000_123_456_789_000L, 1_500_000_000_000_000_000L, (1L << 59), -(1L << 59) - 1, Long.MAX_VALUE,
          Long.MIN_VALUE };
      for (final String declared : new String[] { "LONG", "DATETIME_NANOS" }) {
        final String typeName = "Wide_" + declared;
        database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared + ") SHARDS 1");
        final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();

        final long[] timestamps = new long[wide.length];
        final Object[] values = new Object[wide.length];
        for (int i = 0; i < wide.length; i++) {
          timestamps[i] = BASE_TS + i;
          values[i] = wide[i];
        }
        engine.appendSamples(timestamps, values);

        // Compacting twice: the failure used to repeat because the poisoned sample stayed in the mutable layer
        engine.compactAll();
        engine.compactAll();

        final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
        assertThat(rows).as(declared).hasSize(wide.length);
        for (int i = 0; i < wide.length; i++)
          assertThat(rows.get(i)[1]).as("%s row %d", declared, i).isEqualTo(wide[i]);
      }
    }

    // #9309
    @Test
    void oneWideValueDoesNotStopTheTypeSealing() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Mixed TIMESTAMP ts FIELDS (v LONG) SHARDS 1");
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Mixed")).getEngine();

      final int n = 1000;
      final long[] timestamps = new long[n];
      final Object[] values = new Object[n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = BASE_TS + i;
        values[i] = i == 500 ? 1L << 60 : (long) i;
      }
      engine.appendSamples(timestamps, values);
      engine.compactAll();

      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(n);
      for (int i = 0; i < n; i++)
        assertThat(rows.get(i)[1]).as("row %d", i).isEqualTo(values[i]);
    }
  }
}
