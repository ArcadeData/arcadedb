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
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10372: the fix for #10223 moved {@code readBytes} off the shared channel when it recovers, but the three other
 * entry points that run under the directory READ lock - {@code getFileSizeBytes}, {@code readWholeSealedFile} and a
 * reporting {@code checkIntegrity} - still asked the shared descriptor for the file length and retried on it. Another
 * thread closing the channel between a reopen and the {@code length()} failed them with {@code IOException("Stream
 * Closed")} on a file that was intact, and those are the HA ship of a sealed blob and its size pre-check.
 * <p>
 * The decoded-column cache is disabled (budget 0) so every read below really goes to the file.
 */
class Issue10372SealedStoreReadLockEntryPointsTest {

  private static final String ROOT      = "target/databases/Issue10372SealedStoreReadLockEntryPointsTest";
  private static final String BASE_PATH = ROOT + "/sealed";
  private static final int    BLOCKS    = 4;
  private static final int    SAMPLES   = 64;
  private static final long   BASE_TS   = 1_700_000_000_000L;
  private static final int    CALLS     = 200;
  private static final int    CALLERS   = 3;

  private List<ColumnDefinition> columns;
  private TimeSeriesSealedStore  store;

  @BeforeEach
  void setUp() throws IOException {
    FileUtils.deleteRecursively(new File(ROOT));
    new File(ROOT).mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("usage", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    store = new TimeSeriesSealedStore(BASE_PATH, columns, 0L);
    for (int b = 0; b < BLOCKS; b++)
      appendBlock(store, BASE_TS + (long) b * SAMPLES * 1_000L);
    store.flushHeader();
  }

  @AfterEach
  void tearDown() {
    if (store != null)
      try {
        store.close();
      } catch (final IOException ignored) {
        // ALREADY CLOSED BY THE TEST
      }
    FileUtils.deleteRecursively(new File(ROOT));
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

  @FunctionalInterface
  private interface EntryPoint {
    void call() throws Exception;
  }

  /**
   * The issue's one assertion: the READ-lock entry points run against the repeated closer the #10223 storm test uses (a
   * thread closing the shared channel again the moment it is open), and every call must return the right value. The
   * closer is the worst case of other threads' interrupts landing on the shared channel: nothing that depends on the
   * shared channel survives it, whatever the speed of the machine.
   * <p>
   * The WRITE-lock entry points ({@code appendBlock}, {@code flushHeader}) are not in the storm: under the write lock no
   * other thread does I/O on the channel, so only the writer's own interrupt can close it, and its retry clears the flag
   * first. {@code Issue9621SealedStoreChannelReopenTest} covers them against a close.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // A HANG DETECTOR, NOT A LATENCY BOUND: THE CLOSER AND CALLERS SPIN
  void everyReadLockEntryPointSurvivesOtherThreadsClosingTheSharedChannel() throws Exception {
    final File sealed = new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION);
    final long expectedLength = sealed.length();
    final byte[] expectedBytes = Files.readAllBytes(sealed.toPath());

    assertSurvivesTheStorm("scanRange", () ->
        assertThat(store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(BLOCKS * SAMPLES));
    assertSurvivesTheStorm("getFileSizeBytes", () -> assertThat(store.getFileSizeBytes()).isEqualTo(expectedLength));
    assertSurvivesTheStorm("readWholeSealedFile", () -> assertThat(store.readWholeSealedFile()).isEqualTo(expectedBytes));
    assertSurvivesTheStorm("checkIntegrity(REPORT_ONLY)",
        () -> assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems()).isEmpty());
    assertSurvivesTheStorm("checkIntegrity(deep)",
        () -> assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty());

    // THE STORE IS HEALTHY ONCE THE CLOSES STOP
    assertThat(store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(BLOCKS * SAMPLES);
    assertThat(store.getFileSizeBytes()).isEqualTo(expectedLength);
    assertThat(store.getIndexChannelForTest().isOpen()).isTrue();
    assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
  }

  private void assertSurvivesTheStorm(final String name, final EntryPoint entryPoint) throws Exception {
    // A block read repairs the shared channel a previous storm left closed, so this one has an open channel to close
    assertThat(store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(BLOCKS * SAMPLES);
    assertThat(store.getIndexChannelForTest().isOpen()).isTrue();

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong closes = new AtomicLong();
    final Thread closer = new Thread(() -> {
      while (!stop.get()) {
        Thread.onSpinWait();
        final FileChannel channel = store.getIndexChannelForTest();
        if (channel.isOpen())
          try {
            channel.close();
            closes.incrementAndGet();
          } catch (final IOException ignored) {
            // THE CLOSE OF A CHANNEL THAT IS ALREADY GONE
          }
      }
    }, "issue10372-closer");
    closer.start();
    // The callers start only once the shared channel has been closed under them: an entry point that no longer touches it
    // would otherwise never meet a closed channel and prove nothing
    while (closes.get() == 0)
      Thread.onSpinWait();

    // Several callers at once: the retry on the shared channel decided whether to go on by asking whether the channel was
    // still closed, and another caller's reopen answered "no" and made it rethrow the "Stream Closed" it had just caught
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread[] callers = new Thread[CALLERS];
    for (int c = 0; c < CALLERS; c++) {
      callers[c] = new Thread(() -> {
        for (int call = 0; call < CALLS && failure.get() == null; call++) {
          try {
            entryPoint.call();
          } catch (final Throwable t) {
            failure.compareAndSet(null, new AssertionError(name + " failed on call " + call + " after " + closes.get()
                + " closes of the shared channel", t));
          }
        }
      }, "issue10372-caller-" + c);
      callers[c].start();
    }

    try {
      for (final Thread caller : callers)
        caller.join();
    } finally {
      stop.set(true);
      closer.join();
    }
    if (failure.get() != null)
      throw new AssertionError(failure.get().getMessage(), failure.get().getCause());
  }

  /**
   * The guarantee: the sealed store's channel is protected from interrupts the way every page file's is, so an interrupt
   * landing on a reader in the middle of a read no longer closes the channel for every other thread. The reader still gets
   * its rows and keeps its interrupt flag, and the channel is never reopened because it was never closed.
   */
  @Test
  void anInterruptedReaderDoesNotCloseTheSharedChannel() throws Exception {
    final FileChannel before = store.getIndexChannelForTest();
    final AtomicInteger rows = new AtomicInteger(-1);
    final AtomicBoolean flagKept = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread reader = new Thread(() -> {
      Thread.currentThread().interrupt();
      try {
        for (int i = 0; i < 20; i++)
          rows.set(store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null).size());
      } catch (final Throwable t) {
        failure.set(t);
      }
      flagKept.set(Thread.currentThread().isInterrupted());
    });
    reader.start();
    reader.join();

    assertThat(failure.get()).isNull();
    assertThat(rows.get()).isEqualTo(BLOCKS * SAMPLES);
    assertThat(flagKept.get()).isTrue();
    assertThat(before.isOpen()).as("the interrupt did not close the shared channel").isTrue();
    assertThat(store.getIndexChannelForTest()).isSameAs(before);
    assertThat(store.getReopenCountForTest()).isZero();
  }

  /**
   * The WRITE-lock side of the split: a repairing integrity check holds the WRITE lock, so it still recovers a closed
   * channel on the shared one, and {@code retryAfterReopen}'s new requirement of that lock does not refuse it. Closing the
   * store afterwards flushes the header through the same path.
   */
  @Test
  void aWriteLockEntryPointStillRecoversOnTheSharedChannel() throws Exception {
    store.getIndexChannelForTest().close();
    assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.fixOnly()).problems()).isEmpty();
    assertThat(store.getIndexChannelForTest().isOpen()).isTrue();

    store.getIndexChannelForTest().close();
    appendBlock(store, BASE_TS + (long) BLOCKS * SAMPLES * 1_000L);
    store.getIndexChannelForTest().close();
    store.close();

    store = new TimeSeriesSealedStore(BASE_PATH, columns, 0L);
    assertThat(store.getBlockCount()).isEqualTo(BLOCKS + 1);
    assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
  }

  /**
   * A store closed on purpose is not asked for its size even though its file is still there: the refusal comes from the
   * flag, not from the file having gone.
   */
  @Test
  void theLengthOfAStoreClosedOnPurposeIsRefusedWithItsFileIntact() throws Exception {
    final TimeSeriesSealedStore closedStore = store;
    store = null;
    closedStore.close();

    assertThat(new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION).exists()).isTrue();
    assertThatThrownBy(closedStore::getFileSizeBytes).isInstanceOf(IOException.class).hasMessageContaining("closed on purpose");
    assertThatThrownBy(closedStore::readWholeSealedFile).isInstanceOf(IOException.class);
  }

  /**
   * The file length no longer comes from the shared descriptor, so the refusals that the reopen used to make on behalf of
   * a READ-lock caller are made by the length read itself: a store closed on purpose is not asked for its size, and a
   * file that is gone is not reported as an empty one, which the HA ship would otherwise send to every follower.
   */
  @Test
  void theLengthOfAStoreClosedOnPurposeOrWhoseFileIsGoneIsRefused() throws Exception {
    final File sealed = new File(BASE_PATH + TimeSeriesSealedStore.FILE_EXTENSION);
    assertThat(sealed.delete()).isTrue();
    assertThatThrownBy(() -> store.getFileSizeBytes()).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> store.readWholeSealedFile()).isInstanceOf(IOException.class);
    assertThat(sealed.exists()).isFalse();

    final TimeSeriesSealedStore closedStore = store;
    store = null;
    try {
      closedStore.close();
    } catch (final IOException ignored) {
      // THE FILE IS GONE
    }
    assertThatThrownBy(closedStore::getFileSizeBytes).isInstanceOf(IOException.class);
  }
}
