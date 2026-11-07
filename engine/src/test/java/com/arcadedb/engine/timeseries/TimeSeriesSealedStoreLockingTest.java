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
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sealed store and compaction locking: read lock entry points, follower install lock, scan versus compaction lock,
 * layer publishers waiting for readers, drop racing a rewrite and the engine-less type lock. Each nested group keeps
 * its own fixture and carries the issue number it regression-tests.
 */
class TimeSeriesSealedStoreLockingTest {

  /**
   * Issue #10372: the fix for #10223 moved {@code readBytes} off the shared channel when it recovers, but the three other
   * entry points that run under the directory READ lock - {@code getFileSizeBytes}, {@code readWholeSealedFile} and a
   * reporting {@code checkIntegrity} - still asked the shared descriptor for the file length and retried on it. Another
   * thread closing the channel between a reopen and the {@code length()} failed them with {@code IOException("Stream
   * Closed")} on a file that was intact, and those are the HA ship of a sealed blob and its size pre-check.
   * <p>
   * The decoded-column cache is disabled (budget 0) so every read below really goes to the file.
   */
  @Nested
  class ReadLockEntryPoints {

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
     * first. {@code TimeSeriesSealedStoreFileIoTest.SealedStoreChannelReopen} covers them against a close.
     */
    // #10372
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
    // #10372
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
    // #10372
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
    // #10372
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
    // #10372
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

  /**
   * Issue #7337: on an HA follower the sealed store is not rewritten by local compaction - it is REPLACED by the
   * leader's blob - and that path took only the store's own {@code directoryLock}, so
   * {@link TimeSeriesCompactionPause} did not exclude it.
   * <p>
   * The tear that produced is invisible. The leader ships the rewritten sealed bytes and the WAL that clears the
   * mutable bucket in ONE Raft entry, so they are atomic with respect to each other; a backup or a snapshot ship
   * running on that follower can still capture a page image from before the entry (samples in the mutable bucket)
   * and a sealed image from after it (the same samples, now sealed). Restored, every one of those samples appears
   * twice, and nothing reports an error. The {@code compactionInProgress} watermark that repairs a torn LOCAL
   * compaction is never set for a shipped blob, so open-time recovery has nothing to key on either.
   * <p>
   * {@link TimeSeriesSealedInstallLock} is what makes the pause mean on a follower what it already means on a
   * standalone database: the install takes the same shard lock a local compaction takes. These tests hold the two
   * halves of that - a held pause really does block an install, and a held install really does block a pause -
   * plus the resolution and ordering rules the lock has to follow to be safe to take on the Raft apply thread.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7337">issue #7337</a>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FollowerSealedInstallLock extends TestHelper {

    private static final long BASE_TS = 1_700_000_000_000L;
    /** A wait that is EXPECTED to expire: it IS the assertion. A stall can only make it more true. */
    private static final long BLOCKED_PROBE_MS = 2_000L;
    /**
     * Budget for a thread to reach the lock it is expected to park on. Generous on purpose: a wider bound cannot
     * turn a passing run red, it only decides how long a genuinely broken test takes to say so.
     */
    private static final long PARK_WAIT_MS     = 30_000L;

    @Override
    protected void beginTest() {
      database.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    }

    /**
     * The fix, stated directly: a backup holding the compaction pause excludes a follower's sealed-store install,
     * which is exactly what it did not do before. Without the lock the install would run straight through the
     * pause and the archive could pair a pre-clear page image with a post-install sealed one.
     */
    // #7337
    @Test
    void aHeldCompactionPauseBlocksAFollowerSealedInstall() throws Exception {
      ingest(2_000);

      final CountDownLatch installed = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
        final Thread applier = new Thread(() -> {
          try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
              List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 60_000L)) {
            installed.countDown();
          } catch (final Throwable e) {
            failure.set(e);
            installed.countDown();
          }
        }, "issue7337-applier");
        applier.setDaemon(true);
        applier.start();

        // Same reason as its sibling below (CodeRabbit on PR #7474): a bare await() also expires when the thread
        // was never scheduled, so it would keep passing after the lock stopped being taken at all.
        awaitParkedIn(applier, "TimeSeriesSealedInstallLock", "acquire");

        assertThat(installed.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
            .as("a sealed install must not get through a held pause: that is the whole tear")
            .isFalse();

        pause.close();

        assertThat(installed.await(60, TimeUnit.SECONDS))
            .as("and it must proceed the moment the pause is released, or a backup would stall replication")
            .isTrue();
        assertThat(failure.get()).isNull();
        applier.join(60_000);
      }
    }

    /** The other direction: a pause cannot start while an install is in flight, so it never sees a half-applied entry. */
    // #7337
    @Test
    void aHeldSealedInstallBlocksTheCompactionPause() throws Exception {
      ingest(2_000);

      final CountDownLatch paused = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L)) {

        assertThat(lock.getLockedShards()).isEqualTo(1);

        final Thread backup = new Thread(() -> {
          try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 60_000L)) {
            paused.countDown();
          } catch (final Throwable e) {
            failure.set(e);
            paused.countDown();
          }
        }, "issue7337-backup");
        backup.setDaemon(true);
        backup.start();

        awaitParkedIn(backup, "TimeSeriesCompactionPause", "acquire");

        assertThat(paused.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
            .as("a copy of the database must wait for an install in flight rather than photograph it halfway")
            .isFalse();

        lock.close();

        assertThat(paused.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull();
        backup.join(60_000);
      }
    }

    /** A shard the entry does not name is left alone, so an unrelated compaction is not held up by the apply. */
    // #7337
    @Test
    void onlyTheShardsTheEntryNamesAreLocked() {
      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 1)), 30_000L)) {
        assertThat(lock.getLockedShards()).isEqualTo(1);
        // Shard 0 is untouched, so a second lock on it is free.
        try (final TimeSeriesSealedInstallLock other = TimeSeriesSealedInstallLock.acquire(database,
            List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 5_000L)) {
          assertThat(other.getLockedShards()).isEqualTo(1);
        }
      }
    }

    /**
     * A reference that resolves to nothing is skipped rather than refused. The apply path has its own diagnostic
     * for an unknown type, a non-TimeSeries one and an engine that never started; a shard that does not exist
     * cannot be torn, so failing the whole entry here would only turn a logged skip into a halted replica.
     */
    // #7337
    @Test
    void anUnresolvableShardReferenceLocksNothingRatherThanFailing() {
      database.command("sql", "CREATE DOCUMENT TYPE Plain");

      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database, List.of(
          new TimeSeriesSealedInstallLock.ShardRef("NoSuchType", 0),
          new TimeSeriesSealedInstallLock.ShardRef("Plain", 0),
          new TimeSeriesSealedInstallLock.ShardRef("Reading", 99)), 30_000L)) {
        assertThat(lock.getLockedShards()).isZero();
      }

      assertThat(TimeSeriesSealedInstallLock.acquire(database, List.of(), 30_000L).getLockedShards()).isZero();
      assertThat(TimeSeriesSealedInstallLock.acquire(database, null, 30_000L).getLockedShards()).isZero();
    }

    /**
     * The budget is a hang detector: when it expires, everything already taken is released, so a failed apply
     * never leaves a shard locked for the life of the node.
     */
    // #7337
    @Test
    void aTimedOutAcquisitionReleasesWhateverItHadTaken() throws Exception {
      ingest(1_000);

      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
        assertThatThrownBy(() -> TimeSeriesSealedInstallLock.acquire(database, List.of(
            new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
            new TimeSeriesSealedInstallLock.ShardRef("Reading", 1)), 200L))
            .isInstanceOf(TimeoutException.class)
            .hasMessageContaining("Reading");
      }

      // Nothing was left held: the pause is takeable again, and so is the install lock.
      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 5_000L)) {
        assertThat(pause.getPausedShards()).isEqualTo(2);
      }
      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database, List.of(
          new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
          new TimeSeriesSealedInstallLock.ShardRef("Reading", 1)), 5_000L)) {
        assertThat(lock.getLockedShards()).isEqualTo(2);
      }
    }

    /** Closing twice is a no-op, so the explicit early release and the try-with-resources can both fire. */
    // #7337
    @Test
    void closingTheLockTwiceIsANoOp() {
      final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L);
      lock.close();
      lock.close();

      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 5_000L)) {
        assertThat(pause.getPausedShards()).isEqualTo(2);
      }
    }

    /**
     * Blocks until {@code thread} is parked inside {@code className.methodName}, or fails.
     * <p>
     * What turns "it was still blocked after 2s" into an assertion about the LOCK: an elapsed wait proves nothing
     * on its own, because a thread that was never scheduled produces the same elapsed wait as one correctly held.
     * The stack is read rather than only {@link Thread.State}, since a thread can be WAITING for reasons that have
     * nothing to do with the lock under test.
     */
    private static void awaitParkedIn(final Thread thread, final String className, final String methodName)
        throws InterruptedException {
      final long deadline = System.currentTimeMillis() + PARK_WAIT_MS;
      while (System.currentTimeMillis() < deadline) {
        final Thread.State state = thread.getState();
        if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
          for (final StackTraceElement frame : thread.getStackTrace())
            if (frame.getClassName().endsWith(className) && methodName.equals(frame.getMethodName()))
              return;
        if (state == Thread.State.TERMINATED)
          break;
        Thread.sleep(10);
      }
      throw new AssertionError("thread '" + thread.getName() + "' never parked in " + className + "." + methodName
          + " (state=" + thread.getState() + "); the block assertion that follows would have been vacuous");
    }

    // ---------------------------------------------------------------------------------------------------------
    // The total order both acquisitions rest on (code review on PR #7474).
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Both locks must agree on ONE order over the shards, or a backup walking every shard and an apply walking a
     * subset can each hold what the other is waiting for. The agreement used to be "we both walk
     * {@code getSchema().getTypes()}", which is a {@code ConcurrentHashMap}'s bucket order: a resize moves an entry
     * to bucket {@code i} or {@code i + n}, so two types already present can come back in the opposite relative
     * order, and two snapshots taken either side of a {@code CREATE ... TYPE} disagree.
     * <p>
     * The types here are created in the REVERSE of their name order precisely so a walk that inherited the
     * schema's own order would be visible as such.
     */
    // #7337
    @Test
    void everyAcquisitionWalksTheShardsInOneStableOrder() {
      database.command("sql",
          "CREATE TIMESERIES TYPE Zulu TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
      database.command("sql",
          "CREATE TIMESERIES TYPE Alpha TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 3");

      final List<String> order = new ArrayList<>();
      for (final TimeSeriesShardOrder.ShardSlot slot : TimeSeriesShardOrder.of(database))
        order.add(slot.typeName() + "#" + slot.shardIndex());

      assertThat(order)
          .as("by type NAME then ascending shard index, whatever order the schema's own map hands them back in")
          .containsExactly("Alpha#0", "Alpha#1", "Alpha#2", "Reading#0", "Reading#1", "Zulu#0", "Zulu#1");

      // Repeated after a further schema mutation, which is what could resize the map underneath: the relative
      // order of the types already present must not move.
      database.command("sql",
          "CREATE TIMESERIES TYPE Mike TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      final List<String> after = new ArrayList<>();
      for (final TimeSeriesShardOrder.ShardSlot slot : TimeSeriesShardOrder.of(database))
        after.add(slot.typeName() + "#" + slot.shardIndex());

      assertThat(after).containsExactly("Alpha#0", "Alpha#1", "Alpha#2", "Mike#0", "Reading#0", "Reading#1",
          "Zulu#0", "Zulu#1");
      assertThat(after.stream().filter(order::contains).toList())
          .as("every type that was already there keeps its place relative to the others")
          .isEqualTo(order);
    }

    /**
     * The property that order buys, across MORE THAN ONE type - the shape none of the other tests exercise, and
     * the one a disagreement would actually show up in. A pause covering every shard of every type and an install
     * taking a subset of them must be able to block each other and still both complete, rather than each ending up
     * holding what the other waits for.
     */
    // #7337
    @Test
    void aPauseAndAnInstallAcrossSeveralTypesStillMakeProgress() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE Zulu TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
      database.command("sql",
          "CREATE TIMESERIES TYPE Alpha TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");

      final CountDownLatch installed = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
        assertThat(pause.getPausedShards())
            .as("every shard of every type: 2 (Reading) + 2 (Zulu) + 2 (Alpha)")
            .isEqualTo(6);

        // An entry naming shards of two types, in the order the ENTRY happens to list them - which is the reverse
        // of the lock order, so a walk driven by the entry rather than by the shared one would take them backwards.
        final Thread applier = new Thread(() -> {
          try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database, List.of(
              new TimeSeriesSealedInstallLock.ShardRef("Zulu", 1),
              new TimeSeriesSealedInstallLock.ShardRef("Alpha", 0)), 60_000L)) {
            assertThat(lock.getLockedShards()).isEqualTo(2);
          } catch (final Throwable e) {
            failure.set(e);
          } finally {
            installed.countDown();
          }
        }, "issue7337-multi-type-applier");
        applier.setDaemon(true);
        applier.start();

        awaitParkedIn(applier, "TimeSeriesSealedInstallLock", "acquire");

        assertThat(installed.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
            .as("the install waits for the pause, across types as within one")
            .isFalse();

        pause.close();

        assertThat(installed.await(60, TimeUnit.SECONDS))
            .as("and both complete once released - a disagreement over the order would leave each holding what "
                + "the other waits for until a 60s deadline expired")
            .isTrue();
        assertThat(failure.get()).isNull();
        applier.join(60_000);
      }
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine();
    }

    private void ingest(final int samples) throws Exception {
      final long[] timestamps = new long[samples];
      final Object[] hosts = new Object[samples];
      final Object[] values = new Object[samples];
      for (int i = 0; i < samples; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      engine().appendBatch(timestamps, new Object[][] { hosts, values });
    }
  }

  /**
   * Issue #7897: {@code forEachRow} ran the caller's visitor while the shard held {@code compactionLock.readLock()}
   * and the sealed store held {@code directoryLock.readLock()}, so the time a compaction - and therefore an append,
   * which queues behind the waiting writer - spent waiting was the caller's TOTAL work rather than one block's.
   * <p>
   * The regression that made it visible was the #7697 fix: {@code JsonlExporterFormat} swapped {@code iterateQuery}
   * for {@code forEachRow} and its visitor writes a gzip chunk to the archive every {@code TIMESERIES_CHUNK_SIZE}
   * rows, so an {@code EXPORT DATABASE} held a shard's compaction read lock across its own file I/O for the whole
   * export and ingest for the type stalled for exactly that long.
   * <p>
   * These tests pin the property the fix establishes rather than a duration: with a visitor that is blocked and
   * stays blocked, an append still completes, and a compaction that lands DURING such a scan neither loses a row
   * nor hands one over twice.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ScanDoesNotHoldTheCompactionLock extends TestHelper {

    private static final long BASE_TS      = 1_700_000_000_000L;
    private static final int  SEALED_ROWS  = 400;
    private static final int  MUTABLE_ROWS = 40;

    private TimeSeriesEngine engine;

    @BeforeEach
    void populate() throws IOException {
      // One shard: the scan then blocks inside the only shard there is, which is what makes the append it must not
      // block a deterministic thing to ask for.
      database.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      engine = ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine();

      appendRange(0, SEALED_ROWS);
      // Sealed, so the walk blocks in the sealed layer - the half that used to hold BOTH locks across the visitor.
      engine.compactAll();
      appendRange(SEALED_ROWS, MUTABLE_ROWS);
    }

    private void appendRange(final int from, final int count) throws IOException {
      final long[] timestamps = new long[count];
      final Object[] hosts = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = BASE_TS + (from + i) * 1_000L;
        hosts[i] = "host_" + ((from + i) % 4);
        values[i] = (double) (from + i);
      }
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
    }

    /**
     * The reported defect, through {@code forEachRow}: a visitor that is parked mid-scan must not keep an append
     * out. A compaction is started first because that is what turns a shared read lock into a barrier - a queued
     * writer makes {@code appendSamples}' own read lock wait.
     */
    // #7897
    @Test
    @Timeout(180)
    void anAppendCompletesWhileAForEachRowVisitorIsParked() throws Exception {
      assertAppendCompletesWhileVisitorIsParked(
          visitor -> engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, visitor));
    }

    /**
     * The same shape, one method over: {@code forEachTagCombination} held the identical pair of locks across the
     * identical caller-supplied visitor, and {@code GET /prom/api/v1/series} is its caller.
     */
    // #7897
    @Test
    @Timeout(180)
    void anAppendCompletesWhileAForEachTagCombinationVisitorIsParked() throws Exception {
      final int[] tagOnly = { 0 };
      assertAppendCompletesWhileVisitorIsParked(
          visitor -> engine.forEachTagCombination(Long.MIN_VALUE, Long.MAX_VALUE, tagOnly, null, visitor));
    }

    private interface Scan {
      boolean run(TimeSeriesRowVisitor visitor) throws IOException;
    }

    private void assertAppendCompletesWhileVisitorIsParked(final Scan scan) throws Exception {
      final CountDownLatch visitorParked = new CountDownLatch(1);
      final CountDownLatch releaseVisitor = new CountDownLatch(1);
      final AtomicBoolean parkedOnce = new AtomicBoolean();
      final AtomicReference<Throwable> scanFailure = new AtomicReference<>();

      final Thread scanThread = new Thread(() -> {
        try {
          scan.run(row -> {
            if (parkedOnce.compareAndSet(false, true)) {
              visitorParked.countDown();
              try {
                // Stands in for the exporter's gzip chunk write: work the caller does per row that the engine
                // knows nothing about. It is released by the assertions below, or by the timeout, never by itself.
                releaseVisitor.await(150, TimeUnit.SECONDS);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            return true;
          });
        } catch (final Throwable t) {
          scanFailure.set(t);
        }
      }, "issue7897-scan");
      scanThread.start();

      final AtomicReference<Throwable> compactFailure = new AtomicReference<>();
      final Thread compactThread = new Thread(() -> {
        try {
          engine.compactAll();
        } catch (final Throwable t) {
          compactFailure.set(t);
        }
      }, "issue7897-compact");

      final CountDownLatch appendDone = new CountDownLatch(1);
      final AtomicReference<Throwable> appendFailure = new AtomicReference<>();
      final Thread appendThread = new Thread(() -> {
        try {
          appendRange(SEALED_ROWS + MUTABLE_ROWS, 1);
        } catch (final Throwable t) {
          appendFailure.set(t);
        } finally {
          appendDone.countDown();
        }
      }, "issue7897-append");

      try {
        assertThat(visitorParked.await(60, TimeUnit.SECONDS)).as("the scan must reach its first row").isTrue();

        compactThread.start();
        // Give the compaction long enough to reach - and, once fixed, to get past - the write lock it needs. Not
        // asserted on: before the fix it parks there for the whole scan, after the fix it simply runs.
        compactThread.join(TimeUnit.SECONDS.toMillis(10));

        appendThread.start();
        assertThat(appendDone.await(60, TimeUnit.SECONDS))
            .as("an append must not wait for a parked scan visitor: the compaction read lock is what it queues behind")
            .isTrue();
        assertThat(appendFailure.get()).isNull();
      } finally {
        releaseVisitor.countDown();
        scanThread.join(TimeUnit.SECONDS.toMillis(60));
        compactThread.join(TimeUnit.SECONDS.toMillis(60));
        appendThread.join(TimeUnit.SECONDS.toMillis(60));
      }

      assertThat(scanFailure.get()).isNull();
      assertThat(compactFailure.get()).isNull();
    }

    /**
     * The other thing a walk that releases the directory lock between blocks has to survive: a RETENTION pass, which
     * rewrites the sealed file with the surviving blocks only and so moves every remaining block's offsets. A
     * snapshot entry read through its own offsets afterwards would be reading the wrong bytes of a shorter file;
     * {@code resolveLiveBlock} is what turns it back into the block it names, or into nothing when retention dropped
     * it.
     */
    // #7897
    @Test
    @Timeout(180)
    void aRetentionPassDuringTheScanMovesEveryOffsetAndTheWalkSurvivesIt() throws Exception {
      // A fresh type: one block per compaction, so the walk has blocks to be between.
      database.command("sql",
          "CREATE TIMESERIES TYPE Blocky TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      final TimeSeriesEngine blocky = ((LocalTimeSeriesType) database.getSchema().getType("Blocky")).getEngine();

      final int blocks = 6;
      final int perBlock = 500;
      final List<Long> originals = new ArrayList<>();
      for (int b = 0; b < blocks; b++) {
        final long[] timestamps = new long[perBlock];
        final Object[] hosts = new Object[perBlock];
        final Object[] values = new Object[perBlock];
        for (int i = 0; i < perBlock; i++) {
          timestamps[i] = BASE_TS + ((long) b * perBlock + i) * 1_000L;
          hosts[i] = "host_" + (i % 4);
          values[i] = (double) i;
          originals.add(timestamps[i]);
        }
        blocky.appendBatch(timestamps, new Object[][] { hosts, values });
        blocky.compactAll();
      }

      // Drops the first half of the blocks, so the second half's offsets all move.
      final long cutoff = BASE_TS + ((long) blocks / 2 * perBlock) * 1_000L;

      final List<Long> seen = new ArrayList<>();
      final AtomicBoolean retainedOnce = new AtomicBoolean();
      final AtomicReference<Throwable> retentionFailure = new AtomicReference<>();

      blocky.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
        seen.add((Long) row[0]);
        if (retainedOnce.compareAndSet(false, true)) {
          final Thread retention = new Thread(() -> {
            try {
              blocky.applyRetention(cutoff);
            } catch (final Throwable t) {
              retentionFailure.set(t);
            }
          }, "issue7897-retention");
          retention.start();
          try {
            retention.join(TimeUnit.SECONDS.toMillis(60));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return true;
      });

      assertThat(retentionFailure.get()).isNull();
      assertThat(seen).as("the walk must not invent a timestamp out of a block it read at the wrong offset")
          .isSubsetOf(originals);
      assertThat(new HashSet<>(seen)).as("nor hand one over twice").hasSize(seen.size());

      final List<Long> survivors = new ArrayList<>();
      for (final Long ts : originals)
        if (ts >= cutoff)
          survivors.add(ts);
      assertThat(seen).as("every sample retention KEPT is still handed over, at its new offset")
          .containsAll(survivors);
    }

    /**
     * The third directory-rewriting writer, and the one the other two tests leave unexercised against a walk in
     * flight: downsampling REPLACES blocks rather than dropping them, so the rows a snapshot entry names may still
     * be in the store under a different sample count - which is precisely the shape {@code resolveLiveBlock}'s
     * identity does NOT match, and has to decline rather than read at the old offsets (code review on PR #7970).
     * <p>
     * What the walk does with that declined block changed in issue #8166. It used to carry on, so the walk
     * "survived" by returning the rows it had read and silently none of the rest - and the rows it skipped had not
     * been deleted, they had been COARSENED, so the answer was neither the fine rows nor the coarse ones and
     * nothing said so. It now raises {@link TimeSeriesWalkCoarsenedException}: a walk that has already emitted
     * fine rows cannot go on to emit their coarse replacements without counting the crossing bucket at two
     * resolutions, so the honest outcome is to stop and let the caller re-run.
     * <p>
     * The two properties this test was written for are unchanged and still asserted: what reached the visitor
     * BEFORE the raise is real samples read at live offsets, never garbage decoded through a stale one, and never
     * the same sample twice.
     */
    // #7897
    @Test
    @Timeout(180)
    void aDownsamplingPassDuringTheScanIsRefusedRatherThanAnsweredShort() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE Coarse TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      final TimeSeriesEngine coarse = ((LocalTimeSeriesType) database.getSchema().getType("Coarse")).getEngine();

      final int blocks = 6;
      final int perBlock = 500;
      final List<Long> originals = new ArrayList<>();
      for (int b = 0; b < blocks; b++) {
        final long[] timestamps = new long[perBlock];
        final Object[] hosts = new Object[perBlock];
        final Object[] values = new Object[perBlock];
        for (int i = 0; i < perBlock; i++) {
          timestamps[i] = BASE_TS + ((long) b * perBlock + i) * 1_000L;
          hosts[i] = "host_" + (i % 4);
          values[i] = (double) i;
          originals.add(timestamps[i]);
        }
        coarse.appendBatch(timestamps, new Object[][] { hosts, values });
        coarse.compactAll();
      }

      final long newest = BASE_TS + ((long) blocks * perBlock - 1) * 1_000L;
      // Everything older than the last block's worth of samples is folded to one point per minute.
      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier((long) perBlock * 1_000L, 60_000L));

      final List<Long> seen = new ArrayList<>();
      final AtomicBoolean downsampledOnce = new AtomicBoolean();
      final AtomicReference<Throwable> downsampleFailure = new AtomicReference<>();

      assertThatThrownBy(() -> coarse.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
        seen.add((Long) row[0]);
        if (downsampledOnce.compareAndSet(false, true)) {
          final Thread downsampler = new Thread(() -> {
            try {
              coarse.applyDownsampling(tiers, newest);
            } catch (final Throwable t) {
              downsampleFailure.set(t);
            }
          }, "issue7897-downsample");
          downsampler.start();
          try {
            downsampler.join(TimeUnit.SECONDS.toMillis(60));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return true;
      }))
          .as("the rows past the rewrite are coarser, not gone, so no answer from here on is one resolution")
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class);

      assertThat(downsampleFailure.get()).as("the downsample itself must not be what failed").isNull();
      // The walk reads through LIVE offsets or not at all: what the visitor got before the refusal is real samples,
      // never whatever a stale offset into a rewritten file decodes to, and never one of them twice. A SUBSET, not
      // equality, because the refusal deliberately cuts the walk short.
      assertThat(seen).as("no timestamp the store never held").isSubsetOf(originals);
      assertThat(new HashSet<>(seen)).as("and none of them twice").hasSize(seen.size());
      assertThat(seen).as("the rows read before the rewrite landed still reached the visitor").isNotEmpty();

      // And the rows ARE still in the store, coarsened: re-running the read is what the refusal asks the caller to
      // do, and it answers.
      final List<Long> afterwards = new ArrayList<>();
      coarse.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> afterwards.add((Long) row[0]));
      assertThat(afterwards).as("a fresh walk over the coarsened store is a whole answer at one resolution")
          .isNotEmpty();
    }

    /**
     * The one walk that never reaches {@code resolveLiveBlock}: a block whose directory entry declares a single
     * value for every projected TAG column is answered from the SNAPSHOT entry alone - one synthetic row, no file
     * read, no decode (issue #7710) - so the rewrite protection the other race tests exercise is bypassed
     * altogether and what is left holding the answer up is that those entry fields are immutable once published.
     * Raced against a retention pass here rather than argued, because "it cannot go wrong" is the kind of claim
     * this issue was filed about (code review on PR #7970).
     */
    // #7897
    @Test
    @Timeout(180)
    void theDeclaredCombinationFastPathSurvivesARewriteToo() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE OneHostPerBlock TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      final TimeSeriesEngine perBlockHost =
          ((LocalTimeSeriesType) database.getSchema().getType("OneHostPerBlock")).getEngine();

      // ONE host per block, which is what makes each block's directory entry declare a single value for the
      // projected tag column and so makes the #7710 fast path fire instead of a scan.
      final int blocks = 6;
      final int perBlock = 200;
      final List<String> hostNames = new ArrayList<>();
      for (int b = 0; b < blocks; b++) {
        final String host = "host_" + b;
        hostNames.add(host);
        final long[] timestamps = new long[perBlock];
        final Object[] hosts = new Object[perBlock];
        final Object[] values = new Object[perBlock];
        for (int i = 0; i < perBlock; i++) {
          timestamps[i] = BASE_TS + ((long) b * perBlock + i) * 1_000L;
          hosts[i] = host;
          values[i] = (double) i;
        }
        perBlockHost.appendBatch(timestamps, new Object[][] { hosts, values });
        perBlockHost.compactAll();
      }

      final long cutoff = BASE_TS + ((long) blocks / 2 * perBlock) * 1_000L;

      final List<Object> seen = new ArrayList<>();
      final AtomicBoolean retainedOnce = new AtomicBoolean();
      final AtomicReference<Throwable> retentionFailure = new AtomicReference<>();

      // Counted, not assumed: a block answered from its declaration is counted as SKIPPED precisely because it was
      // never decompressed, so a non-zero count is the proof that this test drove the fast path and not a scan.
      final AggregationMetrics metrics = new AggregationMetrics();

      perBlockHost.forEachTagCombination(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 }, metrics, row -> {
        seen.add(row[1]);
        if (retainedOnce.compareAndSet(false, true)) {
          final Thread retention = new Thread(() -> {
            try {
              perBlockHost.applyRetention(cutoff);
            } catch (final Throwable t) {
              retentionFailure.set(t);
            }
          }, "issue7897-fastpath-retention");
          retention.start();
          try {
            retention.join(TimeUnit.SECONDS.toMillis(60));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return true;
      });

      assertThat(retentionFailure.get()).isNull();
      assertThat(seen).as("the fast path must hand back declared host names, not bytes read at a moved offset")
          .isSubsetOf(hostNames.toArray());
      assertThat(seen).as("and it must still answer, rather than skipping every block once the file was rewritten")
          .isNotEmpty();
      assertThat(metrics.getSkippedBlocks())
          .as("at least one block was answered from its directory entry - otherwise this raced an ordinary scan")
          .isPositive();
      assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
          .as("and the fast path decompressed nothing to do it").isZero();
    }

    /**
     * The other half of releasing the locks: the walk no longer sees one frozen shard, so it has to be shown that a
     * compaction landing in the middle of it neither drops a row into the gap between the two layers nor hands one
     * over from both.
     */
    // #7897
    @Test
    @Timeout(180)
    void aCompactionDuringTheScanNeitherLosesNorDuplicatesARow() throws Exception {
      final List<Long> seen = new ArrayList<>();
      final AtomicBoolean compactedOnce = new AtomicBoolean();
      final AtomicReference<Throwable> compactFailure = new AtomicReference<>();

      engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
        seen.add((Long) row[0]);
        if (compactedOnce.compareAndSet(false, true)) {
          // Runs to completion before the scan resumes, so the mutable bucket the scan still has to read has been
          // sealed and cleared underneath it - the exact window the old whole-scan read lock made unreachable.
          final Thread compactor = new Thread(() -> {
            try {
              engine.compactAll();
            } catch (final Throwable t) {
              compactFailure.set(t);
            }
          }, "issue7897-inline-compact");
          compactor.start();
          try {
            compactor.join(TimeUnit.SECONDS.toMillis(60));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return true;
      });

      assertThat(compactFailure.get()).isNull();
      assertThat(compactedOnce.get()).as("the scan must have visited at least one row").isTrue();

      final List<Long> expected = new ArrayList<>();
      for (int i = 0; i < SEALED_ROWS + MUTABLE_ROWS; i++)
        expected.add(BASE_TS + i * 1_000L);

      assertThat(seen).as("every row exactly once, whatever the compaction did while the walk was in flight")
          .containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  /**
   * Regression test for issue #9745: the windowed {@code SELECT count(*) FROM T WHERE ts >= X} is answered by
   * {@code TimeSeriesEngine.aggregateMulti}, which reads a shard's sealed blocks and its mutable bucket inside one
   * compaction read-lock window. That window is only consistent if NOTHING changes either layer while a reader holds it,
   * which means every operation that publishes into the sealed store or clears the mutable bucket has to do it under the
   * same lock's write side.
   * <p>
   * Issue #9716's test pins this for the compaction's Phase 4c (swap then clear). This one pins it for every other
   * publisher a standalone database runs: {@code COMPACT TIMESERIES TYPE} is a compaction followed by a merge of small
   * blocks, and the maintenance scheduler adds retention and downsampling. Each operation is started while the test
   * holds the shard's compaction read lock - exactly what a running aggregation holds - and must leave both layers and the
   * windowed count unchanged until that lock is released. A publisher that skipped the write lock would finish (or
   * publish) inside the window, and the layers read under it would no longer match the snapshot.
   * <p>
   * One shard on purpose: the lock under test is per shard, and with one shard the aggregation reads it on the calling
   * thread, which is what lets this thread re-enter the read lock it already holds to take the windowed count.
   */
  @Nested
  class LayerPublishersWaitForReaders extends TestHelper {

    private static final long T0              = 1_767_225_600_000L;
    private static final int  ROWS_PER_BLOCK  = 1_000;
    private static final int  SEALED_ROUNDS   = 3;
    private static final int  MUTABLE_ROWS    = 500;
    private static final long WINDOW_START_TS = T0 + 500_000L;

    // #9745
    @Test
    void compactionWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
      fill();
      final State after = runWhileAReaderHoldsTheLock(TimeSeriesEngine::compactAll);
      assertThat(after.mutableSamples()).as("the compaction moved the mutable rows once the reader let go").isZero();
      assertThat(after.totalSamples()).isEqualTo(totalRows());
    }

    // #9745
    @Test
    void mergeOfSmallBlocksWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
      fill();
      final int blocksBefore = shard().getSealedStore().getBlockCount();
      assertThat(blocksBefore).as("one small block per compaction round").isGreaterThan(1);
      final State after = runWhileAReaderHoldsTheLock(engine -> engine.mergeSmallBlocks(1));
      assertThat(after.sealedBlocks()).as("the merge folded the small blocks once the reader let go").isLessThan(blocksBefore);
      assertThat(after.totalSamples()).isEqualTo(totalRows());
    }

    // #9745
    @Test
    void retentionWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
      fill();
      final long sealedBefore = shard().getSealedStore().getTotalSampleCount();
      // The cutoff is the first timestamp of the second round, so it is past every sample of the first round's block (the
      // last one is a second earlier) and before every other block: retention drops whole blocks whose samples are all
      // older than the cutoff, which here is exactly that one
      final State after = runWhileAReaderHoldsTheLock(engine -> engine.applyRetention(T0 + ROWS_PER_BLOCK * 1000L));
      assertThat(after.sealedSamples()).as("retention dropped the oldest block once the reader let go")
          .isEqualTo(sealedBefore - ROWS_PER_BLOCK);
      assertThat(after.mutableSamples()).as("retention does not touch the mutable bucket").isEqualTo(MUTABLE_ROWS);
    }

    // #9745
    @Test
    void downsamplingWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
      fill();
      final long sealedBefore = shard().getSealedStore().getTotalSampleCount();
      // One sample per second downsampled to one per minute, for every sealed block
      final State after = runWhileAReaderHoldsTheLock(
          engine -> engine.applyDownsampling(List.of(new DownsamplingTier(1L, 60_000L)), Long.MAX_VALUE / 2));
      assertThat(after.sealedSamples()).as("downsampling reduced the sealed samples once the reader let go").isLessThan(sealedBefore)
          .isGreaterThan(0L);
      assertThat(after.mutableSamples()).as("downsampling does not touch the mutable bucket").isEqualTo(MUTABLE_ROWS);
    }

    @FunctionalInterface
    private interface Maintenance {
      void run(TimeSeriesEngine engine) throws Exception;
    }

    private record State(int sealedBlocks, long sealedSamples, long mutableSamples, long windowedCount) {
      long totalSamples() {
        return sealedSamples + mutableSamples;
      }
    }

    /**
     * Runs {@code maintenance} on another thread while this thread holds the shard's compaction read lock, and checks that
     * neither layer nor the windowed count moved before the lock is released. The operation is observed parked on the
     * lock, not timed: the 30 s bounds are hang guards only.
     */
    private State runWhileAReaderHoldsTheLock(final Maintenance maintenance) throws Exception {
      final TimeSeriesEngine engine = engine();
      assertThat(shard().getCompactionLock()).as("the queue checks below need the JDK's ReentrantReadWriteLock")
          .isInstanceOf(ReentrantReadWriteLock.class);
      final ReentrantReadWriteLock lock = (ReentrantReadWriteLock) shard().getCompactionLock();
      final AtomicReference<Thread> maintenanceThread = new AtomicReference<>();
      final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        final Thread thread = new Thread(r, "issue9745-maintenance");
        maintenanceThread.set(thread);
        return thread;
      });
      try {
        final Future<?> running;
        lock.readLock().lock();
        try {
          final State before = state();
          running = executor.submit(() -> {
            maintenance.run(engine);
            return null;
          });
          final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
          while (!isQueued(lock, maintenanceThread.get()) && !running.isDone() && System.nanoTime() < deadline)
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));

          assertThat(running.isDone()).as("the operation waits for the reader instead of publishing past it").isFalse();
          // Not done AND queued: a publisher that skipped the write lock fails here, whether it already finished or is
          // still running unlocked, before state() is even asked
          assertThat(isQueued(lock, maintenanceThread.get())).as("the operation is parked on the compaction lock").isTrue();
          // state() runs the windowed count on THIS thread, which already holds the read lock: ReentrantReadWriteLock lets a
          // reentrant read through even with a writer queued. aggregateMulti reads a single shard on the calling thread; if it
          // ever read it from another thread, that read would queue behind the parked writer and this would hang up to the
          // 30 s bound below
          assertThat(state()).as("nothing changed under the reader's compaction read lock").isEqualTo(before);
        } finally {
          lock.readLock().unlock();
        }
        running.get(30, TimeUnit.SECONDS);
        final State after = state();
        assertThat(after.windowedCount()).as("the windowed count agrees with the layers it was read from")
            .isEqualTo(sqlCount("SELECT count(*) AS c FROM T WHERE ts >= " + WINDOW_START_TS));
        return after;
      } finally {
        executor.shutdownNow();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
    }

    private static boolean isQueued(final ReentrantReadWriteLock lock, final Thread thread) {
      return thread != null && lock.hasQueuedThread(thread);
    }

    /**
     * One small sealed block per round, then rows left in the mutable bucket, so every operation has something to act on.
     */
    private void fill() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
      int row = 0;
      for (int round = 0; round < SEALED_ROUNDS; round++) {
        insert(row, ROWS_PER_BLOCK);
        row += ROWS_PER_BLOCK;
        engine().compactAll();
      }
      insert(row, MUTABLE_ROWS);
      assertThat(shard().getMutableBucket().getSampleCount()).isEqualTo(MUTABLE_ROWS);
      assertThat(sqlCount("SELECT count(*) AS c FROM T")).isEqualTo(totalRows());
    }

    private void insert(final int first, final int count) {
      database.transaction(() -> {
        for (int i = first; i < first + count; i++)
          database.command("sql", "INSERT INTO T SET ts = ?, host = ?, v = ?", T0 + i * 1000L, "h" + (i % 4), (double) i);
      });
    }

    private static long totalRows() {
      return (long) SEALED_ROUNDS * ROWS_PER_BLOCK + MUTABLE_ROWS;
    }

    private State state() throws Exception {
      final TimeSeriesShard shard = shard();
      return new State(shard.getSealedStore().getBlockCount(), shard.getSealedStore().getTotalSampleCount(),
          shard.getMutableBucket().getSampleCount(), sqlCount("SELECT count(*) AS c FROM T WHERE ts >= " + WINDOW_START_TS));
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    }

    private TimeSeriesShard shard() {
      return engine().getShard(0);
    }

    private long sqlCount(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  /**
   * Issue #10867: a rewrite of a shard's sealed file in flight when DROP TYPE ran swapped its temp file onto the path the drop had
   * just cleared, so the sealed file came back holding every sample of the dropped type, and a type created later under the same name
   * adopted them. The drop and the swap are driven here in the order the race produces - the rewrite has written its temp file, the
   * drop runs, the rewrite installs - which is what the maintenance scheduler and a concurrent DROP TYPE do from two threads.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropRacesSealedRewrite extends TestHelper {

    // #10867
    @Test
    void aMergePlannedBeforeTheDropDoesNotRecreateTheSealedFile() throws Exception {
      final TimeSeriesSealedStore store = createTypeWithSmallBlocks();
      final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);
      assertThat(plan).as("four small blocks are worth a merge").isNotNull();

      database.command("sql", "DROP TYPE Ticker");
      assertThat(sealedFile()).doesNotExist();

      assertThat(store.commitMerge(plan)).as("a merge of a dropped store is not installed").isFalse();
      assertNoFileOfTheTypeIsLeft();

      assertTheRecreatedTypeIsEmpty();
    }

    // #10867
    @Test
    void aRetentionPassAfterTheDropDoesNotRecreateTheSealedFile() throws Exception {
      final TimeSeriesSealedStore store = createTypeWithSmallBlocks();

      database.command("sql", "DROP TYPE Ticker");

      // a cutoff above every block: the rewrite of a dropped store does not even start, so it creates no temp file to leave behind
      assertThatThrownBy(() -> store.truncateBefore(Long.MAX_VALUE)).isInstanceOf(IOException.class)
          .hasMessageContaining("closed on purpose");
      assertNoFileOfTheTypeIsLeft();

      assertTheRecreatedTypeIsEmpty();
    }

    // #10867
    @Test
    void aCompactionInFlightAcrossTheDropDoesNotRecreateTheSealedFile() throws Exception {
      final TimeSeriesSealedStore store = createTypeWithSmallBlocks();

      // what Phase 3 of a compaction in its lock-free window has written when the drop runs: the temp file Phase 4c swaps in
      final File temp = new File(getDatabasePath(), "Ticker_shard_0.ts.sealed.tmp");
      assertThat(temp.createNewFile()).isTrue();

      database.command("sql", "DROP TYPE Ticker");
      assertThat(temp).as("the drop deletes the temp files of the type too").doesNotExist();

      assertThatThrownBy(() -> store.commitTempCompactionFile(List.of())).isInstanceOf(IOException.class)
          .hasMessageContaining("closed on purpose");
      assertNoFileOfTheTypeIsLeft();
      assertTheRecreatedTypeIsEmpty();
    }

    /**
     * The temp paths are fixed, so once the type is created again they belong to the new store: a rewrite of the dropped store still
     * running must neither start a temp file there nor delete, append to or install the new store's.
     */
    // #10867
    @Test
    void aRewriteOfTheDroppedStoreLeavesTheTempFilesOfTheRecreatedTypeAlone() throws Exception {
      final TimeSeriesSealedStore dropped = createTypeWithSmallBlocks();
      final TimeSeriesSealedStore.MergePlan plan = dropped.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);

      database.command("sql", "DROP TYPE Ticker");
      database.command("sql", "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

      // the temp files a rewrite of the new store is in the middle of writing
      final File compactionTemp = new File(getDatabasePath(), "Ticker_shard_0.ts.sealed.tmp");
      final File mergeTemp = new File(getDatabasePath(), "Ticker_shard_0" + TimeSeriesSealedStore.MERGE_TEMP_SUFFIX);
      Files.writeString(compactionTemp.toPath(), "the new store's compaction");
      Files.writeString(mergeTemp.toPath(), "the new store's merge");

      assertThat(dropped.commitMerge(plan)).isFalse();
      assertThatThrownBy(() -> dropped.commitTempCompactionFile(List.of())).isInstanceOf(IOException.class);
      assertThatThrownBy(() -> dropped.appendBlocksToTempFile(List.<byte[][]>of(new byte[][] { {} }), List.<long[]>of(new long[] { 0L, 0L, 1L }),
          List.of(), null, new ArrayList<>())).isInstanceOf(IOException.class);
      assertThatThrownBy(() -> dropped.truncateBefore(Long.MAX_VALUE)).isInstanceOf(IOException.class);
      dropped.deleteTempFileIfExists();
      dropped.deleteMergeTempFileIfExists();

      assertThat(compactionTemp).hasContent("the new store's compaction");
      assertThat(mergeTemp).hasContent("the new store's merge");
      assertThat(sealedFile()).as("the new store's own file, untouched by the old merge").exists();
      assertThat(countSamples()).isZero();

      Files.delete(compactionTemp.toPath());
      Files.delete(mergeTemp.toPath());
    }

    /**
     * A store closed without a drop - the engine a schema reload replaced, whose successor has the file open - refuses a swap as
     * well: the swap would move a file under the successor's open channel.
     */
    // #10867
    @Test
    void aStoreClosedWithoutADropRefusesTheSwapToo() throws Exception {
      final TimeSeriesSealedStore store = createTypeWithSmallBlocks();
      final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);
      final long sizeBefore = sealedFile().length();

      store.close();

      assertThat(store.commitMerge(plan)).isFalse();
      assertThat(sealedFile()).hasSize(sizeBefore);

      // the engine that opens the file next finds it as the closed store left it
      reopenDatabase();
      assertThat(countSamples()).isEqualTo(100);
    }

    /** The control: the same merge before the drop is installed, and the drop then deletes its result. */
    // #10867
    @Test
    void aMergeInstalledBeforeTheDropIsDroppedWithTheType() throws Exception {
      final TimeSeriesSealedStore store = createTypeWithSmallBlocks();
      final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);
      assertThat(store.commitMerge(plan)).isTrue();
      assertThat(store.getBlockCount()).isEqualTo(1);

      database.command("sql", "DROP TYPE Ticker");
      assertNoFileOfTheTypeIsLeft();
      assertTheRecreatedTypeIsEmpty();
    }

    /** Four small sealed blocks of 25 samples each, which a merge folds into one. */
    private TimeSeriesSealedStore createTypeWithSmallBlocks() throws IOException {
      database.command("sql", "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Ticker")).getEngine();
      for (int block = 0; block < 4; block++) {
        final long[] timestamps = new long[25];
        final Object[][] columns = new Object[2][25];
        for (int i = 0; i < 25; i++) {
          timestamps[i] = 1_700_000_000_000L + (block * 25L + i) * 1_000L;
          columns[0][i] = "S" + (i % 3);
          columns[1][i] = (double) (block * 25 + i);
        }
        engine.appendBatch(timestamps, columns);
        engine.compactAll();
      }
      final TimeSeriesSealedStore store = engine.getShard(0).getSealedStore();
      assertThat(store.getBlockCount()).isEqualTo(4);
      assertThat(countSamples()).isEqualTo(100);
      return store;
    }

    private File sealedFile() {
      return new File(getDatabasePath(), "Ticker_shard_0.ts.sealed");
    }

    private void assertNoFileOfTheTypeIsLeft() {
      final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.startsWith("Ticker_"));
      assertThat(files).as("the files of the dropped type").isEmpty();
    }

    private void assertTheRecreatedTypeIsEmpty() {
      database.command("sql", "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
      assertThat(countSamples()).isZero();
      reopenDatabase();
      assertThat(countSamples()).isZero();
    }

    private long countSamples() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Ticker")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  /**
   * Issue #7475: a sealed-store repair on a type whose engine never loaded was excluded by neither
   * {@link TimeSeriesCompactionPause} nor {@link TimeSeriesSealedInstallLock}, because both walked the per-shard
   * compaction locks and a type with no engine has no shard to take one from.
   * <p>
   * The tests drive the type into that state with {@link LocalTimeSeriesType#close()}, which is the same
   * "engine == null" the schema loader leaves behind for issue #6356, and check both directions of the exclusion
   * through the lock that now belongs to the TYPE ({@link LocalTimeSeriesType#getEngineLifecycleLock()}).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EngineLessTypeLock extends TestHelper {
    /** A wait that is EXPECTED to expire: it IS the assertion. A stall can only make it more true. */
    private static final long BLOCKED_PROBE_MS = 1_500L;
    private static final long PARK_WAIT_MS     = 30_000L;

    @Override
    protected void beginTest() {
      database.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    }

    /** The type is left without an engine on purpose, which the after-test integrity check rightly reports. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    private LocalTimeSeriesType engineLessType() {
      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Reading");
      type.close();
      assertThat(type.isEngineAvailable()).as("the state this test starts from: no engine, so no shard").isFalse();
      return type;
    }

    /** The reported case: a backup pause is held, and the repair's install lock must wait for it. */
    // #7475
    @Test
    void aHeldPauseBlocksTheInstallLockOfAnEngineLessType() throws Exception {
      engineLessType();

      final CountDownLatch locked = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
        assertThat(pause.getPausedShards())
            .as("there is no shard to count: it is the type's lifecycle lock that makes the pause cover it").isZero();

        final Thread repair = new Thread(() -> {
          try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
              List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 60_000L)) {
            locked.countDown();
          } catch (final Throwable e) {
            failure.set(e);
            locked.countDown();
          }
        }, "issue7475-repair");
        repair.setDaemon(true);
        repair.start();

        awaitParkedIn(repair, "TimeSeriesSealedInstallLock", "acquire");
        assertThat(locked.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
            .as("a repair must not run through a held pause: that is the duplicated-samples restore").isFalse();

        pause.close();

        assertThat(locked.await(60, TimeUnit.SECONDS))
            .as("and it must proceed the moment the pause is released, or the test proves nothing").isTrue();
        assertThat(failure.get()).isNull();
        repair.join(60_000);
      }
    }

    /** The other direction: a pause cannot start while a repair holds the type. */
    // #7475
    @Test
    void aHeldInstallLockOfAnEngineLessTypeBlocksThePause() throws Exception {
      engineLessType();

      final CountDownLatch paused = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L)) {
        assertThat(lock.getLockedShards()).as("no shard exists yet").isZero();

        final Thread backup = new Thread(() -> {
          try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 60_000L)) {
            paused.countDown();
          } catch (final Throwable e) {
            failure.set(e);
            paused.countDown();
          }
        }, "issue7475-backup");
        backup.setDaemon(true);
        backup.start();

        awaitParkedIn(backup, "TimeSeriesCompactionPause", "acquire");
        assertThat(paused.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
            .as("a copy of the database must wait for a repair in flight rather than photograph it halfway").isFalse();

        lock.close();

        assertThat(paused.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull();
        backup.join(60_000);
      }
    }

    /**
     * A repair that brings the engine up while a pause was waiting must not leave the pause covering nothing: the
     * pause takes the type's lock first and only then reads the engine, so the shards the repair created are locked
     * too. Without the re-read the pause would be granted when the repair ends and hold no shard, and a whole
     * compaction could start and finish inside the window it believes closed.
     */
    // #7475
    @Test
    void aPauseGrantedAfterTheEngineCameUpAlsoHoldsItsShards() throws Exception {
      final LocalTimeSeriesType type = engineLessType();

      final AtomicReference<Integer> pausedShards = new AtomicReference<>();
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread backup;
      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L)) {
        // The pause is taken and closed on ITS thread: the read locks are released by the thread that took them.
        backup = new Thread(() -> {
          try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 60_000L)) {
            pausedShards.set(pause.getPausedShards());
          } catch (final Throwable e) {
            failure.set(e);
          }
        }, "issue7475-late-backup");
        backup.setDaemon(true);
        backup.start();
        awaitParkedIn(backup, "TimeSeriesCompactionPause", "acquire");

        // The repair: the engine comes up while the pause waits.
        try {
          type.initEngine();
        } catch (final Exception e) {
          throw new AssertionError(e);
        }
      }

      backup.join(60_000);
      assertThat(failure.get()).isNull();
      assertThat(pausedShards.get()).as("both shards that appeared while it waited").isEqualTo(2);
    }

    /** Nothing is left held when a budget runs out on an engine-less type. */
    // #7475
    @Test
    void aTimedOutAcquisitionOnAnEngineLessTypeReleasesEverything() {
      engineLessType();

      try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
        assertThatThrownBy(() -> TimeSeriesSealedInstallLock.acquire(database,
            List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 200L))
            .isInstanceOf(TimeoutException.class).hasMessageContaining("Reading");
      }

      try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 5_000L)) {
        assertThat(lock.getLockedShards()).isZero();
      }
    }

    private static void awaitParkedIn(final Thread thread, final String className, final String methodName)
        throws InterruptedException {
      final long deadline = System.currentTimeMillis() + PARK_WAIT_MS;
      while (System.currentTimeMillis() < deadline) {
        final Thread.State state = thread.getState();
        if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
          for (final StackTraceElement frame : thread.getStackTrace())
            if (frame.getClassName().endsWith(className) && methodName.equals(frame.getMethodName()))
              return;
        if (state == Thread.State.TERMINATED)
          break;
        Thread.sleep(10);
      }
      throw new AssertionError("thread '" + thread.getName() + "' never parked in " + className + "." + methodName
          + " (state=" + thread.getState() + "); the block assertion that follows would have been vacuous");
    }
  }
}
