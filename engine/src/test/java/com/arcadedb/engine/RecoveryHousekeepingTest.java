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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;

/**
 * Recovery used to adopt the WAL files under replay into the pool the housekeeping timer rotates. A reopened file has
 * zero pending flush acknowledgements even though none of its transactions has been replayed, so rotating it
 * mid-replay deleted it and made the recovery loop read the next transaction from its empty replacement instead.
 *
 * These are replay-order tests, not record-durability tests: valid synthetic WAL records and an observing
 * applyChanges override isolate the recovery loop from page-version mechanics. The real timer pass is driven
 * on a separate thread at a deterministic replay boundary. Each fixture crosses the actual 64 MiB threshold.
 */
@Tag("slow")
class RecoveryHousekeepingTest extends TestHelper {
  private static final long FIRST_TX = 9_000_000L;
  private static final int LARGE_DELTA = 65 * 1024 * 1024;

  @Test
  void housekeepingCannotReplaceTheWALBetweenRecoveryTransactions() throws Exception {
    final ObservingManager manager = installManager();
    final Path wal = writeRecoveryInput();
    manager.afterFirstReplay = () -> driveHousekeeping(manager);

    manager.checkIntegrity();

    assertThat(manager.replayed).as("the second valid transaction must not disappear behind an empty rotated WAL")
        .containsExactly(FIRST_TX, FIRST_TX + 1);
    assertThat(manager.getLastTransactionId()).isEqualTo(FIRST_TX + 1);
    assertThat(wal).as("successful recovery may remove its fully replayed input").doesNotExist();
  }

  @Test
  void aFailedReplayPreservesItsInputAcrossLaterHousekeepingTicks() throws Exception {
    final ObservingManager manager = installManager();
    final Path wal = writeRecoveryInput();
    manager.afterFirstReplay = () -> { throw new IllegalStateException("injected replay failure"); };

    assertThatThrownBy(manager::checkIntegrity).isInstanceOf(IllegalStateException.class)
        .hasMessage("injected replay failure");
    driveHousekeeping(manager);
    driveHousekeeping(manager);

    assertThat(wal).as("failed recovery input must remain available, not be retired by the next timer tick").exists();
    assertThat(manager.replayed).containsExactly(FIRST_TX);
  }

  /**
   * After an escaped replay failure the pool slots still name the input's paths, closed. A clean close then retired them
   * as ordinary files and its directory sweep deleted the WAL the next open needed; only the failed-open path, which
   * always asks for preservation, was safe.
   */
  @Test
  void aCleanCloseAfterAFailedReplayStillPreservesItsInput() throws Exception {
    final ObservingManager manager = installManager();
    final Path wal = writeRecoveryInput();
    manager.afterFirstReplay = () -> { throw new IllegalStateException("injected replay failure"); };
    assertThatThrownBy(manager::checkIntegrity).isInstanceOf(IllegalStateException.class);

    final List<Object[]> logs = captureLogs(() -> assertThat(manager.close(false, false))
        .as("the close must report the WAL as preserved, so the lock file stays too").isTrue());
    assertThat(wal).as("a clean close must not delete a WAL the replay never finished").exists();
    assertThat(logs).as("the preservation must name the failed replay as its reason, not unacked pages")
        .anySatisfy(arguments -> assertThat(arguments).contains("a replay of this WAL did not complete"));
  }

  /**
   * #8626: when the fsync after a full replay fails, the replayed files are retired into the inactive pool so the
   * housekeeping pass drops them once an fsync succeeds. They must stay open and locked there like any retired WAL file:
   * the replay's own cleanup must not close files it no longer owns.
   */
  @Test
  @SuppressWarnings("unchecked")
  void aReplayWhoseFsyncFailsHandsItsInputToHousekeepingStillOpen() throws Exception {
    final ObservingManager manager = installManager();
    final Path wal = writeRecoveryInput();
    manager.failReplaySync = true;

    final List<Object[]> logs = captureLogs(manager::checkIntegrity);
    assertThat(manager.replayed).containsExactly(FIRST_TX, FIRST_TX + 1);
    assertThat(wal).as("the fsync failed, so the WAL is still the only durable copy of the replayed pages").exists();
    assertThat(logs).extracting(arguments -> arguments[2])
        .contains("Recovery of database '%s' completed, its WAL files are kept until an fsync of the data files succeeds")
        .doesNotContain("Recovery of database '%s' completed");

    final Field field = TransactionManager.class.getDeclaredField("inactiveWALFilePool");
    field.setAccessible(true);
    final List<WALFile> retired = List.copyOf((List<WALFile>) field.get(manager));
    assertThat(retired).extracting(WALFile::getFilePath).contains(wal.toString());
    assertThat(retired).as("a retired WAL file stays open and locked until the housekeeping pass drops it")
        .allSatisfy(file -> assertThat(file.isOpen()).isTrue());

    // The data files can be synced again: the next pass fsyncs them and only then drops the replayed WAL.
    driveHousekeeping(manager);
    assertThat(wal).as("once an fsync succeeds, the fully replayed WAL goes").doesNotExist();
    assertThat((List<WALFile>) field.get(manager)).isEmpty();
  }

  @Test
  void normalRotationStillWorksAfterSuccessfulRecovery() throws Exception {
    final ObservingManager manager = installManager();
    writeRecoveryInput();
    manager.checkIntegrity();
    assertThat(manager.replayed).containsExactly(FIRST_TX, FIRST_TX + 1);

    final Field field = TransactionManager.class.getDeclaredField("activeWALFilePool");
    field.setAccessible(true);
    final WALFile active = ((WALFile[]) field.get(manager))[0];
    final Path runtimeWal = Path.of(active.getFilePath());
    try (FileChannel channel = FileChannel.open(runtimeWal, StandardOpenOption.WRITE)) {
      writeFully(channel, transaction(FIRST_TX + 2, LARGE_DELTA));
      channel.force(true);
    }
    driveHousekeeping(manager);
    assertThat(runtimeWal).as("normal runtime rotation resumes once replay has finished").doesNotExist();
  }

  @Test
  void anExceptionDuringRecoveryDoesNotLogSuccess() throws Exception {
    final ObservingManager manager = installManager();
    writeRecoveryInput();
    manager.afterFirstReplay = () -> { throw new IllegalStateException("injected replay failure"); };
    assertNoSuccessLog(() -> assertThatThrownBy(manager::checkIntegrity).isInstanceOf(IllegalStateException.class));
  }

  @Test
  void aVersionGapDuringRecoveryDoesNotLogSuccess() throws Exception {
    final ObservingManager manager = installManager();
    writeRecoveryInput();
    manager.afterFirstReplay = () -> { throw new WALVersionGapException("injected version gap"); };
    assertNoSuccessLog(manager::checkIntegrity);
  }

  @Test
  void closeWaitsForAnInFlightHousekeepingPassAndNoPassRunsAfterIt() throws Exception {
    assertStopWaitsForInFlightPass(manager -> manager.close(false, false));
  }

  @Test
  void killWaitsForAnInFlightHousekeepingPassAndNoPassRunsAfterIt() throws Exception {
    assertStopWaitsForInFlightPass(TransactionManager::kill);
  }

  /**
   * close() and kill() used to wait for a running pass through a latch the pass raised itself, AFTER its "is the
   * database open" check: a tick caught between the two ran concurrently with the pool retirement. The pass is parked
   * here inside its size check, where a rotation is decided, and the stop must wait for it, then refuse later passes.
   */
  private void assertStopWaitsForInFlightPass(final Consumer<TransactionManager> stop) throws Exception {
    final ObservingManager manager = installManager();
    final ProbeWALFile parked = new ProbeWALFile(Path.of(database.getDatabasePath(), "txlog_parked_test.wal"), true);
    manager.replaceActiveWALFileForTesting(0, parked).close();

    final Thread pass = new Thread(manager::runWALHousekeeping, "recovery-housekeeping-pass-test");
    pass.setDaemon(true);
    pass.start();
    assertThat(parked.entered.await(60, TimeUnit.SECONDS)).as("the pass must reach the size check").isTrue();

    final AtomicReference<Throwable> stopFailure = new AtomicReference<>();
    final Thread stopper = new Thread(() -> {
      try {
        stop.accept(manager);
      } catch (final Throwable error) {
        stopFailure.set(error);
      }
    }, "recovery-housekeeping-stop-test");
    stopper.setDaemon(true);
    stopper.start();
    // Either the stop parks behind the pass (correct) or it runs to completion under it (the defect): no timing guess.
    for (int i = 0; i < 6_000 && stopper.getState() != Thread.State.WAITING && stopper.isAlive(); ++i)
      Thread.sleep(10);
    assertThat(stopper.getState()).as("the stop must wait for the pass that is deciding a rotation")
        .isEqualTo(Thread.State.WAITING);

    parked.release.countDown();
    pass.join(60_000);
    stopper.join(60_000);
    assertThat(pass.isAlive()).isFalse();
    assertThat(stopper.isAlive()).isFalse();
    assertThat(stopFailure.get()).isNull();

    // Even handed a live file, a pass after the stop must not look at the pool again.
    final ProbeWALFile late = new ProbeWALFile(Path.of(database.getDatabasePath(), "txlog_late_test.wal"), false);
    try {
      manager.replaceActiveWALFileForTesting(0, late);
      manager.runWALHousekeeping();
      assertThat(late.sizeChecks).as("no housekeeping pass may run once the manager is stopped").hasValue(0);
    } finally {
      manager.replaceActiveWALFileForTesting(0, null);
      late.close();
    }
  }

  private static void assertNoSuccessLog(final Runnable action) {
    final List<Object> messages = captureLogs(action).stream().map(arguments -> arguments[2]).toList();
    assertThat(messages).doesNotContain("Recovery of database '%s' completed")
        .contains("Recovery of database '%s' did not complete");
  }

  /** The arguments of every log call {@code action} makes: requester, level, message format, exception, context, args. */
  private static List<Object[]> captureLogs(final Runnable action) {
    final Logger previous = LogManager.instance().getLogger();
    final Logger capture = spy(previous);
    LogManager.instance().setLogger(capture);
    try {
      action.run();
    } finally {
      LogManager.instance().setLogger(previous);
    }
    return mockingDetails(capture).getInvocations().stream()
        .filter(invocation -> invocation.getMethod().getName().equals("log"))
        .map(invocation -> invocation.getArguments()).toList();
  }

  private ObservingManager installManager() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
    db.getTransactionManager().close(false, false);
    final ObservingManager manager = new ObservingManager(db);
    final Field field = db.getClass().getDeclaredField("transactionManager");
    field.setAccessible(true);
    field.set(db, manager);
    return manager;
  }

  private Path writeRecoveryInput() throws Exception {
    final Path wal = Path.of(database.getDatabasePath(), "txlog_recovery_test.wal");
    try (FileChannel channel = FileChannel.open(wal, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      writeFully(channel, transaction(FIRST_TX, LARGE_DELTA));
      writeFully(channel, transaction(FIRST_TX + 1, 1));
      channel.force(true);
    }
    final WALFile input = new WALFile(wal.toString());
    try {
      final WALFile.WALTransaction first = input.getFirstTransaction();
      assertThat(first.txId).isEqualTo(FIRST_TX);
      assertThat(input.getTransaction(first.endPositionInLog).txId).isEqualTo(FIRST_TX + 1);
      assertThat(input.getSize()).isGreaterThan(64L * 1024 * 1024);
    } finally {
      input.close();
    }
    return wal;
  }

  static void driveHousekeeping(final TransactionManager manager) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        manager.runWALHousekeeping();
      } catch (final Throwable error) {
        failure.set(error);
      }
    }, "recovery-housekeeping-test");
    worker.setDaemon(true);
    worker.start();
    try {
      worker.join(60_000);
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
    assertThat(worker.isAlive()).as("a housekeeping tick must not wait for the replay it is testing").isFalse();
    assertThat(failure.get()).isNull();
  }

  private static final class ObservingManager extends TransactionManager {
    private final List<Long> replayed = new ArrayList<>();
    private Runnable afterFirstReplay;
    private boolean  failReplaySync;

    ObservingManager(final DatabaseInternal db) throws Exception {
      super(db);
      final Field field = TransactionManager.class.getDeclaredField("task");
      field.setAccessible(true);
      ((Timer) field.get(this)).cancel();
    }

    @Override
    public boolean applyChanges(final WALFile.WALTransaction tx, final Map<Integer, Integer> delta, final boolean ignoreErrors) {
      replayed.add(tx.txId);
      if (tx.txId == FIRST_TX && afterFirstReplay != null)
        afterFirstReplay.run();
      return false;
    }

    @Override
    boolean syncReplayedDataFiles() {
      return !failReplaySync && super.syncReplayedDataFiles();
    }
  }

  /** Counts, and optionally parks, the size checks a housekeeping pass makes to decide a rotation. */
  private static final class ProbeWALFile extends WALFile {
    private final AtomicInteger  sizeChecks = new AtomicInteger();
    private final CountDownLatch entered    = new CountDownLatch(1);
    private final CountDownLatch release;

    ProbeWALFile(final Path path, final boolean park) throws FileNotFoundException {
      super(path.toString());
      release = new CountDownLatch(park ? 1 : 0);
    }

    @Override
    public long getSize() throws IOException {
      sizeChecks.incrementAndGet();
      entered.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return super.getSize();
    }
  }

  private static ByteBuffer transaction(final long txId, final int deltaSize) {
    // The same format used by Issue4508TornWALRecoveryTest, with a large valid page delta to cross rotation's limit.
    final int segmentSize = 24 + deltaSize;
    final ByteBuffer buffer = ByteBuffer.allocate(24 + segmentSize + 12);
    buffer.putLong(txId).putLong(1L).putInt(1).putInt(segmentSize);
    buffer.putInt(990_000).putInt(0).putInt(BasePage.PAGE_HEADER_SIZE)
        .putInt(BasePage.PAGE_HEADER_SIZE + deltaSize - 1).putInt(1).putInt(BasePage.PAGE_HEADER_SIZE + deltaSize);
    buffer.position(buffer.position() + deltaSize);
    buffer.putInt(segmentSize).putLong(WALFile.MAGIC_NUMBER);
    return buffer.flip();
  }

  private static void writeFully(final FileChannel channel, final ByteBuffer buffer) throws Exception {
    while (buffer.hasRemaining())
      channel.write(buffer);
  }
}
