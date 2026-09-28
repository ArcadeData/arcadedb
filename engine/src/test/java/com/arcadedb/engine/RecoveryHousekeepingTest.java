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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;

/**
 * Recovery adopts WAL files into the pool also used by the timer. A reopened file has zero pending flush
 * acknowledgements even though none of its transactions has been replayed. Rotating that file mid-replay can
 * delete it and make the recovery loop read the next transaction from its empty replacement instead.
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

  private static void assertNoSuccessLog(final Runnable action) {
    final Logger previous = LogManager.instance().getLogger();
    final Logger capture = spy(previous);
    LogManager.instance().setLogger(capture);
    try {
      action.run();
    } finally {
      LogManager.instance().setLogger(previous);
    }
    final List<String> messages = mockingDetails(capture).getInvocations().stream()
        .filter(invocation -> invocation.getMethod().getName().equals("log"))
        .map(invocation -> (String) invocation.getArgument(2)).toList();
    assertThat(messages).doesNotContain("Recovery of database '%s' completed")
        .contains("Recovery of database '%s' did not complete");
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
