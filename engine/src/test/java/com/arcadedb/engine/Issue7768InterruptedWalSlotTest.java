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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7768: one interrupted commit used to permanently kill a WAL pool slot.
 * <p>
 * {@code WALFile} held a raw {@code FileChannel} with no recovery for it. A thread interrupted anywhere
 * inside a commit makes NIO close that channel and throw {@code ClosedByInterruptException} -
 * {@code AbstractInterruptibleChannel.begin()} fires the interruptor even when the flag is ALREADY set, so
 * no timing race is needed - and nothing then repaired or replaced the slot: the {@code WALFile} stayed in
 * {@code TransactionManager}'s active pool with {@code open == true} and a dead channel,
 * {@code checkWALFiles()} caught the {@code ClosedChannelException} ahead of the {@code IOException} branch
 * that was written to fence and answered it with a bare {@code close()}, and the size-based rotation branch
 * that could have replaced the slot requires {@code isOpen()} and so could never fire for it again. Every
 * later transaction whose {@code threadId % poolSize} landed on that slot then stalled for the whole 30s
 * {@code WRITE_WAL_TIMEOUT} and failed with a message that reads like a slow disk, on a database that still
 * reported itself open. With the documented {@code arcadedb.txWalFiles=1} the database stopped committing
 * outright.
 * <p>
 * The fix has three parts, and there is a test below per entry point of each:
 * <ol>
 *   <li>{@code WALFile} reopens a channel an interrupt closed and retries the operation - on every one of
 *   its channel operations, not only the commit path - with the clear/restore of the interrupt flag that a
 *   reopened channel needs. This is the WAL-side twin of the recovery {@code PaginatedComponentFile} grew
 *   for the data files in #4930.</li>
 *   <li>{@code checkWALFiles()} no longer swallows {@code ClosedChannelException}: one the reopen refused
 *   means the file is genuinely gone, which is the #7479 case the {@code IOException} branch fences on.</li>
 *   <li>{@code writeTransactionToWAL} offers the transaction to the other files in the pool before spinning
 *   on its own slot, and its timeout message names the state of the pool instead of implying disk latency.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7768InterruptedWalSlotTest extends TestHelper {

  /**
   * A bound generous enough that it cannot redden a healthy run, yet far below the 30s
   * {@code WRITE_WAL_TIMEOUT} the dead slot used to cost: a tripwire between a bounded commit and the
   * unbounded one this issue is about, never a latency measurement.
   */
  private static final long COMMIT_IS_NOT_STALLING_MS = 10_000;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  private static FileChannel channelOf(final WALFile walFile) throws Exception {
    final Field field = WALFile.class.getDeclaredField("channel");
    field.setAccessible(true);
    return (FileChannel) field.get(walFile);
  }

  // ---------------------------------------------------------------------------------------------------
  // 1. WALFile reopens the channel, once per channel operation that can meet an interrupt.
  // ---------------------------------------------------------------------------------------------------

  @Test
  void appendReopensAChannelTheInterruptClosedAndWritesTheWholeRecord() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_7768_append.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    try {
      final FileChannel before = channelOf(walFile);
      final byte[] record = new byte[512];
      for (int i = 0; i < record.length; ++i)
        record[i] = (byte) (i % 251);

      Thread.currentThread().interrupt();
      try {
        walFile.append(ByteBuffer.wrap(record));

        assertThat(Thread.currentThread().isInterrupted())
            .as("the cancellation must stay observable to the caller after the retry")
            .isTrue();
      } finally {
        Thread.interrupted();
      }

      assertThat(channelOf(walFile))
          .as("the interrupt must actually have closed the channel and the append must have reopened it - "
              + "same channel instance means this test proved nothing")
          .isNotSameAs(before);
      assertThat(walFile.isOpen()).isTrue();
      assertThat(walFile.getSize()).isEqualTo(record.length);
      assertThat(Files.readAllBytes(path.toPath()))
          .as("the record must land exactly once and in full, not after a torn prefix the interrupted write left")
          .isEqualTo(record);
    } finally {
      walFile.close();
    }
  }

  @Test
  void getSizeReopensAChannelTheInterruptClosed() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_7768_size.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    try {
      walFile.append(ByteBuffer.wrap(new byte[128]));
      final FileChannel before = channelOf(walFile);

      Thread.currentThread().interrupt();
      final long size;
      try {
        size = walFile.getSize();
      } finally {
        Thread.interrupted();
      }

      assertThat(size).isEqualTo(128);
      assertThat(channelOf(walFile)).isNotSameAs(before);
    } finally {
      walFile.close();
    }
  }

  @Test
  void forceReopensAChannelTheInterruptClosed() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_7768_force.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    try {
      walFile.append(ByteBuffer.wrap(new byte[64]));
      final FileChannel before = channelOf(walFile);

      Thread.currentThread().interrupt();
      try {
        walFile.force(true);
      } finally {
        Thread.interrupted();
      }

      assertThat(channelOf(walFile)).isNotSameAs(before);
      assertThat(walFile.getSize()).isEqualTo(64);
    } finally {
      walFile.close();
    }
  }

  @Test
  void recoveryReadsReopenAChannelTheInterruptClosed() throws Exception {
    // A real, replayable WAL record, produced by the same writer recovery reads back.
    database.transaction(() -> database.newDocument("Doc").set("k", 1).save());

    final File path = new File(database.getDatabasePath(), "txlog_7768_read.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    try {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<MutablePage> pages = Collections.emptyList();
      walFile.append(WALFile.writeTransactionToBuffer(pages, 4242L).getByteBuffer());

      final FileChannel before = channelOf(walFile);

      Thread.currentThread().interrupt();
      final WALFile.WALTransaction tx;
      try {
        // getFirstTransaction -> getTransaction -> readFully/readChunk: the recovery read path, which
        // checkIntegrity() drives at open time.
        tx = walFile.getFirstTransaction();
      } finally {
        Thread.interrupted();
      }

      assertThat(channelOf(walFile)).isNotSameAs(before);
      assertThat(tx).isNotNull();
      assertThat(tx.txId).isEqualTo(4242L);
      assertThat(db.isOpen()).isTrue();
    } finally {
      walFile.close();
    }
  }

  @Test
  void aChannelClosedOnPurposeIsNeverReopened() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_7768_closed.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    walFile.close();

    assertThatThrownBy(walFile::getSize)
        .as("close()/drop() set open=false on purpose; the reopen must refuse, not resurrect the descriptor")
        .isInstanceOf(ClosedChannelException.class);
    assertThat(walFile.isOpen()).isFalse();
  }

  @Test
  void aWalFileGoneFromDiskIsNeverRecreatedByTheReopen() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_7768_vanished.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    try {
      walFile.append(ByteBuffer.wrap(new byte[32]));
      assertThat(path).exists();

      // What #7479 is about: something outside this instance removed the file while it was still open.
      assertThat(path.delete()).isTrue();

      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(walFile::getSize)
            .as("RandomAccessFile(path, \"rw\") would resurrect a WAL file a concurrent instance deleted; "
                + "the reopen must refuse instead so checkWALFiles() can fence")
            .isInstanceOf(FileNotFoundException.class);
      } finally {
        Thread.interrupted();
      }

      assertThat(path).as("the refused reopen must not have re-created the file").doesNotExist();
    } finally {
      walFile.close();
    }
  }

  // ---------------------------------------------------------------------------------------------------
  // 2. The reported end-to-end symptom: the pool slot survives an interrupted commit.
  // ---------------------------------------------------------------------------------------------------

  @Test
  void anInterruptedCommitLeavesTheWalPoolSlotUsable() throws Exception {
    database.close();

    // arcadedb.txWalFiles=1 is documented and supported, and makes the reported failure deterministic:
    // every thread hashes to slot 0, so the one slot the interrupt kills is the whole commit path.
    final Object previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
    GlobalConfiguration.TX_WAL_FILES.setValue(1);
    try {
      database = factory.open();
      database.getSchema().getOrCreateDocumentType("Doc");

      database.transaction(() -> database.newDocument("Doc").set("k", "before").save());

      // The interrupted commit itself may still fail - an interrupt is a real cancellation and the
      // commit is free to honour it. What must NOT happen is the shared pool slot dying with it.
      Thread.currentThread().interrupt();
      try {
        database.transaction(() -> database.newDocument("Doc").set("k", "interrupted").save());
      } catch (final Throwable expectedOrNot) {
        // IGNORE IT: this thread's own outcome is not what this test is about.
      } finally {
        Thread.interrupted();
        if (database.isTransactionActive())
          database.rollback();
      }

      final LocalDatabase db = (LocalDatabase) database;
      final TransactionManager txManager = db.getTransactionManager();

      final StallAwareStopwatch afterInterrupt = StallAwareStopwatch.start();
      database.transaction(() -> database.newDocument("Doc").set("k", "after").save());
      afterInterrupt.assertGaveUpWithin(COMMIT_IS_NOT_STALLING_MS,
          "a commit on a live WAL slot versus the 30s WRITE_WAL_TIMEOUT a dead slot used to cost every "
              + "thread congruent to it");

      // One housekeeping pass, standing in for the once-a-second timer: it used to be the point of no
      // return, closing the slot for good. It must now find a healthy pool and fence nothing.
      txManager.checkWALFilesForTesting();
      assertThat(db.isFencedForRecovery())
          .as("a transient interrupt is not a reason to fence the database")
          .isFalse();

      final StallAwareStopwatch afterHousekeeping = StallAwareStopwatch.start();
      database.transaction(() -> database.newDocument("Doc").set("k", "after-housekeeping").save());
      afterHousekeeping.assertGaveUpWithin(COMMIT_IS_NOT_STALLING_MS,
          "a commit after the housekeeping pass versus the permanent 30s stall the closed slot used to cause");

      assertThat(database.countType("Doc", true))
          .as("every commit from a clean thread must have landed")
          .isGreaterThanOrEqualTo(3);
    } finally {
      GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
    }
  }

  // ---------------------------------------------------------------------------------------------------
  // 3. checkWALFiles() no longer swallows ClosedChannelException.
  // ---------------------------------------------------------------------------------------------------

  /** A WAL file whose channel is permanently dead the way a refused reopen leaves it. */
  private static class DeadChannelWALFile extends WALFile {
    DeadChannelWALFile(final String filePath) throws IOException {
      super(filePath);
    }

    @Override
    public long getSize() throws IOException {
      throw new ClosedChannelException();
    }
  }

  @Test
  void aClosedChannelNoReopenCanRepairFencesInsteadOfBeingSwallowed() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager txManager = db.getTransactionManager();

    assertThat(db.isFencedForRecovery()).isFalse();

    final WALFile dead = new DeadChannelWALFile(db.getDatabasePath() + "/txlog_7768_dead.wal");
    WALFile displaced = null;
    try {
      displaced = txManager.replaceActiveWALFileForTesting(0, dead);

      final List<String> severeLines = WarningCapture.captureSevere(txManager::checkWALFilesForTesting);

      assertThat(db.isFencedForRecovery())
          .as("ClosedChannelException IS an IOException: the narrower catch used to steal - and silently "
              + "close the slot over - exactly the case the IOException branch fences on")
          .isTrue();
      assertThat(severeLines)
          .as("one clear fence diagnostic, not the silent close the old handler did; got: %s", severeLines)
          .hasSize(1)
          .allMatch(line -> line.contains("fenced for recovery"));
    } finally {
      dead.close();
      if (displaced != null)
        txManager.replaceActiveWALFileForTesting(0, displaced);
    }

    database.close();
  }

  /**
   * A WAL file that passes the {@code isOpen()} guard once and then reports itself closed: the shutdown
   * racing the housekeeping timer, which must stay benign rather than fence a database that is going away.
   * <p>
   * Staging the race this way couples the test to {@code checkWALFiles()} consulting {@code isOpen()} twice -
   * once in the rotation guard, once in the {@code ClosedChannelException} branch. Were that ever refactored
   * into a single cached read, this stub would report "open" throughout and the test would keep passing while
   * quietly exercising nothing, so the call count is asserted below rather than assumed.
   */
  private static class ClosingUnderneathWALFile extends WALFile {
    private int isOpenCalls = 0;

    ClosingUnderneathWALFile(final String filePath) throws IOException {
      super(filePath);
    }

    @Override
    public boolean isOpen() {
      return ++isOpenCalls == 1;
    }

    @Override
    public long getSize() throws IOException {
      throw new ClosedChannelException();
    }
  }

  @Test
  void aSlotClosedConcurrentlyWithTheHousekeepingPassDoesNotFence() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager txManager = db.getTransactionManager();

    final ClosingUnderneathWALFile closing = new ClosingUnderneathWALFile(db.getDatabasePath() + "/txlog_7768_closing.wal");
    WALFile displaced = null;
    try {
      displaced = txManager.replaceActiveWALFileForTesting(0, closing);

      txManager.checkWALFilesForTesting();
      final int isOpenCalls = closing.isOpenCalls;

      assertThat(db.isFencedForRecovery())
          .as("a file closed on purpose while this pass was running is a shutdown, not an inaccessible WAL")
          .isFalse();
      assertThat(isOpenCalls)
          .as("checkWALFiles() must ask isOpen() again inside the ClosedChannelException branch; if it ever "
              + "stops doing so this stub never reports itself closed and the race below stops being tested")
          .isGreaterThanOrEqualTo(2);
    } finally {
      closing.close();
      if (displaced != null)
        txManager.replaceActiveWALFileForTesting(0, displaced);
    }
  }

  // ---------------------------------------------------------------------------------------------------
  // 4. writeTransactionToWAL falls back to the rest of the pool instead of spinning for 30s.
  // ---------------------------------------------------------------------------------------------------

  @Test
  void aCommitFallsBackToAnotherWalFileWhenItsOwnSlotIsClosed() throws Exception {
    database.close();

    final Object previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
    GlobalConfiguration.TX_WAL_FILES.setValue(2);
    try {
      database = factory.open();
      database.getSchema().getOrCreateDocumentType("Doc");

      final DatabaseInternal db = (DatabaseInternal) database;
      final TransactionManager txManager = db.getTransactionManager();

      final WALFile closed = new WALFile(database.getDatabasePath() + "/txlog_7768_fallback.wal");
      closed.close();

      final int slot = (int) (Thread.currentThread().threadId() % 2);
      final WALFile displaced = txManager.replaceActiveWALFileForTesting(slot, closed);
      try {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        database.transaction(() -> database.newDocument("Doc").set("k", "fallback").save());
        watch.assertGaveUpWithin(COMMIT_IS_NOT_STALLING_MS,
            "a commit served by another pool slot versus the 30s WRITE_WAL_TIMEOUT spent spinning on a slot "
                + "whose file is closed for good");

        assertThat(database.countType("Doc", true)).isEqualTo(1);
      } finally {
        txManager.replaceActiveWALFileForTesting(slot, displaced);
      }
    } finally {
      GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
    }
  }

  @Test
  void theWalTimeoutDiagnosticNamesThePoolStateInsteadOfImplyingASlowDisk() throws Exception {
    // "Timeout on writing transaction to WAL" on its own reads as a slow disk, which is what sent the
    // reporter of #7768 looking at storage latency rather than at a pool slot that was closed for good.
    // Asserted on the builder directly: producing it through writeTransactionToWAL means waiting out the
    // full 30s WRITE_WAL_TIMEOUT, which is not something a unit test should spend.
    final WALFile open = new WALFile(database.getDatabasePath() + "/txlog_7768_open.wal");
    final WALFile closed = new WALFile(database.getDatabasePath() + "/txlog_7768_message.wal");
    closed.close();
    final WALFile rotatedOut = new WALFile(database.getDatabasePath() + "/txlog_7768_rotated.wal");
    rotatedOut.setActive(false);

    try {
      final String description = TransactionManager.describeWALFilePool(new WALFile[] { open, closed, rotatedOut, null });

      assertThat(description).contains("txlog_7768_open.wal[open]");
      assertThat(description)
          .as("the slot that is actually broken has to be identifiable from the error alone")
          .contains("txlog_7768_message.wal[CLOSED]");
      assertThat(description).contains("txlog_7768_rotated.wal[open][inactive]");
      assertThat(description).contains("3=<none>");
    } finally {
      open.close();
      rotatedOut.close();
    }
  }
}
