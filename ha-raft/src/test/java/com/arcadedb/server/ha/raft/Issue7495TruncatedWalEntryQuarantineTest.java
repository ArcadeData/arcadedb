/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7495: a committed TX entry whose WAL payload is too short to hold its transaction id has to be
 * quarantined like any other unreadable committed entry, not reported as a bare replication error.
 * <p>
 * Since issue #6965 {@code applyTxEntry} reads the WAL transaction id before anything else, to match the entry
 * against a transaction this node originated. That read reported a truncated payload as a plain
 * {@link ReplicationException}, and {@code applyWithRetry} rethrows a {@code ReplicationException} unchanged -
 * it is the resync signal the WAL-gap path raises - so the failure went straight past
 * {@code handleUnexpectedApplyError}. No database was marked diverged, no targeted snapshot resync was
 * triggered, and the failed future is one Ratis swallows while advancing its own applied index: the corrupt
 * entry was skipped on this node and nothing said so.
 * <p>
 * The fix classifies it as what it is - a {@link RaftLogEntryDecodeException}, the type issue #7138 introduced
 * for a committed entry of a KNOWN type this version cannot read - so it reaches the per-database quarantine
 * (issue #4797) and the leader resends the database as a snapshot.
 * <p>
 * The harness uses a {@code null} server, so no cluster is stood up; the quarantine bookkeeping and the
 * exception classification are what these tests read.
 */
class Issue7495TruncatedWalEntryQuarantineTest {

  private static TransactionContext txEntry(final ArcadeStateMachine sm, final String databaseName,
      final byte[] walData, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  @Test
  void aTruncatedWalPayloadIsClassifiedAsADecodeFailureAndQuarantinesItsDatabase() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // Shorter than the 8 bytes the transaction id occupies: the id cannot be read at all.
    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[3], 5L));

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("Apply error on database 'db-A'")
        .hasMessageContaining("per-database snapshot resync in progress");

    // The classification is the fix: without it the failure is a bare ReplicationException that
    // applyWithRetry rethrows unchanged, so nothing below this line would hold.
    final Throwable failure = catchThrowable(future::join);
    assertThat(failure.getCause().getCause())
        .as("the quarantine wraps the decode failure that caused it")
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining("Cannot read the WAL transaction id");
    assertThat(failure.getCause().getCause().getCause())
        .hasMessageContaining("Corrupted WAL transaction entry");

    assertThat(sm.isDatabaseDiverged("db-A")).as("quarantined for a targeted resync").isTrue();
    assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
    assertThat(sm.isHaltedAfterCriticalError()).as("one bad entry must not halt the whole node").isFalse();
  }

  @Test
  void anEmptyWalPayloadIsTreatedTheSameWay() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L));

    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("per-database snapshot resync in progress");
    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  /**
   * The entry type the decode failure reports matters: {@code handleUnexpectedApplyError} quarantines only when
   * the failure names a database, and a TX entry always does.
   */
  @Test
  void theDecodeFailureNamesTheEntryTypeAndTheDatabase() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[1], 9L));

    final Throwable decode = catchThrowable(future::join).getCause().getCause();
    assertThat(decode).isInstanceOf(RaftLogEntryDecodeException.class);
    assertThat(((RaftLogEntryDecodeException) decode).getType()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(((RaftLogEntryDecodeException) decode).getDatabaseName()).isEqualTo("db-A");
  }

  /**
   * The same reclassification one step further in: a payload long enough to carry the transaction id but corrupt
   * beyond it fails in {@code deserializeWalTransaction}, which rejects a misaligned page count with the same
   * {@link ReplicationException} (issue #4420) and had the same consequence on this path - rethrown unchanged,
   * never quarantined.
   */
  @Test
  void aCorruptWalPayloadPastTheTransactionIdIsAlsoQuarantined() {
    // databaseFor() is the seam the apply resolves a database through, and the decode is the next line after it.
    // Returning null gets past it without a server, so the decode is what fails rather than the lookup.
    final ArcadeStateMachine sm = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return null;
      }
    };

    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    buf.putLong(1L); // txId - readable, so this gets past the transaction-id read
    buf.putLong(0L); // timestamp
    buf.putInt(-7);  // pageCount: the corruption of issue #4420
    buf.putInt(0);   // segmentSize

    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", buf.array(), 11L));

    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("Apply error on database 'db-A'")
        .hasMessageContaining("per-database snapshot resync in progress");

    final Throwable decode = catchThrowable(future::join).getCause().getCause();
    assertThat(decode).isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining("Cannot decode the WAL payload of");
    assertThat(decode.getCause()).hasMessageContaining("invalid page count");

    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  /**
   * The narrow window between the two decoders: 8 bytes is enough for the transaction id, but the WAL header the
   * decoder reads is 24, so a payload in between runs out of buffer before {@code deserializeWalTransaction}
   * reaches any of its explicit checks and raises a {@code BufferUnderflowException} instead of a
   * {@link ReplicationException}. It is the same condition - a committed entry this node cannot read - and it
   * must be reported as the same thing, rather than the diagnosis depending on how far in the corruption starts.
   */
  @Test
  void aPayloadTooShortForTheWalHeaderIsClassifiedLikeAnyOtherUndecodableOne() {
    final ArcadeStateMachine sm = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return null;
      }
    };

    // 10 bytes: past the 8-byte transaction id, short of the 24-byte header.
    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[10], 13L));

    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("Apply error on database 'db-A'");

    final Throwable decode = catchThrowable(future::join).getCause().getCause();
    assertThat(decode).isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining("Cannot decode the WAL payload of");
    assertThat(decode.getCause()).isInstanceOf(BufferUnderflowException.class);

    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  /**
   * The bounded-escalation budget must be charged ONCE per failure.
   * <p>
   * {@code handleUnexpectedApplyError} rethrows the original error once the budget is exhausted, so a node that
   * can never resync halts rather than degrading silently. The original here is a
   * {@link RaftLogEntryDecodeException}, and {@code applyTransaction} catches that type separately as the handler
   * for an unreadable ENVELOPE - a different failure, decoded before {@code applyWithRetry} is ever called.
   * Without the guard in {@code applyWithRetry} the escalated exception reaches that catch and is handled a
   * second time, charging one failure twice against the budget that just tripped.
   */
  @Test
  void theEscalationBudgetIsChargedOncePerUndecodableEntry() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // The budget is `incrementAndGet() > 100`, so the first 100 quarantine and the 101st escalates.
    for (int i = 1; i <= 100; i++) {
      final int index = i;
      assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], index)).join())
          .as("failure #%d is recoverable", i)
          .hasCauseInstanceOf(ReplicationException.class);
    }
    assertThat(sm.divergedSwallowedErrorCount()).isEqualTo(100);
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();

    final CompletableFuture<Message> escalated = sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 101L));

    assertThat(escalated.isCompletedExceptionally()).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).as("a node that can never resync halts loudly").isTrue();
    assertThat(sm.divergedSwallowedErrorCount())
        .as("one failure, one swallow charged - not two")
        .isEqualTo(101);
  }

  /**
   * The coverage {@code ArcadeStateMachinePerDatabaseHaltTest} was written for and quietly stopped having: an
   * apply error that is NOT a decode failure. A payload long enough to carry the transaction id gets past the
   * read that this issue is about and fails where that test's javadoc says it does - resolving the database
   * against a {@code null} server - which is the representative "unexpected error" issue #4797 scoped to one
   * database.
   */
  @Test
  void anUnexpectedApplyErrorPastTheWalReadStillQuarantinesOneDatabaseOnly() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // 8 bytes: the transaction id reads back as 0 and the apply proceeds to resolve the database.
    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[Long.BYTES], 5L));

    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("Apply error on database 'db-A'");
    assertThatThrownBy(future::join).getRootCause().isInstanceOf(NullPointerException.class);

    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();

    // A healthy database keeps being applied on its own merits rather than being refused node-wide.
    final CompletableFuture<Message> dbB = sm.applyTransaction(txEntry(sm, "db-B", new byte[Long.BYTES], 6L));
    assertThatThrownBy(dbB::join).hasMessageNotContaining("halted after critical error");
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }
}
