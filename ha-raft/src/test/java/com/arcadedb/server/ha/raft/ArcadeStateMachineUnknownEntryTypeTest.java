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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4798: a committed Raft log entry whose leading type byte is unrecognised
 * (e.g. written by a newer node during a rolling upgrade) must NOT be silently skipped. The old code
 * advanced {@code lastAppliedIndex}, persisted it, and returned {@code OK}, which permanently discarded
 * a committed mutation while the moved-forward index hid the gap from every lag/recovery check (silent
 * divergence). The fix halts the node loudly and leaves the applied index untouched so the entry is
 * replayed once the node is upgraded to a compatible version.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeStateMachineUnknownEntryTypeTest {

  /** A type byte that does not map to any {@link RaftLogEntryType} (valid ids are 1..6). */
  private static final byte UNKNOWN_TYPE_BYTE = (byte) 99;

  private static TransactionContext entryWithUnknownType(final ArcadeStateMachine sm, final long term, final long index) {
    final ByteString unknownPayload = ByteString.copyFrom(new byte[] { UNKNOWN_TYPE_BYTE });
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(unknownPayload).build())
        .build();
    return TransactionContext.newBuilder()
        .setStateMachine(sm)
        .setLogEntry(logEntry)
        .build();
  }

  @Test
  void unknownEntryTypeHaltsInsteadOfSilentlySkipping() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final long unknownIndex = 5L;

    final CompletableFuture<Message> future = sm.applyTransaction(entryWithUnknownType(sm, 1L, unknownIndex));

    // It must fail (not return "OK"): an unknown committed entry is never safely applicable.
    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(future::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("Unknown Raft log entry type at index " + unknownIndex);

    // The node must be halted so an operator notices, rather than degrading silently.
    assertThat(sm.isHaltedAfterCriticalError()).isTrue();

    // Crucially, the applied index must NOT have advanced to the unknown entry: leaving it untouched
    // is what lets the entry be replayed after the node is upgraded, instead of being lost forever.
    assertThat(sm.getLastAppliedTermIndex().getIndex()).isNotEqualTo(unknownIndex);
  }

  @Test
  void afterHaltSubsequentEntriesShortCircuit() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // First unknown entry trips the halt.
    sm.applyTransaction(entryWithUnknownType(sm, 1L, 5L)).exceptionally(t -> null);
    assertThat(sm.isHaltedAfterCriticalError()).isTrue();

    // A later entry (even a higher index) must be refused without touching state, so the
    // StateMachineUpdater cannot cascade on inconsistent in-memory state while the server stops.
    final CompletableFuture<Message> next = sm.applyTransaction(entryWithUnknownType(sm, 1L, 6L));
    assertThat(next.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(next::join)
        .hasCauseInstanceOf(ReplicationException.class)
        .hasMessageContaining("halted after critical error");
  }
}
