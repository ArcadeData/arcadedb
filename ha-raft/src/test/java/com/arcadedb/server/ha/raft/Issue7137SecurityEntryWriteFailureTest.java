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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7137: a local {@link IOException} writing {@code server-users.jsonl} while
 * applying a {@code SECURITY_USERS_ENTRY} halted the WHOLE node - every co-located database - and, because
 * the halt deliberately does not advance the applied index, the next start replayed the same entry and
 * halted again. With an environmental cause (a full or read-only config volume) the pod crash-looped
 * indefinitely, with nothing in the log connecting it to the password change that triggered it.
 * <p>
 * The payload is valid and every other node applies it, and nothing here can diverge the databases the node
 * replicates - the failure is confined to one file of server-local configuration. A security entry that cannot
 * be persisted locally is an operational problem, not a state-divergence risk, so the node must stay up with a
 * loud SEVERE instead of halting.
 * <p>
 * Staying up must not mean honouring credentials the operator has just revoked, which is the other half of the
 * fix: {@code ServerSecurity.applyReplicatedUsers} publishes the new list in memory BEFORE reporting the write
 * failure, so the revocation is effective here immediately and only its durability is outstanding. That half is
 * covered by {@code Issue7137ReplicatedUsersAppliedOnWriteFailureTest} in the server module; this class covers
 * the node not halting.
 * <p>
 * The empty {@code databaseName} the codec gives this entry is what routed the failure into the node-wide
 * branch of {@code handleUnexpectedApplyError}, since the per-database quarantine of #4797 is skipped for
 * entries with no single target database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7137SecurityEntryWriteFailureTest {

  private static final String USERS_JSON = "[{\"name\":\"root\",\"password\":\"x\"}]";

  private static TransactionContext securityUsersEntry(final ArcadeStateMachine sm, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON);
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  /** A server whose security layer cannot persist the replicated users file: the full/read-only volume. */
  private static ArcadeDBServer serverThatCannotPersistUsers() {
    final ServerSecurity security = mock(ServerSecurity.class);
    doThrow(new ServerException("Failed to save replicated users file",
        new IOException("No space left on device"))).when(security).applyReplicatedUsers(anyString());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    return server;
  }

  @Test
  void aLocalUsersFileWriteFailureDoesNotHaltTheNode() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverThatCannotPersistUsers());

    final CompletableFuture<Message> future = sm.applyTransaction(securityUsersEntry(sm, 5L));

    assertThat(future.isCompletedExceptionally()).as("the entry itself must still fail").isTrue();
    assertThat(sm.isHaltedAfterCriticalError())
        .as("a fail-safe, node-scoped apply failure must not stop the whole node (issue #7137)")
        .isFalse();
  }

  /**
   * The crash loop is the real damage: with the halt tripped, every later entry short-circuits on the
   * node-wide flag, so the node is dead for its databases too. It must keep applying after the failure.
   */
  @Test
  void laterEntriesAreStillAppliedAfterASecurityEntryFails() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverThatCannotPersistUsers());

    sm.applyTransaction(securityUsersEntry(sm, 5L)).exceptionally(t -> null);

    final CompletableFuture<Message> next = sm.applyTransaction(securityUsersEntry(sm, 6L));
    assertThatThrownBy(next::join)
        .as("the second entry must fail on its own merits, not on a node-wide halt flag")
        .hasMessageNotContaining("halted after critical error");
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  /** No database may be quarantined for it either: this entry targets none, and "" is not a database. */
  @Test
  void noDatabaseIsQuarantinedForANodeScopedEntry() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverThatCannotPersistUsers());

    sm.applyTransaction(securityUsersEntry(sm, 5L)).exceptionally(t -> null);

    assertThat(sm.isDatabaseDiverged("")).isFalse();
  }
}
