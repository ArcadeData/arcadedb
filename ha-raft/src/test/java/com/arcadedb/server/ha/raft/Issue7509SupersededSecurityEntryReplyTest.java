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
import com.arcadedb.server.security.ServerSecurity;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7509, the half a {@code ServerSecurity} test cannot reach: what the state machine ANSWERS for a
 * security entry whose compare-and-set precondition no longer holds.
 * <p>
 * Refusing the entry is only half a fix. The node that submitted it has already answered its operator, and
 * unless the refusal travels back that operator is told a change landed that was thrown away - the silent
 * success the issue is about, just relocated. The verdict has to ride the REPLY rather than be observed
 * locally, because the submitter is not necessarily the leader and a follower's own apply of the entry can lag
 * the reply it gets back.
 * <p>
 * So what is pinned here is the contract between the apply and {@code RaftTransactionBroker.wasApplied}: a
 * refused entry completes NORMALLY - the applied index must advance, it is a no-op on every node, not a
 * failure - carrying {@link ArcadeStateMachine#SECURITY_ENTRY_SUPERSEDED_REPLY}, and an installed one carries
 * something else.
 */
class Issue7509SupersededSecurityEntryReplyTest {

  private static final String USERS_JSON  = "[{\"name\":\"root\",\"password\":\"x\"}]";
  private static final String FINGERPRINT = "0f".repeat(32);

  @Test
  void aRefusedUsersEntryAnswersTheSupersededMarkerAndDoesNotFail() {
    final ServerSecurity security = securityThatRefuses();
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(
        entry(sm, 5L, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, FINGERPRINT)));

    assertThat(future.isCompletedExceptionally())
        .as("a refusal is a no-op apply, not a failed one: the applied index must still advance").isFalse();
    assertThat(future.join().getContent().toStringUtf8())
        .isEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  @Test
  void anInstalledUsersEntryAnswersSomethingElse() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.applyReplicatedUsers(anyString(), anyString())).thenReturn(true);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(
        entry(sm, 5L, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, FINGERPRINT)));

    assertThat(future.join().getContent().toStringUtf8())
        .isNotEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  @Test
  void aRefusedGroupsEntryAnswersTheSupersededMarker() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.applyReplicatedGroups(anyString(), anyString())).thenReturn(false);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(entry(sm, 5L,
        RaftLogEntryCodec.encodeSecurityGroupsEntry("{\"databases\":{},\"version\":2}", FINGERPRINT)));

    assertThat(future.join().getContent().toStringUtf8())
        .isEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  @Test
  void aRefusedApiTokensEntryAnswersTheSupersededMarker() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.applyReplicatedApiTokens(anyString(), anyString())).thenReturn(false);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(entry(sm, 5L,
        RaftLogEntryCodec.encodeSecurityApiTokensEntry("{\"version\":1,\"tokens\":[]}", FINGERPRINT)));

    assertThat(future.join().getContent().toStringUtf8())
        .isEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  /**
   * An entry with no precondition - a seed, and every entry a node that predates issue #7509 wrote - must not
   * even reach the compare-and-set overload: it goes to the unconditional apply, unchanged.
   */
  @Test
  void anEntryWithoutAPreconditionTakesTheUnconditionalApply() {
    final ServerSecurity security = mock(ServerSecurity.class);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(
        entry(sm, 5L, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON)));

    verify(security).applyReplicatedUsers(USERS_JSON);
    assertThat(future.join().getContent().toStringUtf8())
        .isNotEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  private static ServerSecurity securityThatRefuses() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.applyReplicatedUsers(anyString(), anyString())).thenReturn(false);
    return security;
  }

  private static ArcadeStateMachine stateMachine(final ServerSecurity security) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private static TransactionContext entry(final ArcadeStateMachine sm, final long index, final ByteString payload) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
