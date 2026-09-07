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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7227, item 2: what happens to a {@code SECURITY_USERS_ENTRY} whose local persist failed, AFTER the
 * node kept running.
 * <p>
 * The javadoc on {@code applySecurityUsersEntry} used to tell operators the entry "replays on the next start",
 * which is the opposite of what the same method's SEVERE and the contract note on
 * {@code ServerSecurity.applyReplicatedUsers} say - and the opposite of what the code does. The comment was
 * corrected; this test pins the BEHAVIOUR it now describes, so a later change that makes the entry genuinely
 * replayable is caught here rather than by an operator waiting for a replay that never comes.
 * <p>
 * The mechanism: the failed entry does not advance the applied position itself, but it does not halt the node
 * either (#7137), so the very NEXT entry writes its own higher index over both the persisted position and the
 * Ratis-side one. A restart therefore resumes above the failed index with nothing left to replay, and the user
 * change has to be reissued.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7227SecurityEntryIsNotReplayedTest {

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

  /**
   * A config volume that rejects the FIRST write of the users file and accepts every later one - the ordinary
   * shape of the incident: a full or read-only volume that an operator then fixes.
   */
  private static ArcadeDBServer serverWhoseUsersFileFailsOnce(final AtomicBoolean firstWriteFailed,
      final Path databaseDirectory) {
    final ServerSecurity security = mock(ServerSecurity.class);
    doAnswer(invocation -> {
      if (firstWriteFailed.compareAndSet(false, true))
        throw new ReplicatedUsersPersistenceException("Replicated users applied in memory but could NOT be persisted",
            new IOException("No space left on device"));
      return null;
    }).when(security).applyReplicatedUsers(anyString());

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);
    when(server.getConfiguration()).thenReturn(configuration);
    return server;
  }

  /**
   * The claim the corrected comment makes: the entry is gone, not pending. Both positions a restart consults -
   * the persisted applied index and the Ratis-side last applied term/index - end up ABOVE the failed entry, so
   * {@code reinitialize()} resumes past it.
   */
  @Test
  void aFailedSecurityEntryIsNotReplayedBecauseTheNextEntryMovesThePositionPastIt(@TempDir final Path databaseDirectory) {
    final AtomicBoolean firstWriteFailed = new AtomicBoolean();
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverWhoseUsersFileFailsOnce(firstWriteFailed, databaseDirectory));

    final CompletableFuture<Message> failed = sm.applyTransaction(securityUsersEntry(sm, 5L));
    assertThat(failed.isCompletedExceptionally()).as("the entry itself still fails").isTrue();
    assertThat(firstWriteFailed).isTrue();

    // On its own, the failed entry leaves the position where it was: this is what made the old comment
    // plausible, and it is only half the story.
    assertThat(sm.readPersistedAppliedIndex())
        .as("the failing entry does not record itself as applied")
        .isLessThan(5L);

    // The other half: the node stayed up (#7137), so the next entry applies and writes ITS index everywhere.
    final CompletableFuture<Message> next = sm.applyTransaction(securityUsersEntry(sm, 6L));
    assertThat(next.isCompletedExceptionally()).as("the volume is writable again").isFalse();

    assertThat(sm.readPersistedAppliedIndex())
        .as("the persisted replay floor is now past index 5, so a restart never revisits it")
        .isEqualTo(6L);
    assertThat(sm.getLastAppliedTermIndex().getIndex())
        .as("and so is the position reported to Ratis")
        .isEqualTo(6L);
  }
}
