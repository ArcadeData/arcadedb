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
 * The javadoc on {@code applySecurityUsersEntry} used to tell operators the entry "replays on the next start".
 * It does not, for free: the failed entry does not advance the applied position itself, but it does not halt
 * the node either (#7137), so the very NEXT entry moves {@code lastAppliedIndex} past it - and that counter is
 * what {@code takeSnapshot()} checkpoints from, so any snapshot after this point puts the entry permanently
 * out of replay range.
 * <p>
 * <b>What this test does and does not cover.</b> It pins the in-process half: the counter that feeds
 * {@code takeSnapshot()} ends up past the failed index. It does NOT exercise a restart. Per
 * {@code ha-raft/CLAUDE.md} the replay position comes solely from the Ratis snapshot marker, so whether the
 * entry actually comes back depends on whether a snapshot was taken between the failure and the restart -
 * always on a graceful stop, on the compaction scheduler's interval otherwise, possibly never before a kill.
 * Covering that needs a snapshot-and-replay integration test; issue #7252 tracks its absence. The corrected
 * javadoc is worded for that uncertainty rather than around it.
 * <p>
 * Note which counter is asserted, because the module note calls confusing them the most common wrong turn
 * here: {@code readAppliedIndexCounter()} is the one that decides replay; {@code readPersistedAppliedIndex()}
 * is the {@code .raft/applied-index} file, which feeds bootstrap decisions and never the replay position. Both
 * are checked below, for what each is actually for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7227SecurityEntryAppliedPositionMovesPastFailureTest {

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
   * The half of the claim that is unconditional: the counter {@code takeSnapshot()} checkpoints from ends up
   * above the failed entry, so from the next snapshot onwards there is nothing left to replay. What that
   * snapshot's timing does to a particular restart is out of this test's reach (see the class javadoc).
   */
  @Test
  void theCounterThatFeedsTheSnapshotMovesPastAFailedSecurityEntry(@TempDir final Path databaseDirectory) {
    final AtomicBoolean firstWriteFailed = new AtomicBoolean();
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverWhoseUsersFileFailsOnce(firstWriteFailed, databaseDirectory));

    final CompletableFuture<Message> failed = sm.applyTransaction(securityUsersEntry(sm, 5L));
    assertThat(failed.isCompletedExceptionally()).as("the entry itself still fails").isTrue();
    assertThat(firstWriteFailed).isTrue();

    // On its own, the failed entry leaves every position where it was: this is what made the old comment
    // plausible, and it is only half the story. It is also what arms the assertions below - without it, a
    // counter that read 6 afterwards could have read 6 all along.
    assertThat(sm.readAppliedIndexCounter())
        .as("the failing entry does not record itself as applied")
        .isLessThan(5L);
    assertThat(sm.readPersistedAppliedIndex()).isLessThan(5L);

    // The other half: the node stayed up (#7137), so the next entry applies and writes ITS index.
    final CompletableFuture<Message> next = sm.applyTransaction(securityUsersEntry(sm, 6L));
    assertThat(next.isCompletedExceptionally()).as("the volume is writable again").isFalse();

    assertThat(sm.readAppliedIndexCounter())
        .as("takeSnapshot() checkpoints from this counter, so a snapshot from here on puts index 5 out of reach")
        .isEqualTo(6L);
    assertThat(sm.readPersistedAppliedIndex())
        .as("the .raft/applied-index file moves too - bootstrap bookkeeping, NOT the replay position")
        .isEqualTo(6L);
    assertThat(sm.getLastAppliedTermIndex().getIndex())
        .as("and so does the position reported to Ratis while this process runs")
        .isEqualTo(6L);
  }
}
