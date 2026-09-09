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
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7252: whether a {@code SECURITY_USERS_ENTRY} whose local persist failed is replayed after a restart,
 * pinned on BOTH sides of the one thing that decides it.
 * <p>
 * #7137 made a local write failure of {@code server-users.jsonl} non-fatal - the node keeps running and is
 * already enforcing the replicated list in memory - so what is outstanding is durability alone. #7227 corrected
 * the javadoc that promised the entry "replays on the next start" into a hedge, and
 * {@link Issue7227SecurityEntryAppliedPositionMovesPastFailureTest} pinned the in-process half: the failed entry
 * does not advance the applied position itself, but the NEXT entry does, and that counter is what
 * {@code takeSnapshot()} checkpoints from. Neither branch of the restart was covered anywhere, which is what
 * made the hedge unfalsifiable: a later change that made the replay deterministic - or removed it - would have
 * left the operator guidance silently wrong in one direction or the other.
 * <p>
 * <b>The one input.</b> Per {@code ha-raft/CLAUDE.md} the replay position after a restart comes SOLELY from the
 * Ratis snapshot marker; {@code .raft/applied-index} is read in {@code reinitialize()} and never feeds it. So
 * these two tests differ in exactly one step - whether {@code takeSnapshot()} ran before the restart - and
 * assert opposite outcomes. The second one asserts the persisted file reads 6 while the replay floor is -1, the
 * confusion the module note calls the most common wrong turn in this module.
 * <p>
 * <b>What models Ratis.</b> {@link #replayRetainedLog} feeds back the retained entries ABOVE the seeded floor,
 * in order, which is what {@code StateMachineUpdater} does after {@code reinitialize()} returns. The floor is
 * read from the state machine rather than assumed, so the model cannot pass by agreeing with itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7252SecurityEntryReplayAfterRestartTest {

  private static final String USERS_JSON = "[{\"name\":\"root\",\"password\":\"x\"}]";

  /** The index of the entry whose local persist fails, and of the successful one right behind it. */
  private static final long FAILED_INDEX     = 5L;
  private static final long SUCCEEDING_INDEX = 6L;

  /**
   * A snapshot taken after the failure moves the marker past the failed index, and from there on the entry is out
   * of replay range for good: the restarted node is seeded at 6 and Ratis feeds it nothing. The users file stays
   * at whatever it held before, which is exactly why the SEVERE tells the operator to reissue the user change.
   */
  @Test
  void aSnapshotTakenAfterTheFailurePutsTheEntryOutOfReplayRange(@TempDir final Path raftDirectory,
      @TempDir final Path databaseDirectory, @TempDir final Path usersDirectory) throws Exception {

    final Path usersFile = usersDirectory.resolve("server-users.jsonl");
    final long marker = runUntilRestart(raftDirectory, databaseDirectory, usersFile, true);
    assertThat(marker)
        .as("takeSnapshot() checkpoints from the counter the successful entry advanced")
        .isEqualTo(SUCCEEDING_INDEX);

    final AtomicBoolean neverFails = new AtomicBoolean(true);
    final ArcadeStateMachine restarted = new ArcadeStateMachine();
    final RaftStorage reopened = newRecoveredStorage(raftDirectory);
    try {
      restarted.setServer(serverWhoseUsersFileFailsOnce(neverFails, databaseDirectory, usersFile));
      restarted.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), reopened);

      assertThat(restarted.getStateMachineStorage().getLatestSnapshot())
          .as("the marker written before the restart is rediscovered")
          .isNotNull();
      final long replayFloor = restarted.readAppliedIndexCounter();
      assertThat(replayFloor)
          .as("reinitialize() seeds the replay position from the marker, which is already past the failed entry")
          .isEqualTo(SUCCEEDING_INDEX);

      replayRetainedLog(restarted, replayFloor, FAILED_INDEX, SUCCEEDING_INDEX);

      assertThat(usersFile)
          .as("nothing above the marker is left to replay, so the failed entry never reaches this node again "
              + "and its user list is not restored by the restart")
          .doesNotExist();
    } finally {
      restarted.close();
      reopened.close();
    }
  }

  /**
   * The mirror case: a kill before any snapshot leaves no marker, {@code reinitialize()} seeds -1, and Ratis
   * replays the whole retained log - so the entry comes back and, on a volume that has since been fixed, its
   * durability restores itself with nothing asked of the operator.
   */
  @Test
  void withNoSnapshotTheEntryIsReplayedAndTheUsersFileIsWritten(@TempDir final Path raftDirectory,
      @TempDir final Path databaseDirectory, @TempDir final Path usersDirectory) throws Exception {

    final Path usersFile = usersDirectory.resolve("server-users.jsonl");
    final long marker = runUntilRestart(raftDirectory, databaseDirectory, usersFile, false);
    assertThat(marker).as("no snapshot was taken before the kill").isEqualTo(RaftLog.INVALID_LOG_INDEX);

    final AtomicBoolean neverFails = new AtomicBoolean(true);
    final ArcadeStateMachine restarted = new ArcadeStateMachine();
    final RaftStorage reopened = newRecoveredStorage(raftDirectory);
    try {
      restarted.setServer(serverWhoseUsersFileFailsOnce(neverFails, databaseDirectory, usersFile));
      restarted.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), reopened);

      assertThat(restarted.getStateMachineStorage().getLatestSnapshot())
          .as("a kill takes no snapshot, so there is no marker to seed from")
          .isNull();
      final long replayFloor = restarted.readAppliedIndexCounter();
      assertThat(replayFloor).isEqualTo(-1L);
      assertThat(restarted.readPersistedAppliedIndex())
          .as(".raft/applied-index survived the restart reading 6 and still does not decide the replay position "
              + "- the confusion ha-raft/CLAUDE.md calls the most common wrong turn here")
          .isEqualTo(SUCCEEDING_INDEX);

      replayRetainedLog(restarted, replayFloor, FAILED_INDEX, SUCCEEDING_INDEX);

      assertThat(usersFile)
          .as("the entry was replayed onto a volume that is writable again, so durability restored itself")
          .exists();
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).isEqualTo(USERS_JSON);
    } finally {
      restarted.close();
      reopened.close();
    }
  }

  /**
   * Drives one state machine through the incident - a persist failure at {@link #FAILED_INDEX}, a successful
   * entry at {@link #SUCCEEDING_INDEX} - and shuts it down, optionally checkpointing first. Returns what
   * {@code takeSnapshot()} reported, or {@link RaftLog#INVALID_LOG_INDEX} when it was never called.
   *
   * @param snapshotBeforeShutdown {@code true} models a graceful stop (Ratis's {@code StateMachineUpdater.stop()}
   *                               always takes a snapshot), {@code false} a kill.
   */
  private static long runUntilRestart(final Path raftDirectory, final Path databaseDirectory, final Path usersFile,
      final boolean snapshotBeforeShutdown) throws Exception {

    final AtomicBoolean firstWriteFailed = new AtomicBoolean();
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftStorage storage = newFormattedStorage(raftDirectory);
    try {
      sm.setServer(serverWhoseUsersFileFailsOnce(firstWriteFailed, databaseDirectory, usersFile));
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

      final CompletableFuture<Message> failed = sm.applyTransaction(securityUsersEntry(sm, FAILED_INDEX));
      assertThat(failed.isCompletedExceptionally()).as("the local persist failed").isTrue();
      assertThat(firstWriteFailed).isTrue();
      assertThat(usersFile).as("nothing reached disk on the failing volume").doesNotExist();

      // #7137: the node stays up, so the next committed entry applies and carries the applied position past the
      // failed one. This is the step that makes the snapshot below decide the whole question.
      final CompletableFuture<Message> next = sm.applyTransaction(securityUsersEntry(sm, SUCCEEDING_INDEX));
      assertThat(next.isCompletedExceptionally()).as("the volume is writable again").isFalse();
      Files.deleteIfExists(usersFile);

      return snapshotBeforeShutdown ? sm.takeSnapshot() : RaftLog.INVALID_LOG_INDEX;
    } finally {
      sm.close();
      storage.close();
    }
  }

  /**
   * What Ratis feeds a restarted state machine: every retained entry whose index is ABOVE the seeded floor, in
   * order. {@code StateMachineUpdater} starts from {@code getLatestSnapshot().getIndex()}, which is the value
   * {@code reinitialize()} has just published, so passing the floor the state machine itself reports keeps this
   * model honest rather than self-confirming.
   */
  private static void replayRetainedLog(final ArcadeStateMachine sm, final long floor, final long... retainedIndexes) {
    for (final long index : retainedIndexes)
      if (index > floor)
        sm.applyTransaction(securityUsersEntry(sm, index));
  }

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
   * A config volume that rejects the FIRST write of the users file and writes every later one to {@code usersFile}
   * - the ordinary shape of the incident: a full or read-only volume that an operator then fixes. Writing for
   * real is what lets the replay branch assert the file rather than a call count.
   */
  private static ArcadeDBServer serverWhoseUsersFileFailsOnce(final AtomicBoolean firstWriteFailed,
      final Path databaseDirectory, final Path usersFile) {
    final ServerSecurity security = mock(ServerSecurity.class);
    doAnswer(invocation -> {
      if (firstWriteFailed.compareAndSet(false, true))
        throw new ReplicatedUsersPersistenceException("Replicated users applied in memory but could NOT be persisted",
            new IOException("No space left on device"));
      Files.writeString(usersFile, invocation.getArgument(0, String.class), StandardCharsets.UTF_8);
      return null;
    }).when(security).applyReplicatedUsers(anyString());

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getDatabaseNames()).thenReturn(Set.of());
    return server;
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder().setDirectory(dir.toFile()).setOption(RaftStorage.StartupOption.FORMAT).build();
  }

  private static RaftStorage newRecoveredStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder().setDirectory(dir.toFile()).setOption(RaftStorage.StartupOption.RECOVER).build();
  }

  /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue7252SecurityEntryReplayAfterRestartTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
