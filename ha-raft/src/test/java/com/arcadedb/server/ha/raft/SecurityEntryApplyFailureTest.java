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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
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
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for how the state machine applies security entries when the local users file cannot be written (halt, applied position, replay after restart, durability floor).
 *
 * <p>Regression coverage for #7137:
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
 * covered by {@code ReplicatedSecurityEntryTest.UsersAppliedOnWriteFailure} in the server module; this class covers
 * the node not halting.
 * <p>
 * The empty {@code databaseName} the codec gives this entry is what routed the failure into the node-wide
 * branch of {@code handleUnexpectedApplyError}, since the per-database quarantine of #4797 is skipped for
 * entries with no single target database.
 *
 * <p>Regression coverage for #7227:
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
 * <p>Regression coverage for #7252:
 * Issue #7252: whether a {@code SECURITY_USERS_ENTRY} whose local persist failed is replayed after a restart,
 * pinned on BOTH sides of the one thing that decides it.
 * <p>
 * #7137 made a local write failure of {@code server-users.jsonl} non-fatal - the node keeps running and is
 * already enforcing the replicated list in memory - so what is outstanding is durability alone. #7227 corrected
 * the javadoc that promised the entry "replays on the next start" into a hedge, and
 * {@link SecurityEntryApplyFailureTest} pinned the in-process half: the failed entry
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
 * <p>Regression coverage for #7752:
 * Issue #7752: a security entry whose effects did not reach this node's disk stays in the Raft log until they do, so a
 * restart replays it instead of reading back a document - or a compare-and-set baseline - the cluster has moved past.
 * <p>
 * The incident every test drives: a {@code SECURITY_USERS_ENTRY} at {@link #FAILED_INDEX} whose local write fails, then
 * an unrelated {@code SECURITY_GROUPS_ENTRY} at {@link #LATER_INDEX} that applies normally and carries the applied
 * position past it. That second entry is what used to let a snapshot compact the first one away (issue #7227): it is not
 * a users entry, so nothing about it repairs the users document. Issue #7252 pinned that outcome as the behaviour; this
 * pins the replacement. The failure is reported through {@code ServerSecurity.isReplicatedDocumentDurable}, answered
 * here from the same flag the fake's write flips, so the test exercises the state machine's reading of it rather than
 * the file layer, which {@code ReplicatedSecurityEntryTest.ReplayAfterFailedWrite} in the {@code server} module covers.
 */
class SecurityEntryApplyFailureTest {

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
  private static FakeArcadeDBServer serverThatCannotPersistUsers() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.fails("applyReplicatedUsers", new ReplicatedUsersPersistenceException("Replicated users applied in memory but could NOT be persisted",
        new IOException("No space left on device")));

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.security(security);
    server.returns("getConfiguration", new ContextConfiguration());
    return server;
  }

  // Regression for #7137: a local users file write failure does not halt the node
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

  // Regression for #7137: later entries are still applied after a security entry fails
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

  // Regression for #7137: a payload this node cannot read still halts the node
  /**
   * The other half of the classification, and the reason it is not simply {@code catch (RuntimeException)}: a
   * payload this node cannot READ is not "the disk is full". It means this node cannot apply a committed entry
   * its peers applied, with no database to quarantine, and that still halts - the case #4798 argues must never
   * be skipped quietly.
   */
  @Test
  void aPayloadThisNodeCannotReadStillHaltsTheNode() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    // What a user entry missing "name" actually produces: JSONObject.getString throws JSONException, a plain
    // RuntimeException. (An IllegalArgumentException would not do here - applyTransaction has had its own
    // non-halting arm for those since long before this PR, and the point is what the SECURITY catch does.)
    security.fails("applyReplicatedUsers", new JSONException("no 'name' in user entry"));
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.security(security);
    server.returns("getConfiguration", new ContextConfiguration());

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);

    final CompletableFuture<Message> future = sm.applyTransaction(securityUsersEntry(sm, 5L));

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThat(sm.isHaltedAfterCriticalError())
        .as("an unreadable committed entry is not the fail-safe case and must not be downgraded")
        .isTrue();
  }

  // Regression for #7137: no database is quarantined for a node scoped entry
  /** No database may be quarantined for it either: this entry targets none, and "" is not a database. */
  @Test
  void noDatabaseIsQuarantinedForANodeScopedEntry() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverThatCannotPersistUsers());

    sm.applyTransaction(securityUsersEntry(sm, 5L)).exceptionally(t -> null);

    assertThat(sm.isDatabaseDiverged("")).isFalse();
  }

  /**
   * A config volume that rejects the FIRST write of the users file and accepts every later one - the ordinary
   * shape of the incident: a full or read-only volume that an operator then fixes.
   */
  private static ArcadeDBServer serverWhoseUsersFileFailsOnce(final AtomicBoolean firstWriteFailed,
      final Path databaseDirectory) {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("applyReplicatedUsers", args -> {
      if (firstWriteFailed.compareAndSet(false, true))
        throw new ReplicatedUsersPersistenceException("Replicated users applied in memory but could NOT be persisted",
            new IOException("No space left on device"));
      return null;
    });

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    server.security(security);
    return server;
  }

  // Regression for #7227: the counter that feeds the snapshot moves past a failed security entry
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

  /** The index of the entry whose local persist fails, and of the successful one right behind it. */
  private static final long FAILED_INDEX     = 5L;

  private static final long SUCCEEDING_INDEX = 6L;

  // Regression for #7252: a snapshot taken after the failure puts the entry out of replay range
  /**
   * A snapshot taken after the failure moves the marker past the failed index, and from there on the entry is out
   * of replay range for good: the restarted node is seeded at 6 and Ratis feeds it nothing. That is correct here
   * because the entry at 6 is a durable user list that supersedes the failed one; when nothing supersedes it, the
   * checkpoint stops below the failed entry instead ({@code SecurityEntryApplyFailureTest}).
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

  // Regression for #7252: with no snapshot the entry is replayed and the users file is written
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

  /**
   * A config volume that rejects the FIRST write of the users file and writes every later one to {@code usersFile}
   * - the ordinary shape of the incident: a full or read-only volume that an operator then fixes. Writing for
   * real is what lets the replay branch assert the file rather than a call count.
   */
  private static ArcadeDBServer serverWhoseUsersFileFailsOnce(final AtomicBoolean firstWriteFailed,
      final Path databaseDirectory, final Path usersFile) {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("applyReplicatedUsers", args -> {
      if (firstWriteFailed.compareAndSet(false, true))
        throw new ReplicatedUsersPersistenceException("Replicated users applied in memory but could NOT be persisted",
            new IOException("No space left on device"));
      try {
        Files.writeString(usersFile, (String) args[0], StandardCharsets.UTF_8);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      return null;
    });

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    server.security(security);
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
        SecurityEntryApplyFailureTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  private static final long   LATER_INDEX  = 6L;

  // Regression for #7752: a snapshot stops below an undurable security entry so the restart replays it
  /**
   * The defect and its fix in one run: the checkpoint stops below the failed entry, so the restarted node replays it and
   * the users file reaches the disk with nothing asked of the operator.
   */
  @Test
  void aSnapshotStopsBelowAnUndurableSecurityEntrySoTheRestartReplaysIt(@TempDir final Path raftDirectory,
      @TempDir final Path databaseDirectory, @TempDir final Path usersDirectory) throws Exception {
    final Path usersFile = usersDirectory.resolve("server-users.jsonl");

    final long marker;
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftStorage storage = newStorage(raftDirectory, RaftStorage.StartupOption.FORMAT);
    try {
      sm.setServer(serverWhoseUsersFileFailsOnceDurability(new AtomicBoolean(), databaseDirectory, usersFile));
      sm.initialize(stubServerDurability(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

      assertThat(sm.applyTransaction(securityUsersEntryDurability(sm, FAILED_INDEX)).isCompletedExceptionally())
          .as("the local write failed").isTrue();
      assertThat(sm.applyTransaction(securityGroupsEntry(sm, LATER_INDEX)).isCompletedExceptionally()).isFalse();
      assertThat(sm.readAppliedIndexCounter()).as("the applied position moved past the failed entry").isEqualTo(LATER_INDEX);

      // A graceful stop always takes this snapshot (StateMachineUpdater.stop()), which is what used to lose the entry
      marker = sm.takeSnapshot();
    } finally {
      sm.close();
      storage.close();
    }
    assertThat(marker).as("the checkpoint stops right below the entry whose effects are not durable")
        .isEqualTo(FAILED_INDEX - 1);

    final ArcadeStateMachine restarted = new ArcadeStateMachine();
    final RaftStorage reopened = newStorage(raftDirectory, RaftStorage.StartupOption.RECOVER);
    try {
      restarted.setServer(serverWhoseUsersFileFailsOnceDurability(new AtomicBoolean(true), databaseDirectory, usersFile));
      restarted.initialize(stubServerDurability(), RaftGroupId.valueOf(UUID.randomUUID()), reopened);
      final long replayFrom = restarted.readAppliedIndexCounter();
      assertThat(replayFrom).isEqualTo(FAILED_INDEX - 1);

      // What Ratis feeds the restarted state machine: the retained entries above the marker, in order
      restarted.applyTransaction(securityUsersEntryDurability(restarted, FAILED_INDEX));
      restarted.applyTransaction(securityGroupsEntry(restarted, LATER_INDEX));

      assertThat(usersFile).as("the replay wrote the list the failed apply could not").exists();
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).isEqualTo(USERS_JSON);
      assertThat(restarted.takeSnapshot()).as("durable now, so the checkpoint covers everything again").isEqualTo(LATER_INDEX);
    } finally {
      restarted.close();
      reopened.close();
    }
  }

  // Regression for #7752: the floor clears once the document is durable again
  /**
   * Without a restart: as soon as the document is durable again - here reported by the next security entry, in
   * production also by the periodic retry - the floor clears and checkpoints resume, so the Raft log is pinned only for
   * as long as the volume is broken.
   */
  @Test
  void theFloorClearsOnceTheDocumentIsDurableAgain(@TempDir final Path raftDirectory,
      @TempDir final Path databaseDirectory, @TempDir final Path usersDirectory) throws Exception {
    final AtomicBoolean usersDurable = new AtomicBoolean(true);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftStorage storage = newStorage(raftDirectory, RaftStorage.StartupOption.FORMAT);
    try {
      final FakeServerSecurity security = securityWithUsersFailingOnce(new AtomicBoolean(), usersDirectory.resolve("u"),
          usersDurable);
      sm.setServer(serverWith(security, databaseDirectory));
      sm.initialize(stubServerDurability(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

      sm.applyTransaction(securityUsersEntryDurability(sm, FAILED_INDEX));
      sm.applyTransaction(securityGroupsEntry(sm, LATER_INDEX));
      assertThat(sm.takeSnapshot()).isEqualTo(FAILED_INDEX - 1);

      // The volume was fixed and the list written again
      usersDurable.set(true);
      sm.applyTransaction(securityGroupsEntry(sm, LATER_INDEX + 1));
      assertThat(sm.takeSnapshot()).isEqualTo(LATER_INDEX + 1);
    } finally {
      sm.close();
      storage.close();
    }
  }

  // Regression for #7752: an install past the pin drops it and asks for the documents again
  /**
   * A snapshot install moves past the pinned entry, which is then no longer replayable: the pin is dropped, or no
   * checkpoint below the install's marker could ever be taken again, and the install asks for the documents to be
   * re-sent as new entries instead.
   */
  @Test
  void anInstallPastThePinDropsItAndAsksForTheDocumentsAgain() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("isReplicatedDocumentDurable", args -> false);

    final SecurityDurabilityFloor floor = new SecurityDurabilityFloor();
    assertThat(floor.dropCoveredByInstall(100L)).as("nothing pinned, nothing to ask for").isFalse();

    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, FAILED_INDEX, security);
    assertThat(floor.floor()).isEqualTo(FAILED_INDEX);
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, LATER_INDEX, security);
    assertThat(floor.floor()).as("the FIRST undurable entry is the one a replay has to start from").isEqualTo(FAILED_INDEX);

    assertThat(floor.dropCoveredByInstall(FAILED_INDEX - 1)).as("an install below the pin leaves it in place").isFalse();
    assertThat(floor.floor()).isEqualTo(FAILED_INDEX);

    assertThat(floor.dropCoveredByInstall(100L)).isTrue();
    assertThat(floor.floor()).isEqualTo(Long.MAX_VALUE);
  }

  // Regression for #7752: the periodic retry lifts the pin once the write succeeds
  /**
   * The periodic retry, on an injected clock: nothing is written again before the interval, the write is retried once it
   * has passed, and the pin lifts as soon as the retried write reaches the disk - without any further security entry.
   */
  @Test
  void thePeriodicRetryLiftsThePinOnceTheWriteSucceeds() {
    final AtomicLong now = new AtomicLong();
    final AtomicBoolean durable = new AtomicBoolean(false);
    final AtomicBoolean volumeFixed = new AtomicBoolean(false);
    final AtomicInteger retries = new AtomicInteger();
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("isReplicatedDocumentDurable", args -> durable.get());
    security.on("retryReplicatedSecurityPersistence", args -> {
      retries.incrementAndGet();
      durable.set(volumeFixed.get());
      return durable.get();
    });

    final SecurityDurabilityFloor floor = new SecurityDurabilityFloor(now::get);
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, FAILED_INDEX, security);
    assertThat(floor.floor()).isEqualTo(FAILED_INDEX);

    final long interval = TimeUnit.MILLISECONDS.toNanos(SecurityDurabilityFloor.RETRY_INTERVAL_MS);
    now.addAndGet(interval - 1);
    floor.retryIfDue(security);
    assertThat(retries).as("not before the interval: the write that failed was the first attempt").hasValue(0);

    now.addAndGet(1);
    floor.retryIfDue(security);
    assertThat(retries).hasValue(1);
    assertThat(floor.floor()).as("the volume is still broken").isEqualTo(FAILED_INDEX);

    floor.retryIfDue(security);
    assertThat(retries).as("paced: one attempt per interval, however many entries are applied").hasValue(1);

    volumeFixed.set(true);
    now.addAndGet(interval);
    floor.retryIfDue(security);
    assertThat(retries).hasValue(2);
    assertThat(floor.floor()).as("durable again, so checkpoints may cover the entry").isEqualTo(Long.MAX_VALUE);
  }

  // Regression for #7752: each document is pinned on its own and the floor is the lowest
  /** Each document is pinned on its own: the floor is the lowest pin, and a document that recovers releases only its own. */
  @Test
  void eachDocumentIsPinnedOnItsOwnAndTheFloorIsTheLowest() {
    final AtomicBoolean usersDurable = new AtomicBoolean(false);
    final AtomicBoolean groupsDurable = new AtomicBoolean(false);
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("isReplicatedDocumentDurable", args -> ReplicatedSecurityFingerprintRepository.USERS.equals(args[0]) ?
        usersDurable.get() :
        !ReplicatedSecurityFingerprintRepository.GROUPS.equals(args[0]) || groupsDurable.get());

    final SecurityDurabilityFloor floor = new SecurityDurabilityFloor();
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.GROUPS, LATER_INDEX, security);
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, LATER_INDEX + 1, security);
    assertThat(floor.floor()).isEqualTo(LATER_INDEX);

    // An API-token entry that is durable pins nothing and changes nothing
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.API_TOKENS, LATER_INDEX + 2, security);
    assertThat(floor.floor()).isEqualTo(LATER_INDEX);

    groupsDurable.set(true);
    floor.refresh(security);
    assertThat(floor.floor()).as("the users pin still holds").isEqualTo(LATER_INDEX + 1);

    usersDurable.set(true);
    floor.refresh(security);
    assertThat(floor.floor()).isEqualTo(Long.MAX_VALUE);
  }

  // Regression for #7752: the forced seed survives an attempt that did not make it
  /**
   * The re-seed an install past a pin asks for survives the attempts that do not make it: one taken by an attempt that
   * then failed - transiently, or with documents the leader could not seed - is put back for the next one, and only an
   * attempt that made it spends it. Without that the stale files would outlive the install for good.
   */
  @Test
  void theForcedSeedSurvivesAnAttemptThatDidNotMakeIt() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      assertThat(catchUp.isForcedSeedPending()).isFalse();

      // No server to ask through: nothing is queued, but the forced seed is recorded for the next trigger
      catchUp.afterSnapshotInstall(null, null, true);
      assertThat(catchUp.isForcedSeedPending()).isTrue();

      final boolean taken = catchUp.takeForcedSeed();
      assertThat(taken).isTrue();
      assertThat(catchUp.isForcedSeedPending()).as("taken by the attempt asking the leader").isFalse();

      catchUp.forcedSeedNotMade(taken);
      assertThat(catchUp.isForcedSeedPending()).as("put back after an attempt that did not make it").isTrue();

      assertThat(catchUp.takeForcedSeed()).isTrue();
      assertThat(catchUp.isForcedSeedPending()).as("spent by the attempt that made it").isFalse();

      catchUp.forcedSeedNotMade(false);
      assertThat(catchUp.isForcedSeedPending()).as("an attempt that took nothing puts nothing back").isFalse();
    }
  }

  // Regression for #7752: a document that fails again after recovering is pinned at the new entry
  /**
   * A document pinned, released when durable, then pinned again by a later failure is pinned at the LATER entry: the
   * earlier one is durable and needs no replay, so holding the checkpoint back to it would only grow the log.
   */
  @Test
  void aDocumentThatFailsAgainAfterRecoveringIsPinnedAtTheNewEntry() {
    final AtomicBoolean durable = new AtomicBoolean(false);
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("isReplicatedDocumentDurable", args -> durable.get());

    final SecurityDurabilityFloor floor = new SecurityDurabilityFloor();
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, FAILED_INDEX, security);
    assertThat(floor.floor()).isEqualTo(FAILED_INDEX);

    durable.set(true);
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, LATER_INDEX, security);
    assertThat(floor.floor()).as("durable again").isEqualTo(Long.MAX_VALUE);

    durable.set(false);
    floor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, LATER_INDEX + 5, security);
    assertThat(floor.floor()).isEqualTo(LATER_INDEX + 5);
  }

  // Regression for #7752: a forced seed the leader could not commit is retried
  /**
   * A forced seed the leader could not fully commit is retried like a transient failure, so the catch-up's bounded
   * backoff asks again instead of the node waiting, with its stale files, for a trigger a quiet cluster may never send.
   * An ordinary catch-up with failed seeds stays answered: retrying it would only resubmit what the leader reported.
   */
  @Test
  void aForcedSeedTheLeaderCouldNotCommitIsRetried() {
    assertThat(SecurityCatchUp.outcomeOfAnswer(true, List.of("users"))).isEqualTo(SecurityCatchUp.Outcome.TRANSIENT_FAILURE);
    assertThat(SecurityCatchUp.outcomeOfAnswer(true, List.of())).isEqualTo(SecurityCatchUp.Outcome.ASKED);
    assertThat(SecurityCatchUp.outcomeOfAnswer(false, List.of("users"))).isEqualTo(SecurityCatchUp.Outcome.ASKED);
    assertThat(SecurityCatchUp.outcomeOfAnswer(false, List.of())).isEqualTo(SecurityCatchUp.Outcome.ASKED);
  }

  // Regression for #7752: the forced seed survives a restart
  /**
   * The forced re-seed survives a restart: kept in a marker beside the snapshot markers, restored by the next start, and
   * removed only once a forced seed fully committed. A restart inside the seed's round trip used to forget it, leaving
   * the stale files with nothing to replay them and a catch-up that compares documents only.
   */
  @Test
  void theForcedSeedSurvivesARestart(@TempDir final Path stateMachineDirectory) {
    final Path marker = stateMachineDirectory.resolve(SecurityCatchUp.FORCED_SEED_MARKER);
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.keepForcedSeedIn(stateMachineDirectory.toFile());
      catchUp.afterSnapshotInstall(null, null, true);
      assertThat(marker).exists();
    }

    try (final SecurityCatchUp restarted = new SecurityCatchUp()) {
      restarted.keepForcedSeedIn(stateMachineDirectory.toFile());
      assertThat(restarted.isForcedSeedPending()).as("restored from the marker").isTrue();

      final boolean taken = restarted.takeForcedSeed();
      restarted.forcedSeedNotMade(taken);
      assertThat(marker).as("an attempt that did not make it keeps the marker").exists();

      restarted.forcedSeedMade(restarted.takeForcedSeed());
      assertThat(marker).as("spent once a forced seed fully committed").doesNotExist();
    }

    try (final SecurityCatchUp again = new SecurityCatchUp()) {
      again.keepForcedSeedIn(stateMachineDirectory.toFile());
      assertThat(again.isForcedSeedPending()).isFalse();
    }
  }

  // Regression for #7752: a completed forced seed does not delete the marker of one re armed meanwhile
  /**
   * An install that re-arms the forced seed while an earlier forced seed is completing keeps its marker: completing the
   * old one must not delete the record of the new one, or a restart would forget the seed the new install owes.
   */
  @Test
  void aCompletedForcedSeedDoesNotDeleteTheMarkerOfOneReArmedMeanwhile(@TempDir final Path stateMachineDirectory) {
    final Path marker = stateMachineDirectory.resolve(SecurityCatchUp.FORCED_SEED_MARKER);
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.keepForcedSeedIn(stateMachineDirectory.toFile());
      catchUp.afterSnapshotInstall(null, null, true);

      final boolean taken = catchUp.takeForcedSeed();
      // A second install covers another undurable entry while the first seed is in flight
      catchUp.afterSnapshotInstall(null, null, true);
      catchUp.forcedSeedMade(taken);

      assertThat(catchUp.isForcedSeedPending()).isTrue();
      assertThat(marker).as("the re-armed seed is still recorded for a restart").exists();
    }
  }

  private static TransactionContext securityUsersEntryDurability(final ArcadeStateMachine sm, final long index) {
    return entry(sm, index, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON));
  }

  private static TransactionContext securityGroupsEntry(final ArcadeStateMachine sm, final long index) {
    return entry(sm, index, RaftLogEntryCodec.encodeSecurityGroupsEntry("{\"databases\":{},\"version\":1}"));
  }

  private static TransactionContext entry(final ArcadeStateMachine sm, final long index, final ByteString payload) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  private static ArcadeDBServer serverWhoseUsersFileFailsOnceDurability(final AtomicBoolean firstWriteFailed,
      final Path databaseDirectory, final Path usersFile) {
    return serverWith(securityWithUsersFailingOnce(firstWriteFailed, usersFile, new AtomicBoolean(true)), databaseDirectory);
  }

  /**
   * A security store whose users file fails the FIRST write and takes every later one, reporting its durability the
   * way the real one does: not durable from the failed write until a write succeeds. The group document is accepted and
   * always durable, so it moves the applied position without repairing anything.
   */
  private static FakeServerSecurity securityWithUsersFailingOnce(final AtomicBoolean firstWriteFailed,
      final Path usersFile, final AtomicBoolean usersDurable) {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("applyReplicatedUsers", args -> {
      if (firstWriteFailed.compareAndSet(false, true)) {
        usersDurable.set(false);
        throw new ReplicatedUsersPersistenceException("Replicated users applied in memory but could NOT be persisted",
            new IOException("No space left on device"));
      }
      try {
        Files.writeString(usersFile, (String) args[0], StandardCharsets.UTF_8);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      usersDurable.set(true);
      return null;
    });
    security.on("applyReplicatedGroups", args -> null);
    security.on("isReplicatedDocumentDurable",
        args -> !ReplicatedSecurityFingerprintRepository.USERS.equals(args[0]) || usersDurable.get());
    security.on("retryReplicatedSecurityPersistence", args -> usersDurable.get());
    return security;
  }

  private static ArcadeDBServer serverWith(final FakeServerSecurity security, final Path databaseDirectory) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    server.security(security);
    return server;
  }

  private static RaftStorage newStorage(final Path dir, final RaftStorage.StartupOption option) throws IOException {
    return RaftStorage.newBuilder().setDirectory(dir.toFile()).setOption(option).build();
  }

  /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubServerDurability() {
    return (RaftServer) Proxy.newProxyInstance(SecurityEntryApplyFailureTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
