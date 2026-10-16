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
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
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

/**
 * Issue #7752: a security entry whose effects did not reach this node's disk stays in the Raft log until they do, so a
 * restart replays it instead of reading back a document - or a compare-and-set baseline - the cluster has moved past.
 * <p>
 * The incident every test drives: a {@code SECURITY_USERS_ENTRY} at {@link #FAILED_INDEX} whose local write fails, then
 * an unrelated {@code SECURITY_GROUPS_ENTRY} at {@link #LATER_INDEX} that applies normally and carries the applied
 * position past it. That second entry is what used to let a snapshot compact the first one away (issue #7227): it is not
 * a users entry, so nothing about it repairs the users document. Issue #7252 pinned that outcome as the behaviour; this
 * pins the replacement. The failure is reported through {@code ServerSecurity.isReplicatedDocumentDurable}, answered
 * here from the same flag the fake's write flips, so the test exercises the state machine's reading of it rather than
 * the file layer, which {@code Issue7752ReplicatedSecurityReplayTest} in the {@code server} module covers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7752SecurityDurabilityFloorTest {

  private static final String USERS_JSON   = "[{\"name\":\"root\",\"password\":\"x\"}]";
  private static final long   FAILED_INDEX = 5L;
  private static final long   LATER_INDEX  = 6L;

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
      sm.setServer(serverWhoseUsersFileFailsOnce(new AtomicBoolean(), databaseDirectory, usersFile));
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

      assertThat(sm.applyTransaction(securityUsersEntry(sm, FAILED_INDEX)).isCompletedExceptionally())
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
      restarted.setServer(serverWhoseUsersFileFailsOnce(new AtomicBoolean(true), databaseDirectory, usersFile));
      restarted.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), reopened);
      final long replayFrom = restarted.readAppliedIndexCounter();
      assertThat(replayFrom).isEqualTo(FAILED_INDEX - 1);

      // What Ratis feeds the restarted state machine: the retained entries above the marker, in order
      restarted.applyTransaction(securityUsersEntry(restarted, FAILED_INDEX));
      restarted.applyTransaction(securityGroupsEntry(restarted, LATER_INDEX));

      assertThat(usersFile).as("the replay wrote the list the failed apply could not").exists();
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).isEqualTo(USERS_JSON);
      assertThat(restarted.takeSnapshot()).as("durable now, so the checkpoint covers everything again").isEqualTo(LATER_INDEX);
    } finally {
      restarted.close();
      reopened.close();
    }
  }

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
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

      sm.applyTransaction(securityUsersEntry(sm, FAILED_INDEX));
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

  private static TransactionContext securityUsersEntry(final ArcadeStateMachine sm, final long index) {
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

  private static ArcadeDBServer serverWhoseUsersFileFailsOnce(final AtomicBoolean firstWriteFailed,
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
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(Issue7752SecurityDurabilityFloorTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
