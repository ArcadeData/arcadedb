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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #8651: the #8577 skip only covered entries that name a database, so a stale replayed
 * entry that Ratis applies through {@code notifyTermIndexUpdated} (its own metadata and configuration entries), or
 * one that names no database (the security entries), still regressed the applied position below the boundary a
 * leader-driven install had registered. The Ratis monotonic check then killed the {@code StateMachineUpdater}
 * thread and left the node a zombie. Driven against a bare {@link ArcadeStateMachine}, like
 * {@link Issue8577StaleEntryAfterInstallBoundaryTest}.
 */
class Issue8651StaleEntryAfterRaftInstallBoundaryTest {

  @Test
  void aStaleMetadataEntryBelowTheInstallBoundaryLeavesTheAppliedPositionAlone() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.recordInstalledRaftBoundary(1867073L);
    sm.notifyTermIndexUpdated(30L, 1867073L);

    // The exact shape from the chaos run: Ratis applies its own metadata entry from the pre-install local log.
    sm.notifyTermIndexUpdated(30L, 1835784L);

    assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(30L, 1867073L));
  }

  @Test
  void anEntryAboveTheInstallBoundaryStillAdvancesTheAppliedPosition() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.recordInstalledRaftBoundary(1867073L);
    sm.notifyTermIndexUpdated(30L, 1867073L);

    sm.notifyTermIndexUpdated(30L, 1867074L);

    assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(30L, 1867074L));
  }

  @Test
  void aRegressionWithNoRecordedBoundaryStillFailsLoudly() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.notifyTermIndexUpdated(30L, 1867073L);

    assertThatThrownBy(() -> sm.notifyTermIndexUpdated(30L, 1835784L))
        .isInstanceOf(IllegalStateException.class);
  }

  @ParameterizedTest
  @EnumSource(value = RaftLogEntryType.class, names = { "SECURITY_USERS_ENTRY", "SECURITY_GROUPS_ENTRY",
      "SECURITY_API_TOKENS_ENTRY" })
  void aStaleEntryThatNamesNoDatabaseIsSkippedAsANoOp(final RaftLogEntryType type) throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.recordInstalledRaftBoundary(1867073L);
    sm.notifyTermIndexUpdated(30L, 1867073L);
    final long appliedBefore = sm.readAppliedIndexCounter();
    final long persistedBefore = sm.readPersistedAppliedIndex();

    final CompletableFuture<Message> result = sm.applyTransaction(entry(sm, securityPayload(type), 30L, 1835784L));

    assertThat(result.isCompletedExceptionally()).as("a stale security entry must not throw or halt").isFalse();
    assertThat(result.get().getContent().toStringUtf8()).isEqualTo("OK");
    assertThat(sm.readAppliedIndexCounter()).as("nothing moves the applied counter backward").isEqualTo(appliedBefore);
    assertThat(sm.getLastAppliedTermIndex()).as("nor the Ratis applied position")
        .isEqualTo(TermIndex.valueOf(30L, 1867073L));
    assertThat(sm.readPersistedAppliedIndex()).as("nor the persisted global position").isEqualTo(persistedBefore);
  }

  /**
   * A database the install did not cover (not installed, missing on the leader, created since) has no boundary on its
   * gate, so its stale entry is applied to it - but the global positions belong to the install and must not move
   * backward; only that database's own applied position is recorded.
   */
  @Test
  void aStaleEntryForADatabaseTheInstallDidNotCoverLeavesTheGlobalPositionsAlone(@TempDir final Path serverDir)
      throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    final ArcadeDBServer server = new ArcadeDBServer(config);
    final String dbPath = serverDir.resolve("uncovered").toString();
    final LocalDatabase db = (LocalDatabase) new DatabaseFactory(dbPath).create();
    try {
      server.registerDatabase("uncovered", db);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(server);
      final ByteString bootstrap = RaftLogEntryCodec.encodeBootstrapFingerprintEntry("uncovered",
          BootstrapFingerprint.compute(new File(dbPath)), db.getLastTransactionId());

      // The node applied up to 120, then an install registered 100 without touching 'uncovered'.
      assertThat(sm.applyTransaction(entry(sm, bootstrap, 30L, 120L)).isCompletedExceptionally()).isFalse();
      sm.recordInstalledRaftBoundary(100L);

      // A replayed entry for that database, below the boundary.
      final CompletableFuture<Message> stale = sm.applyTransaction(entry(sm, bootstrap, 30L, 50L));

      assertThat(stale.isCompletedExceptionally()).isFalse();
      assertThat(sm.readAppliedIndexCounter()).as("the global counter must not move backward").isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex()).as("the persisted global position must not move backward")
          .isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex("uncovered")).as("the database's own position is recorded")
          .isEqualTo(50L);

      // A stale DROP below the boundary, through applyTransaction: it evicts that database's own entry and still
      // leaves the global positions alone.
      sm.writePersistedDatabaseAppliedIndex(40L, "gone", false);
      final CompletableFuture<Message> drop = sm.applyTransaction(
          entry(sm, RaftLogEntryCodec.encodeDropDatabaseEntry("gone"), 30L, 60L));
      assertThat(drop.isCompletedExceptionally()).isFalse();
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex("gone")).as("evicted by the drop").isEqualTo(-1L);
    } finally {
      db.close();
    }
  }

  @Test
  void theBoundaryCoversTheInstalledIndexAndEverythingBelowIt() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.isBelowInstalledRaftBoundary(1L)).as("no install recorded, nothing is covered").isFalse();

    sm.recordInstalledRaftBoundary(100L);
    sm.recordInstalledRaftBoundary(50L);

    assertThat(sm.isBelowInstalledRaftBoundary(99L)).isTrue();
    assertThat(sm.isBelowInstalledRaftBoundary(100L)).as("the boundary index itself is covered").isTrue();
    assertThat(sm.isBelowInstalledRaftBoundary(101L)).isFalse();
  }

  @Test
  void aClosingDivisionOverridesTheRunningProxyState() {
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.CLOSING))
        .as("a division its dying updater closed must not read as RUNNING").isTrue();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.CLOSED)).isTrue();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.EXCEPTION)).isTrue();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.RUNNING)).isFalse();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.STARTING)).isFalse();
  }

  @Test
  void aClosingDivisionIsReportedByTheHealthMonitorOnlyWhenItPersists() {
    final HealthMonitorTest.FakeHealthTarget fake = new HealthMonitorTest.FakeHealthTarget();
    final HealthMonitor monitor = new HealthMonitor(fake, 0);

    fake.state.set(LifeCycle.State.CLOSING);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("a division that is closing right now may be a normal shutdown").isZero();

    fake.state.set(LifeCycle.State.RUNNING);
    monitor.tick();
    fake.state.set(LifeCycle.State.CLOSING);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("a healthy tick in between resets the streak").isZero();

    monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("a division stuck closing across ticks is recovered like a CLOSED one")
        .isEqualTo(1);
  }

  private static ByteString securityPayload(final RaftLogEntryType type) {
    return switch (type) {
      case SECURITY_USERS_ENTRY -> RaftLogEntryCodec.encodeSecurityUsersEntry("{}");
      case SECURITY_GROUPS_ENTRY -> RaftLogEntryCodec.encodeSecurityGroupsEntry("{}");
      case SECURITY_API_TOKENS_ENTRY -> RaftLogEntryCodec.encodeSecurityApiTokensEntry("{}");
      default -> throw new IllegalArgumentException(type.name());
    };
  }

  private static TransactionContext entry(final ArcadeStateMachine sm, final ByteString payload, final long term,
      final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
