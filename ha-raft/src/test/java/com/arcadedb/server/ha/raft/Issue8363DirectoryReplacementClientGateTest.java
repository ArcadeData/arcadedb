/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8363 (consolidating #7959 and #7960 part 2): a client already connected to a node, or
 * dialling it directly, was served from a database whose directory was being replaced from the leader's snapshot.
 * <p>
 * The readiness gate of #7519 only stops an orchestrator routing NEW connections to the node, and only for the
 * first-formation bootstrap. The node-wide {@code snapshotInstallInProgress} 503 opens only around the file swap at
 * the end of an install, and only on HTTP. For the whole download - the long part, with the live copy deliberately
 * left open - a Bolt session, a Postgres connection, a gRPC channel or a pinned HTTP client read the copy the
 * cluster had decided to discard, and so did every client of the two other paths that replace a directory the same
 * way: the operator resync and the leader-driven full resync.
 * <p>
 * {@code RaftReplicatedDatabase} now refuses a client request on such a database with a {@link NeedRetryException},
 * and tells a client from the engine's own threads by {@link ProtocolContext}. These tests pin both halves: every
 * client entry point is refused, and nothing the engine does is.
 */
class Issue8363DirectoryReplacementClientGateTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-8363-replacement-gate";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;

  private LocalDatabase          localDb;
  private RaftReplicatedDatabase replicated;
  private ArcadeStateMachine     stateMachine;
  private RID                    seedRid;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    localDb.getSchema().createDocumentType("Seed");
    localDb.transaction(() -> seedRid = localDb.newDocument("Seed").set("k", 1).save().getIdentity());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration());
    stateMachine = mock(ArcadeStateMachine.class);
    final RaftHAServer raft = mock(RaftHAServer.class);
    // The leader, so a read-only command executes here rather than being forwarded: the gate must hold on the
    // local path, which is the one that reads the copy on disk.
    when(raft.isLeader()).thenReturn(true);
    when(raft.getStateMachine()).thenReturn(stateMachine);
    replicated = new RaftReplicatedDatabase(server, localDb, raft);
    // As a server database runs: a scan with no transaction open begins one, through the wrapper.
    localDb.setAutoTransaction(true);
  }

  @AfterEach
  void tearDown() {
    ProtocolContext.clear();
    SnapshotInstaller.clearInstallInFlightForTesting(Path.of(DB_PATH));
    if (localDb != null && localDb.isOpen()) {
      if (localDb.isTransactionActive())
        localDb.rollbackAllNested();
      localDb.close();
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
  }

  private static ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    // Fail any download on the first attempt: there is no leader to pull from in a unit test.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);
    return config;
  }

  /** Every way a wire protocol reads or writes through the wrapper, by name so a failure says which one leaked. */
  private Map<String, Executable> clientEntryPoints() {
    final Map<String, Executable> calls = new LinkedHashMap<>();
    calls.put("query(sql)", () -> close(replicated.query("sql", "select from Seed")));
    calls.put("query(sql, varargs)", () -> close(replicated.query("sql", "select from Seed where k = ?", 1)));
    calls.put("query(sql, map)", () -> close(replicated.query("sql", "select from Seed where k = :k", Map.of("k", 1))));
    calls.put("command(sql)", () -> close(replicated.command("sql", "select from Seed")));
    calls.put("command(sql, map)", () -> close(replicated.command("sql", "select from Seed", Map.of())));
    calls.put("begin()", () -> replicated.begin());
    calls.put("lookupByRID", () -> replicated.lookupByRID(seedRid, true));
    calls.put("existsRecord", () -> replicated.existsRecord(seedRid));
    calls.put("countType", () -> replicated.countType("Seed", true));
    calls.put("iterateType", () -> replicated.iterateType("Seed", true));
    calls.put("scanType", () -> replicated.scanType("Seed", true, doc -> true));
    calls.put("async()", () -> replicated.async());
    return calls;
  }

  private static void close(final ResultSet resultSet) {
    while (resultSet.hasNext())
      resultSet.next();
    resultSet.close();
  }

  private void assertEveryClientEntryPointIsRefused(final String protocol) {
    ProtocolContext.set(protocol);
    try {
      clientEntryPoints().forEach((name, call) -> assertThatThrownBy(call::execute)
          .as("a %s client's %s on a database being replaced", protocol, name)
          .isInstanceOf(NeedRetryException.class)
          .hasMessageContaining(DB_NAME)
          .hasMessageContaining("being replaced"));
    } finally {
      ProtocolContext.clear();
    }
  }

  private void assertEveryClientEntryPointIsServed(final String protocol) {
    ProtocolContext.set(protocol);
    try {
      clientEntryPoints().forEach((name, call) -> {
        assertThatNoException().as("%s's %s", protocol, name).isThrownBy(call::execute);
        if (localDb.isTransactionActive())
          localDb.rollback();
      });
    } finally {
      ProtocolContext.clear();
    }
  }

  @Test
  void aClientIsServedWhenNothingIsBeingReplaced() {
    assertEveryClientEntryPointIsServed("bolt");
  }

  /**
   * The download of any install - bootstrap, operator resync, leader-driven full resync, reconciler, forced snapshot:
   * all of them go through {@code SnapshotInstaller.install}, which registers the directory before downloading.
   */
  @Test
  void everyClientEntryPointIsRefusedWhileAnInstallReplacesTheDirectory() {
    SnapshotInstaller.markInstallInFlightForTesting(Path.of(DB_PATH));

    for (final String protocol : new String[] { "http", "bolt", "postgres", "grpc", "mongo", "redis", "ws" })
      assertEveryClientEntryPointIsRefused(protocol);

    // A transaction block - what the HTTP handlers run every command in - begins through the wrapper too.
    ProtocolContext.set("http");
    try {
      assertThatThrownBy(() -> replicated.transaction(() -> replicated.countType("Seed", true), false, 1))
          .isInstanceOf(NeedRetryException.class).hasMessageContaining("being replaced");
      assertThat(replicated.isTransactionActive()).isFalse();
    } finally {
      ProtocolContext.clear();
    }

    SnapshotInstaller.clearInstallInFlightForTesting(Path.of(DB_PATH));
    assertEveryClientEntryPointIsServed("postgres");
  }

  /** Between a failed bootstrap download and its retry no install is registered, but the holder still is. */
  @Test
  void everyClientEntryPointIsRefusedWhileTheBootstrapHoldsTheDatabase() {
    when(stateMachine.isBootstrapInstallInFlight(DB_NAME)).thenReturn(true);
    assertEveryClientEntryPointIsRefused("bolt");

    when(stateMachine.isBootstrapInstallInFlight(DB_NAME)).thenReturn(false);
    assertEveryClientEntryPointIsServed("bolt");
  }

  /** The engine's own threads go through the same wrapper and must never be refused: that is a divergence. */
  @Test
  void theEngineIsNeverRefused() {
    SnapshotInstaller.markInstallInFlightForTesting(Path.of(DB_PATH));
    when(stateMachine.isBootstrapInstallInFlight(DB_NAME)).thenReturn(true);

    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
    assertEveryClientEntryPointIsServed(ProtocolContext.INTERNAL);
  }

  /** Per database, not per node: widening it node-wide would refuse every database for a whole resync. */
  @Test
  void anotherDatabaseBeingReplacedDoesNotRefuseThisOne() {
    final Path other = Path.of(DB_DIR, "another-database-8363");
    SnapshotInstaller.markInstallInFlightForTesting(other);
    when(stateMachine.isBootstrapInstallInFlight("another-database-8363")).thenReturn(true);
    try {
      assertEveryClientEntryPointIsServed("http");
    } finally {
      SnapshotInstaller.clearInstallInFlightForTesting(other);
    }
  }

  /**
   * A transaction begun before the replacement started read the copy being discarded and computed its page deltas
   * against it: its commit is refused, and the transaction is rolled back rather than left open behind the refusal.
   */
  @Test
  void aCommitStartedBeforeTheReplacementIsRefusedAndRolledBack() {
    ProtocolContext.set("postgres");
    try {
      replicated.begin();
      replicated.newDocument("Seed").set("k", 2).save();

      SnapshotInstaller.markInstallInFlightForTesting(Path.of(DB_PATH));

      assertThatThrownBy(replicated::commit).isInstanceOf(NeedRetryException.class).hasMessageContaining("being replaced");
      assertThat(replicated.isTransactionActive()).as("the refused transaction is rolled back").isFalse();
    } finally {
      ProtocolContext.clear();
    }
    assertThat(localDb.countType("Seed", true)).as("nothing of the refused transaction was published").isEqualTo(1);
  }

  /**
   * The install is the engine's work even when a client request drives it: the operator resync runs it on the HTTP
   * worker that received {@code POST /api/v1/cluster/resync}. Left tagged, the install would be refused by the very
   * gate its registration opens - the reopen at the end of its swap included. Observed from inside the install and
   * after it returns.
   */
  @Test
  void anInstallDrivenFromAClientThreadRunsAsTheEngineAndHandsTheTagBack() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    // Observed from inside the install, where it reads its retry settings - after it has registered the directory.
    final ContextConfiguration config = configuration();
    final List<String> protocolsInsideTheInstall = new CopyOnWriteArrayList<>();
    final List<Boolean> registeredInsideTheInstall = new CopyOnWriteArrayList<>();
    when(server.getConfiguration()).thenAnswer(invocation -> {
      protocolsInsideTheInstall.add(ProtocolContext.get());
      registeredInsideTheInstall.add(SnapshotInstaller.isInstallInFlight(DB_PATH));
      return config;
    });

    ProtocolContext.set("http");
    try {
      assertThatThrownBy(() -> SnapshotInstaller.install(DB_NAME, DB_PATH, () -> null, () -> null, null, server))
          .as("no leader to download from").isInstanceOf(IOException.class);
      assertThat(ProtocolContext.get()).as("the caller's request tag is handed back").isEqualTo("http");
    } finally {
      ProtocolContext.clear();
    }

    assertThat(protocolsInsideTheInstall).as("the install runs as the engine").isNotEmpty()
        .containsOnly(ProtocolContext.INTERNAL);
    assertThat(registeredInsideTheInstall).as("the directory is registered before the download").containsOnly(true);
    assertThat(SnapshotInstaller.isInstallInFlight(DB_PATH)).as("and released after it").isFalse();
  }

  /**
   * The whole chain through a real entry point: the bootstrap apply path's install, observed from inside. A client
   * thread reading the database at that moment is refused; the install's own thread reading it is not.
   */
  @Test
  void aClientReadingDuringARealBootstrapReinstallIsRefusedAndTheInstallIsNot() throws Exception {
    final ArcadeStateMachine realStateMachine = new ArcadeStateMachine();
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.getStateMachine()).thenReturn(realStateMachine);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration());
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    final RaftReplicatedDatabase wrapper = new RaftReplicatedDatabase(server, localDb, raft);
    when(server.getDatabase(DB_NAME)).thenReturn(new ServerDatabase(null, localDb));

    final AtomicReference<Throwable> clientOutcome = new AtomicReference<>();
    final AtomicReference<Throwable> installThreadOutcome = new AtomicReference<>();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
      final Thread client = new Thread(() -> {
        ProtocolContext.set("postgres");
        try {
          close(wrapper.query("sql", "select from Seed"));
        } catch (final Throwable t) {
          clientOutcome.set(t);
        } finally {
          ProtocolContext.clear();
        }
      });
      client.start();
      client.join();
      try {
        close(wrapper.query("sql", "select from Seed"));
      } catch (final Throwable t) {
        installThreadOutcome.set(t);
      }
      return null;
    });
    realStateMachine.setServer(server);

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), Long.MAX_VALUE);
    assertThatNoException().isThrownBy(
        () -> realStateMachine.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encoded), 7L));

    assertThat(clientOutcome.get()).as("a client reading the copy the baseline decided against")
        .isInstanceOf(NeedRetryException.class).hasMessageContaining(DB_NAME);
    assertThat(installThreadOutcome.get()).as("the install's own thread").isNull();

    // The failed download schedules a retry on the lifecycle executor: let it finish before teardown deletes the
    // directory under it. The holder itself outlives it (issue #8367): nothing here replaces the rejected copy.
    realStateMachine.awaitLifecycleTasksForTesting(30_000);
    assertThat(realStateMachine.isBootstrapInstallInFlight(DB_NAME))
        .as("a client is still refused after the retry failed as well: the copy on disk is still the rejected one")
        .isTrue();
  }
}
