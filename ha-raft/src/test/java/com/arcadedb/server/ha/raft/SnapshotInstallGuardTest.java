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
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards and bookkeeping of the follower-side snapshot install driven by the state machine: self-install refusal, HA server teardown during resolution, divergence marks kept for databases the install did not replace.
 */
class SnapshotInstallGuardTest {

  // ------------------------------------------------------------------------------------------
  // Issue #6202
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #6202: {@code notifyInstallSnapshotFromLeader} - the Ratis-initiated resync, the one
   * that runs when a follower's log is behind the leader's compacted log - resolved the leader's HTTP address and
   * dialled it with no check at all, while the manual {@code triggerSnapshotDownload()} refused twice before
   * dialling the very same kind of address.
   * <p>
   * The address is only as good as the configuration behind it. With no {@code http} port declared in
   * {@code arcadedb.ha.serverList} a peer's HTTP endpoint is derived from the peer's Raft host plus THIS node's HTTP
   * port, so on a cluster whose nodes share a host every peer collapses onto this node's own endpoint, and on one
   * with mixed ports it can name the wrong peer. Neither outcome reported an error: the reconcile succeeded, the
   * install was recorded, the stale-read floor was dropped and the node returned to the ready set carrying whatever
   * it had copied - a node "repaired" from its own incomplete databases, or from a peer that is itself behind.
   * <p>
   * Both paths now ask the same helper, so they cannot drift apart, and the helper also refuses an address that
   * does not identify one peer - the ambiguity check the client routing tables make (#6183), which matters more
   * here, because a confidently wrong routing address costs one redirect while a confidently wrong snapshot source
   * cannot be undone.
   *
   */

  private static final String LEADER_PEER_ID    = "peer-b_2434";

  private static final String LOCAL_HTTP        = "localhost:2480";

  private static final long   FIRST_LOG_INDEX   = 500L;

  private static final long   SNAPSHOT_INDEX    = FIRST_LOG_INDEX - 1;

  private static final long   PERSISTED_APPLIED = 100L;

  // Regression for #6202: an install from this nodes own address is refused
  // -----------------------------------------------------------------------------------------------
  // The Ratis-initiated install must make the refusals the manual resync makes
  // -----------------------------------------------------------------------------------------------

  /**
   * The reachable misconfiguration: every peer derives to this node's own endpoint, so the follower "downloads"
   * its own incomplete databases onto themselves. The manual path has refused this since #6111; this one did not.
   */
  @Test
  void anInstallFromThisNodesOwnAddressIsRefused(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), LOCAL_HTTP);
    raft.localHttpAddress(LOCAL_HTTP);

    assertRefused(tempDir, raft, "is this node's own");
  }

  // Regression for #6202: an install on anode that believes it is the leader is refused
  /**
   * Leadership can move while an install is in flight, and a leader has no peer to pull from: "downloading" from
   * itself would let the install drop the read floor and durably record the snapshot index as applied on state
   * this node never had.
   */
  @Test
  void anInstallOnANodeThatBelievesItIsTheLeaderIsRefused(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");

    assertRefused(tempDir, raft, "this node is the leader");
  }

  // Regression for #6202: an install from an address that identifies no single peer is refused
  /**
   * The refusal #6202 asks for on top of the two the manual path already made: an address two peers both resolve
   * to identifies at most one of them, and reconciling every database from the wrong node is not recoverable.
   */
  @Test
  void anInstallFromAnAddressThatIdentifiesNoSinglePeerIsRefused(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    // The resolver withheld the address: it is claimed by more than one peer, or resolves to nothing.
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), null);
    raft.localHttpAddress(LOCAL_HTTP);

    assertRefused(tempDir, raft, "identifies leader");
  }

  // Regression for #6202: a genuine peer leader is still installed from
  /**
   * Control: a genuine peer leader is still installed from, and the install still resolves the state it is
   * supposed to. Without it the three tests above would also pass against a method that refused everything.
   */
  @Test
  void aGenuinePeerLeaderIsStillInstalledFrom(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress(LOCAL_HTTP);

    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L,
          FIRST_LOG_INDEX)).get();

      assertThat(installed.getIndex())
          .as("the snapshot covers every entry BEFORE the first one still in the log")
          .isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("an install from a real peer resolves the read floor")
          .isEqualTo(-1L);
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(SNAPSHOT_INDEX);
    } finally {
      sm.close();
    }
  }

  // Regression for #6202: a refusal is logged as awarning and not as afault
  /**
   * A refusal is the expected outcome of a guard, and Ratis re-drives the install, so on a misconfigured cluster
   * - or in the window after an election before the leader-role flag catches up - it fires on every attempt.
   * Logged as the two request-driven resync paths log the same refusal: WARNING, message only. It used to fall
   * into the install's generic {@code catch (Exception)} and come out as a SEVERE with a stack trace, which is
   * what log-based alerting keys on.
   */
  @Test
  void aRefusalIsLoggedAsAWarningAndNotAsAFault(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), LOCAL_HTTP);
    raft.localHttpAddress(LOCAL_HTTP);

    final List<LoggedLine> logged = new CopyOnWriteArrayList<>();
    final Logger previous = installCapturingLogger(logged);
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      assertThatThrownBy(() -> sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get(30, TimeUnit.SECONDS)).isNotNull();

      final LoggedLine refusal = logged.stream()
          .filter(l -> l.message().startsWith("Refusing a leader-initiated snapshot install"))
          .findFirst()
          .orElse(null);
      assertThat(refusal).as("the refusal must be logged, so an operator can see why the node stays behind")
          .isNotNull();
      assertThat(refusal.level()).isEqualTo(Level.WARNING);
      assertThat(refusal.throwable())
          .as("a guard firing as designed carries no stack trace worth printing")
          .isNull();

      assertThat(logged.stream().map(LoggedLine::message))
          .as("and it must not ALSO come out of the generic failure arm as a SEVERE fault")
          .noneMatch(m -> m.startsWith("Error during snapshot installation from leader"));
    } finally {
      LogManager.instance().setLogger(previous);
      sm.close();
    }
  }

  // Regression for #6202: a withheld https endpoint does not reach the reconciler
  // -----------------------------------------------------------------------------------------------
  // Both endpoints the install hands the reconciler come from the guard (issue #6221)
  // -----------------------------------------------------------------------------------------------

  /**
   * The encrypted endpoint has to be guarded on its own account, not on the HTTP one's: it is read from the 5th
   * field of {@code HA_SERVER_LIST} where the HTTP address is read from the 3rd, each with its own derive
   * fallback onto THIS node's port for that protocol. A cluster that declares distinct {@code http} ports and
   * omits the {@code https} ones passes the HTTP check with every peer's HTTPS endpoint still collapsed onto this
   * node's own - and the reconciler prefers the encrypted endpoint whenever it is non-null, threading it into
   * every branch it has.
   * <p>
   * Driven with the raw resolver answering an address and the guard withholding it, so a regression to
   * {@code getPeerHttpsAddress} shows up as the wrong address reaching the reconciler rather than as nothing at
   * all.
   */
  @Test
  void aWithheldHttpsEndpointDoesNotReachTheReconciler(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress(LOCAL_HTTP);
    // The best-effort resolver still hands one out - it is what an unguarded caller would dial...
    raft.peerHttpsAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "localhost:2443");
    // ...and the guard withholds it, because it identifies no single peer (or is this node's own).
    raft.peerHttpsAddress(RaftPeerId.valueOf(LEADER_PEER_ID), null);

    final CapturingReconciler reconciler = new CapturingReconciler();
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    replaceReconciler(sm, reconciler);
    sm.setRaftHAServer(raft);
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(reconciler.httpAddr).isEqualTo("peer-b:2480");
      assertThat(reconciler.httpsAddr)
          .as("a withheld encrypted endpoint must reach the reconciler as absent, so it falls back to the "
              + "guarded plain-HTTP one rather than dialling an address that identifies nobody")
          .isNull();
    } finally {
      sm.close();
    }
  }

  // Regression for #6202: a guarded https endpoint is still handed to the reconciler
  /** Control: an encrypted endpoint that passes the guard is still used, so the guard costs SSL nothing. */
  @Test
  void aGuardedHttpsEndpointIsStillHandedToTheReconciler(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress(LOCAL_HTTP);
    raft.peerHttpsAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2443");
    raft.localHttpsAddress("localhost:2443");

    final CapturingReconciler reconciler = new CapturingReconciler();
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    replaceReconciler(sm, reconciler);
    sm.setRaftHAServer(raft);
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(reconciler.httpAddr).isEqualTo("peer-b:2480");
      assertThat(reconciler.httpsAddr).isEqualTo("peer-b:2443");
    } finally {
      sm.close();
    }
  }

  /** Records the endpoints the install hands it, and does nothing else: no network, no databases. */
  private static class CapturingReconciler extends DatabaseReconciler {
    private volatile String      httpAddr;
    private volatile String      httpsAddr;
    /** What the reconcile reports as not brought to the snapshot index (issue #6760); nothing, by default. */
    private volatile Set<String> givenUp = Set.of();

    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
        final String clusterToken, final long installedBoundaryIndex) {
      this.httpAddr = leaderHttpAddr;
      this.httpsAddr = leaderHttpsAddr;
      return new ReconcileFromLeaderResult(givenUp, null);
    }
  }

  private static void replaceReconciler(final ArcadeStateMachine sm, final DatabaseReconciler reconciler)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("reconciler");
    f.setAccessible(true);
    f.set(sm, reconciler);
  }

  // Regression for #6202: every download attempt of an operator resync is re guarded
  // -----------------------------------------------------------------------------------------------
  // A path that re-resolves the address on every retry must re-guard it on every retry
  // -----------------------------------------------------------------------------------------------

  /**
   * {@code SnapshotInstaller.install} takes address SUPPLIERS precisely because leadership can move mid-operation,
   * so a guard applied once at the call site says nothing about the address attempt 3 resolves. The operator
   * resync checked up front and then handed over the raw resolver, which is the same failure #6202 fixes,
   * reopened on a later attempt.
   * <p>
   * Driven by making the resolver answer a good peer once - satisfying the up-front check - and this node's own
   * address from then on, which is what a leadership change onto this node looks like to the supplier.
   */
  @Test
  void everyDownloadAttemptOfAnOperatorResyncIsReGuarded(@TempDir final Path tempDir) throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.leaderId(RaftPeerId.valueOf(LEADER_PEER_ID));
    raft.localHttpAddress(LOCAL_HTTP);
    // First call: a genuine peer, so the up-front refusal does not fire. Every call after it: this node itself.
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480", LOCAL_HTTP);

    final List<LoggedLine> logged = new CopyOnWriteArrayList<>();
    final Logger previous = installCapturingLogger(logged);
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      assertThatThrownBy(() -> sm.resyncDatabaseFromLeader("testdb"))
          .as("the download must fail rather than pull this node's own database onto itself")
          .isInstanceOf(ReplicationException.class)
          .hasMessageContaining("Failed to resync database 'testdb'");

      assertThat(logged.stream().map(LoggedLine::message))
          .as("and the attempt must say it was refused, not merely that no address was known")
          .anyMatch(m -> m.startsWith("Refusing to pull a snapshot"));
    } finally {
      LogManager.instance().setLogger(previous);
      sm.close();
    }
  }

  // Regression for #6202: closing the state machine unwinds an install parked behind an in flight resync
  // -----------------------------------------------------------------------------------------------
  // Serialising the resync paths must not outlive the state machine
  // -----------------------------------------------------------------------------------------------

  /**
   * The install waits for {@code snapshotDownloadLock} rather than racing whatever holds it, and it is the one
   * resync path that may block - it owns its executor thread. That wait has to be interruptible, or a
   * {@code close()} would be held open for the length of somebody else's download; {@code lockInterruptibly()}
   * is what makes {@code shutdownNow()} able to unwind it, and this is the only path in the new locking code
   * without other coverage.
   */
  @Test
  void closingTheStateMachineUnwindsAnInstallParkedBehindAnInFlightResync(@TempDir final Path tempDir)
      throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress(LOCAL_HTTP);

    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);

    // Stands in for a request-driven resync already running: the install must queue behind it, not proceed.
    final ReentrantLock resyncLock = snapshotDownloadLock(sm);
    resyncLock.lock();
    try {
      final CompletableFuture<TermIndex> install = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX));

      for (int i = 0; i < 500 && !resyncLock.hasQueuedThreads(); i++)
        Thread.sleep(10);
      assertThat(resyncLock.hasQueuedThreads())
          .as("the install must park on the lock instead of running concurrently with the in-flight resync")
          .isTrue();

      sm.close();

      // Bounded rather than get(): an install that is NOT unwound never completes, and a regression must fail
      // this test rather than hang the module's run.
      assertThatThrownBy(() -> install.get(30, TimeUnit.SECONDS))
          .as("shutdownNow() must unwind the parked install rather than wait out a download it cannot see")
          .hasRootCauseInstanceOf(InterruptedException.class)
          .hasStackTraceContaining("Interrupted while waiting for an in-flight resync to finish");
    } finally {
      resyncLock.unlock();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * Drives the Ratis-initiated install against {@code raft} and asserts it fails with a refusal naming
   * {@code reasonFragment}, leaving the node visibly behind rather than recorded as caught up.
   */
  private void assertRefused(final Path tempDir, final RaftHAServer raft, final String reasonFragment)
      throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, "testdb");
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      final CompletableFuture<TermIndex> install = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX));

      assertThatThrownBy(install::get)
          .as("the install must fail so Ratis retries it, rather than record a snapshot it never installed")
          .rootCause()
          .hasMessageStartingWith("Refusing a leader-initiated snapshot install: ")
          .hasMessageContaining(reasonFragment);

      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("a refused install resolves nothing, so the stale-read floor must stand")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.readPersistedAppliedIndex())
          .as("and the snapshot index must not be durably recorded as applied")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.isResyncInProgress())
          .as("the node keeps itself out of the ready set instead of pretending it caught up")
          .isTrue();
    } finally {
      sm.close();
    }
  }

  /** The Ratis role info a follower receives, naming {@link #LEADER_PEER_ID} as the installing leader. */
  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  /**
   * A state machine with real Ratis storage (the install registers a snapshot marker through it) rooted at
   * {@code tempDir}, auto-acquire off so a reconcile over zero local databases downloads nothing, and the leader's
   * snapshot-marker read answered locally ({@link NoNetworkMarkerReconciler}, issue #8374) so no leader has to answer.
   */
  private static ArcadeStateMachine newInitializedStateMachine(final Path tempDir) throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    // No backoff to sit through: the download paths under test never reach the network, they are refused first.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final ArcadeDBServer server = new ArcadeDBServer(config);
    sm.setServer(server);
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        newFormattedStorage(tempDir.resolve("raft")));
    // The install reads the leader's snapshot marker even with auto-acquire off (issue #8374); answer it locally.
    try {
      NoNetworkMarkerReconciler.installInto(sm, server);
    } catch (final Exception e) {
      throw new IOException(e);
    }
    return sm;
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  /** One line the capturing logger saw. */
  private record LoggedLine(Level level, String message, Throwable throwable) {
  }

  /**
   * Installs a {@link Logger} that records every line, returning the previous one for restore. Restores a fresh
   * {@link DefaultLogger} rather than the live instance, which is the sanctioned test pattern here.
   */
  private static Logger installCapturingLogger(final List<LoggedLine> logged) {
    final Logger capturing = new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        if (message != null)
          logged.add(new LoggedLine(level, message, throwable));
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object... args) {
        if (message != null)
          logged.add(new LoggedLine(level, message, throwable));
      }

      @Override
      public void flush() {
      }
    };
    final Logger previous = new DefaultLogger();
    LogManager.instance().setLogger(capturing);
    return previous;
  }

  private static ReentrantLock snapshotDownloadLock(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("snapshotDownloadLock");
    f.setAccessible(true);
    return (ReentrantLock) f.get(sm);
  }

  private static void setStaleSnapshotAppliedFloor(final ArcadeStateMachine sm, final long floor) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(floor);
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()};
   * nothing else is reached on this path.
   */
  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        SnapshotInstallGuardTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  // ------------------------------------------------------------------------------------------
  // Issue #7253
  // ------------------------------------------------------------------------------------------
  /*
   * Issue #7253: {@code installSnapshotFromLeader} must read the volatile {@code raftHAServer} field ONCE and use
   * that instance for the whole install.
   * <p>
   * It used to read it twice: {@code resolveSnapshotSource(leaderId)} takes its own local precisely so it can
   * <em>refuse</em> rather than throw when the field is unset, and two lines later the cluster token was read
   * straight off the field again. That is the shape #7221 fixed one branch away, in
   * {@code applyInstallDatabaseEntry}'s {@code forceSnapshot} arm (PR #7249), where {@code getLeaderId()} was
   * evaluated as the ARGUMENT to {@code resolveSnapshotSource} and so ran before the null guard inside it.
   * <p>
   * <b>The window this pins.</b> Unlike the #7221 case the second read sat after the refusal check, so a field that
   * was already null was refused before reaching it. What was left is the field turning null <em>between</em> the
   * two reads - a teardown racing an in-flight install - and the test drives exactly that: the mocked HA server
   * clears the state machine's reference as a side effect of the address resolution that happens inside
   * {@code PeerDialAddress.resolve}. Before the fix the install then died with a {@link NullPointerException} on
   * the automatic resync path, the one path #6202 exists because it had no checks at all.
   *
   */

  private static final RaftPeerId LEADER = RaftPeerId.valueOf("leader-peer");

  private static final RaftPeerId LOCAL  = RaftPeerId.valueOf("local-peer");

  /** The first log index AFTER the snapshot, so the install records {@code getIndex() - 1}. */
  private static final long FIRST_INDEX_IN_LOG = 10L;

  // Regression for #7253: a teardown between the two reads no longer fails the install
  @Test
  void aTeardownBetweenTheTwoReadsNoLongerFailsTheInstall(@TempDir final Path raftDirectory,
      @TempDir final Path databaseDirectory) throws Exception {

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftStorage storage = RaftStorage.newBuilder()
        .setDirectory(raftDirectory.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    try {
      final FakeArcadeDBServer server = followerServer(databaseDirectory);
      sm.setServer(server);
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
      // The install reads the leader's snapshot marker even with auto-acquire off (issue #8374); answer it locally.
      NoNetworkMarkerReconciler.installInto(sm, server);
      sm.setRaftHAServer(haServerThatIsTornDownMidResolution(sm));

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(
          leaderRoleInfoTeardown(), TermIndex.valueOf(3L, FIRST_INDEX_IN_LOG)).get();

      assertThat(sm.getRaftHAServer())
          .as("the teardown really happened while the install was resolving its source")
          .isNull();
      assertThat(installed.getIndex())
          .as("the install completed from the instance it had already resolved against, instead of "
              + "dereferencing the field a second time and throwing")
          .isEqualTo(FIRST_INDEX_IN_LOG - 1);
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(FIRST_INDEX_IN_LOG - 1);
    } finally {
      sm.close();
      storage.close();
    }
  }

  /**
   * A leader whose HTTP address resolves cleanly, and which clears {@code sm}'s reference to itself the moment it
   * is asked for that address - the last read {@code PeerDialAddress.resolve} makes through its own local, and so
   * the exact instant at which a second read of the field would see {@code null}.
   */
  private static FakeRaftHAServer haServerThatIsTornDownMidResolution(final ArcadeStateMachine sm) {
    final FakeRaftHAServer raftHA = FakeRaftHAServer.detached();
    raftHA.leader(false);
    raftHA.localPeerId(LOCAL);
    raftHA.leaderId(LEADER);
    raftHA.clusterToken("cluster-token");
    raftHA.on("getUnambiguousPeerHttpAddress", args -> {
      if (!LEADER.equals(args[0]))
        return null;
      sm.setRaftHAServer(null);
      return "leader-host:2480";
    });
    raftHA.localHttpAddress("local-host:2480");
    raftHA.peerHttpsAddress(LEADER, null);
    return raftHA;
  }

  /**
   * A follower holding no databases, with auto-acquire off so the reconciler takes the refresh-existing path and,
   * with the marker read stubbed, completes without dialling anything: this test is about which field read happens, not about the download.
   */
  private static FakeArcadeDBServer followerServer(final Path databaseDirectory) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());
    configuration.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", configuration);

    return server;
  }

  /** The Ratis callback payload the install reads the leader id out of. */
  private static RaftProtos.RoleInfoProto leaderRoleInfoTeardown() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER.toString()))
                    .build())
                .build())
            .build())
        .build();
  }

  /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        SnapshotInstallGuardTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  // ------------------------------------------------------------------------------------------
  // Issue #8702
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #8702: a leader-driven snapshot install ({@code notifyInstallSnapshotFromLeader}) must not
   * settle the pending bootstrap replacement (issue #8367) nor clear the bootstrap-divergence mark (issue #6124) of a
   * database it did not reinstall.
   * <p>
   * The reconcile can leave a present database untouched two ways: it gave up on it ({@code notInstalled}, issue #6760)
   * or the leader does not hold it ({@code leaderMissing}, issues #8559/#8588). Either way the copy on disk is still the
   * one the committed bootstrap baseline rejected, so the readiness holder that keeps the node out of the Service and the
   * #8363 request gate must stay, and so must the alert. Before the fix the install settled every pending replacement
   * except the given-up ones, and cleared every divergence mark including the given-up ones - the full-resync sibling
   * ({@code downloadAllDatabasesFrom}) already excluded both sets.
   * <p>
   * The pending replacements and marks are seeded through the persisted {@code bootstrap-baselines} file, the same way a
   * node restarted with a replacement still pending finds them (issue #8411), so each one owns its readiness holder.
   *
   */

  private static final long   FIRST_LOG_INDEX_BOOTSTRAP = 5_000L;

  private static final String MISSING_DB      = "db-leader-missing";

  private static final String GIVEN_UP_DB     = "db-given-up";

  private static final String REINSTALLED_DB  = "db-reinstalled";

  // Regression for #8702: a leader missing database keeps its pending replacement and divergence mark
  /**
   * The issue as reported: the reconcile reports a database LEADER_MISSING while its bootstrap replacement is pending.
   * The install must leave the replacement pending with its readiness holder, and the divergence mark standing.
   */
  @Test
  void aLeaderMissingDatabaseKeepsItsPendingReplacementAndDivergenceMark(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(MISSING_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(), Set.of(MISSING_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      assertThat(sm.getPendingBootstrapReplacements())
          .as("precondition: both replacements are pending before the install")
          .containsExactly(MISSING_DB, REINSTALLED_DB);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(MISSING_DB, REINSTALLED_DB);

      sm.notifyInstallSnapshotFromLeader(leaderRoleInfoBootstrap(), TermIndex.valueOf(9L, FIRST_LOG_INDEX_BOOTSTRAP)).get();

      assertThat(sm.getPendingBootstrapReplacements())
          .as("the leader does not hold it, so nothing replaced the rejected copy: still pending")
          .containsExactly(MISSING_DB);
      assertThat(sm.getBootstrapInstallsInFlight())
          .as("and its readiness holder still keeps the node out of the Service")
          .containsExactly(MISSING_DB);
      assertThat(sm.getBootstrapUnreconciledDatabases())
          .as("the divergence alert stays for the copy nothing replaced")
          .containsExactly(MISSING_DB);
      assertThat(sm.hasLeaderServiceGap())
          .as("the node must not advertise itself as a healthy leadership hand-off target (issues #8529/#8665)")
          .isTrue();
      assertPersisted(tempDir, MISSING_DB, true, true);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  // Regression for #8702: a given up database keeps its divergence mark
  /**
   * The older instance of the same shape the issue names: a database the install gave up on already kept its pending
   * replacement, but lost its divergence mark.
   */
  @Test
  void aGivenUpDatabaseKeepsItsDivergenceMark(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(GIVEN_UP_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(GIVEN_UP_DB), Set.of()));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfoBootstrap(), TermIndex.valueOf(9L, FIRST_LOG_INDEX_BOOTSTRAP)).get();

      assertThat(sm.getPendingBootstrapReplacements()).containsExactly(GIVEN_UP_DB);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(GIVEN_UP_DB);
      assertThat(sm.getBootstrapUnreconciledDatabases())
          .as("the install gave up on it, so the copy the overwrite guard kept is still there")
          .containsExactly(GIVEN_UP_DB);
      assertPersisted(tempDir, GIVEN_UP_DB, true, true);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  // Regression for #8702: both unreplaced outcomes in one install
  /** Both outcomes in one install: each database keeps its own state, the reinstalled one is settled. */
  @Test
  void bothUnreplacedOutcomesInOneInstall(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(MISSING_DB, GIVEN_UP_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(GIVEN_UP_DB), Set.of(MISSING_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfoBootstrap(), TermIndex.valueOf(9L, FIRST_LOG_INDEX_BOOTSTRAP)).get();

      assertThat(sm.getPendingBootstrapReplacements()).containsExactly(GIVEN_UP_DB, MISSING_DB);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(GIVEN_UP_DB, MISSING_DB);
      assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(GIVEN_UP_DB, MISSING_DB);
    } finally {
      sm.close();
    }
  }

  // Regression for #8702: each exclusion holds on its own
  /**
   * The two exclusions pinned independently: one LEADER_MISSING database carries only the divergence mark, another
   * only the pending replacement. Each must keep exactly what it had.
   */
  @Test
  void eachExclusionHoldsOnItsOwn(@TempDir final Path tempDir) throws Exception {
    final String markOnlyDb = "db-mark-only";
    final String pendingOnlyDb = "db-pending-only";
    final JSONObject json = new JSONObject();
    json.put(markOnlyDb, baselineEntry().put("unreconciled", true));
    json.put(pendingOnlyDb, baselineEntry().put("pendingReplacement", true));
    json.put(REINSTALLED_DB, baselineEntry().put("unreconciled", true).put("pendingReplacement", true));
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(markOnlyDb, pendingOnlyDb, REINSTALLED_DB), json);
    replaceReconciler(sm, new StubReconciler(Set.of(), Set.of(markOnlyDb, pendingOnlyDb)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfoBootstrap(), TermIndex.valueOf(9L, FIRST_LOG_INDEX_BOOTSTRAP)).get();

      assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(markOnlyDb);
      assertThat(sm.getPendingBootstrapReplacements()).containsExactly(pendingOnlyDb);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(pendingOnlyDb);
      assertPersisted(tempDir, markOnlyDb, false, true);
      assertPersisted(tempDir, pendingOnlyDb, true, false);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  // Regression for #8702: a full install still settles everything
  /** Control: an install that replaced every database settles every replacement and clears every mark, as before. */
  @Test
  void aFullInstallStillSettlesEverything(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(MISSING_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(), Set.of()));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfoBootstrap(), TermIndex.valueOf(9L, FIRST_LOG_INDEX_BOOTSTRAP)).get();

      assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
      assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
      assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
      assertThat(sm.hasLeaderServiceGap()).isFalse();
      assertPersisted(tempDir, MISSING_DB, false, false);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  /** A reconciler that touches nothing and reports exactly the outcome the test wants. */
  private static class StubReconciler extends DatabaseReconciler {
    private final Set<String> notInstalled;
    private final Set<String> leaderMissing;

    private StubReconciler(final Set<String> notInstalled, final Set<String> leaderMissing) {
      this.notInstalled = notInstalled;
      this.leaderMissing = leaderMissing;
    }

    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderPeerId, final String leaderHttpAddr,
        final String leaderHttpsAddr, final String clusterToken, final long installedBoundaryIndex) {
      return new ReconcileFromLeaderResult(notInstalled, leaderMissing, null);
    }
  }

  private static void assertPersisted(final Path tempDir, final String dbName, final boolean pendingReplacement,
      final boolean unreconciled) throws IOException {
    final JSONObject entry = new JSONObject(Files.readString(baselinesFile(tempDir)).trim()).getJSONObject(dbName);
    assertThat(entry.getBoolean("pendingReplacement", false)).as("persisted pendingReplacement of '%s'", dbName)
        .isEqualTo(pendingReplacement);
    assertThat(entry.getBoolean("unreconciled", false)).as("persisted unreconciled mark of '%s'", dbName)
        .isEqualTo(unreconciled);
  }

  private static Path baselinesFile(final Path tempDir) {
    return tempDir.resolve("databases").resolve(".raft").resolve("bootstrap-baselines");
  }

  private static JSONObject baselineEntry() {
    return new JSONObject().put("fingerprint", "0".repeat(64)).put("lastTxId", 5L);
  }

  /** Every database starts with a baseline, a divergence mark and a pending replacement, as after a restart. */
  private static JSONObject markedAndPending(final Set<String> databaseNames) {
    final JSONObject json = new JSONObject();
    for (final String dbName : databaseNames)
      json.put(dbName, baselineEntry().put("unreconciled", true).put("pendingReplacement", true));
    return json;
  }

  private static RaftHAServer followerRaft() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.leaderId(RaftPeerId.valueOf(LEADER_PEER_ID));
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress("localhost:2480");
    return raft;
  }

  private static RaftProtos.RoleInfoProto leaderRoleInfoBootstrap() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir, final Set<String> databaseNames)
      throws IOException {
    return newStateMachine(tempDir, databaseNames, markedAndPending(databaseNames));
  }

  /** Seeds the persisted bootstrap state first, so the state machine loads it the way a restarted node does. */
  private static ArcadeStateMachine newStateMachine(final Path tempDir, final Set<String> databaseNames,
      final JSONObject bootstrapState) throws IOException {
    Files.createDirectories(baselinesFile(tempDir).getParent());
    Files.writeString(baselinesFile(tempDir), bootstrapState.toString());

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);
    server.databaseNames(databaseNames.toArray(new String[0]));

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServerBootstrap(), RaftGroupId.valueOf(UUID.randomUUID()),
        RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
            .build());
    return sm;
  }

  private static RaftServer stubRaftServerBootstrap() {
    return (RaftServer) Proxy.newProxyInstance(
        SnapshotInstallGuardTest.class.getClassLoader(),
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
