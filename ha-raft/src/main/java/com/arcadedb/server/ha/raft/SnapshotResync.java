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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.StaleSnapshotAcceptance;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * The resync of this node from the leader's snapshot: where to download it from, the download of every database or of
 * one, and the stale read floor a partial install leaves until the gap is filled. While the floor is up, reads that need
 * an index past it wait or are refused instead of being served from a copy that does not hold it.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which triggers the resync when it finds this node behind or diverged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SnapshotResync {
  private final ArcadeStateMachine sm;

  SnapshotResync(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Throws {@link StaleResyncOrderException} when {@code order} no longer matches this node (issue #8490). The applied
   * index judged is the trusted one (#6111/#6760 floors), raised to {@code appliedUnderGate} - the highest index the
   * apply thread really applied to this database under its install lock - when that is known: entries that went to
   * this copy are held by it, whatever a floor says about the rest.
   */
  static void checkStalledResyncOrder(final RaftHAServer raft, final String dbName,
      final StalledResyncOrder order, final long appliedUnderGate) {
    final long applied = Math.max(raft.getTrustedAppliedIndex(dbName), appliedUnderGate);
    final String refusal = order.refusal(raft.getCurrentTerm(), applied);
    if (refusal != null)
      throw new StaleResyncOrderException(
          "Stale resync order for database '" + dbName + "': " + refusal + ". The local copy is kept.");
  }

  /**
   * Answers whether a snapshot resync from {@code leaderId} may be attempted, and with which address.
   * <p>
   * Every resync path asks it - the manual {@link ArcadeStateMachine#triggerSnapshotDownload()}, the targeted
   * {@code triggerDatabaseResync(String)} and the Ratis-initiated {@link ArcadeStateMachine#notifyInstallSnapshotFromLeader} -
   * because a resync that pulls from the wrong node is not recoverable and not even visible: the reconcile
   * succeeds, the install is recorded, and the node returns to the ready set carrying whatever it copied. The
   * Ratis-initiated path had none of these checks at all (issue #6202), and duplicating them would have made a
   * fourth hand-maintained copy of a rule the first three already disagreed about.
   * <p>
   * The refusal that is specific to a resync is made here; the rest is the general question "may this node dial
   * that one, and at which address?", which {@link PeerDialAddress#resolve} answers for every caller that acts on
   * a resolved peer address unattended (issue #6221):
   * <ul>
   * <li><b>This node is the leader.</b> A node cannot repair itself from itself. {@code notifyLeaderChanged()}
   * submits a resync unconditionally - including on the node that just WON the election - and the leader address
   * then resolves to this node's own, so the "download" would copy this node's already-incomplete databases back
   * onto themselves and report success. That is merely pointless on some paths, but it lets
   * {@link #resolveStaleSnapshotFloorAfterResync} durably record the marker index as applied and drop the read
   * floor, re-opening issue #6111 and surviving restarts. A role check rather than an identity one: it fires
   * before the address is even resolved, and it is the one refusal that is about what this node <em>is</em>
   * rather than about where the other one lives.</li>
   * <li><b>Everything {@link PeerDialAddress#resolve} refuses</b> - an unknown leader, an address that identifies
   * no single peer (issue #6202), an address that is this node's own (issue #6191). The last is also the backstop
   * for the leader-role check above: leadership can move between the two, and {@code getLeaderId()} can already
   * report this node while {@code isLeader()} has not caught up.</li>
   * </ul>
   * Refusing leaves the node visibly behind - the floor stands, {@link ArcadeStateMachine#isResyncInProgress()} keeps it out of the
   * ready set, reads keep failing honestly - which is the state it is actually in. Ratis retries the install and
   * the {@link HealthMonitor} re-arms the manual path, so a refusal is not a dead end either.
   */
  PeerDialAddress resolveSnapshotSource(final RaftPeerId leaderId) {
    // Read once into a local: the field is volatile and a teardown can null it between two reads, which would
    // turn a refusal into a NullPointerException on the install path.
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return PeerDialAddress.refuse("the HA server is not available on this node");

    if (raftHA.isLeader())
      return PeerDialAddress.refuse("this node is the leader, so there is no peer to pull from. "
          + "The request stays pending until leadership moves elsewhere (issue #6111)");

    return PeerDialAddress.resolve(raftHA, leaderId, "leader");
  }

  /**
   * The leader's HTTP address when a snapshot may be pulled from it, {@code null} when it may not. The
   * supplier-shaped form of {@link #resolveSnapshotSource}, for the {@code SnapshotInstaller.install} overloads
   * that re-resolve the address on every download attempt.
   * <p>
   * Guarding the call site once is not enough for those: the whole reason they take a supplier is that leadership
   * can move mid-operation, and an up-front check says nothing about the address attempt 3 will resolve. Every
   * attempt asks again (issue #6202).
   * <p>
   * This is the arm that logs the refusal - see {@link #guardedLeaderHttpsAddress()} for why that is exactly one
   * line per attempt whether or not SSL is on.
   */
  String guardedLeaderHttpAddress() {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return null;
    final PeerDialAddress source = resolveSnapshotSource(raftHA.getLeaderId());
    if (source.refused()) {
      LogManager.instance().log(sm, Level.WARNING, "Refusing to pull a snapshot: %s", source.refusal());
      return null;
    }
    return source.httpAddress();
  }

  /**
   * The leader's HTTPS address under the same guard, or {@code null}. Needed as well as the HTTP arm because
   * {@code downloadWithRetry} prefers the HTTPS endpoint when SSL is enabled and only falls back to the HTTP one
   * when it comes back null - so guarding HTTP alone would leave the guard unreachable on an SSL cluster.
   * <p>
   * Silent by design, which is what keeps the pair to one log line per attempt: a refusal makes this return null,
   * and the caller then consults the HTTP arm, which logs. When it does NOT refuse there is nothing to log, and
   * the HTTP arm is not consulted at all.
   * <p>
   * The HTTPS endpoint is the guard's own, not the raw resolver's: it is read from a different field of
   * {@code HA_SERVER_LIST} than the HTTP one and derives onto a different local port, so a cluster that declares
   * distinct {@code http} ports and omits the {@code https} ones passes the HTTP check while every peer's HTTPS
   * endpoint still resolves to this node (issue #6221). Withheld, it returns null here and the download falls back
   * to the guarded HTTP endpoint, which is the route an unresolvable HTTPS endpoint has always taken.
   */
  String guardedLeaderHttpsAddress() {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return null;
    return resolveSnapshotSource(raftHA.getLeaderId()).httpsAddress();
  }

  /**
   * Reinstalls every present database from {@code source} and resolves the state a full resync clears. Takes the
   * whole verdict rather than one address so the encrypted endpoint reaching the installer is the guarded one
   * too (issue #6221). The caller has already established that it may be pulled from
   * ({@link #resolveSnapshotSource}) and holds {@link ArcadeStateMachine#snapshotDownloadLock}.
   * <p>
   * "Present" is the registry AND the disk (issue #8464). {@code close database} deregisters a database and leaves its
   * files in place, it is not leader-forwarded, and the next request on this node reopens that directory - so a resync
   * that walked only the registry left the closed copy at its pre-resync state and still recorded the whole gap as
   * applied. The closed ones are listed from the databases directory AFTER the registered ones are reinstalled, so a
   * database closed while this loop runs is caught by the listing rather than missed by both. The install reopens a
   * closed database, which is the only state in which this node can vouch for its content.
   * <p>
   * A closed database that cannot be installed does NOT fail the resync of the others, unlike a registered one. The
   * leader answers only for the databases IT has open, so a database closed on the leader too - an ordinary
   * maintenance close, run on every node - would otherwise fail every full resync of this node for good, with every
   * other database left behind with it. When the leader answers that it does not hold the database (a 404), the copy
   * is reported {@link DatabaseReconciler.AcquireState#LEADER_MISSING} and kept, neither installed nor quarantined
   * (issue #8559): no resync from that leader can ever replace it, so a quarantine - which holds the WHOLE node out of
   * the ready set - would last until an operator intervened. Any other failure is quarantined, with a read floor at its
   * own applied position, which is what a leader-driven install does with a database it gave up on (issue #6760): the
   * node stays out of the ready set while it holds a copy it knows nothing about, and
   * {@link #retryUnfilledSnapshotGap()} re-drives a targeted resync of it.
   * <p>
   * A REGISTERED database the leader answers 404 for gets the same {@code LEADER_MISSING} verdict (issue #8588) - the
   * one the auto-acquire reconcile of a leader-driven install already gives it - rather than failing the resync of
   * every other database with it. Its copy is kept, still registered and served; any other failure of a registered
   * database still fails the whole resync, which keeps the node-wide floor and re-arms it.
   */
  void downloadAllDatabasesFrom(final PeerDialAddress source, final String clusterToken) throws IOException {
    final String leaderHttpAddr = source.httpAddress();
    final String leaderHttpsAddr = source.httpsAddress();
    int resynced = 0;
    // Copies the leader does not hold: neither installed nor quarantined, so they are in neither set below, and nothing
    // replaced them either - so their bootstrap-divergence marks are theirs to keep (issues #8559, #8588).
    final Set<String> leaderMissing = new HashSet<>();
    for (final String dbName : sm.server.getDatabaseNames()) {
      // install() keeps the database open during the download and rolls back on failure, so a
      // watchdog-triggered resync never leaves it closed.
      if (sm.server.existsDatabase(dbName)) {
        try {
          sm.replacement.installLeaderCopy(dbName, leaderHttpAddr, leaderHttpsAddr, clusterToken);
        } catch (final LeaderDoesNotHoldDatabaseException e) {
          // The same leader answers the same 404 on every retry, so failing the whole resync for it kept the node-wide
          // floor for good (issue #8588). Reported as the auto-acquire reconcile reports it, the copy kept and served.
          reportDatabaseTheLeaderDoesNotHold(dbName, false, e);
          leaderMissing.add(dbName);
          continue;
        }
        sm.reconciler.clearLeaderMissing(dbName);
        // Per database, right after its own install, rather than once at the end: a later database failing must
        // not leave this one reported as still carrying the copy the bootstrap baseline rejected (issue #8367).
        sm.bootstrapInstalls.settleBootstrapReplacement(dbName);
        resynced++;
      }
    }

    final Set<String> notInstalled = new HashSet<>();
    for (final String dbName : SnapshotInstaller.closedDatabaseNames(sm.server)) {
      try {
        sm.replacement.installLeaderCopy(dbName, leaderHttpAddr, leaderHttpsAddr, clusterToken);
      } catch (final LeaderDoesNotHoldDatabaseException e) {
        // Not a failed install (issue #8559): the leader does not hold it, and a quarantine - node-wide in its readiness
        // effect - would wait for an install that the same leader refuses on every retry. Reported the way the
        // auto-acquire reconcile reports the same input, the copy kept and nothing downloaded or dropped - but marked
        // unverified, so nothing reopens it on this follower (issue #8589).
        try {
          reportDatabaseTheLeaderDoesNotHold(dbName, true, e);
        } catch (final IOException markFailure) {
          // Without the mark the copy is reopenable and unverified: fall back to the quarantine, which fails closed.
          LogManager.instance().log(sm, Level.SEVERE,
              "Snapshot resync could not mark database '%s' as an unverified closed copy: keeping it quarantined "
                  + "instead (issue #8589)", markFailure, dbName);
          notInstalled.add(dbName);
          continue;
        }
        leaderMissing.add(dbName);
        continue;
      } catch (final IOException e) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Snapshot resync could not reinstall database '%s', which is closed on this node but still on disk and "
                + "would be reopened by the next request that names it: keeping it quarantined until a targeted "
                + "resync succeeds (issue #8464)", e, dbName);
        notInstalled.add(dbName);
        continue;
      }
      LogManager.instance().log(sm, Level.INFO,
          "Snapshot resync reinstalled database '%s', which was closed on this node: it is open again (issue #8464)",
          dbName);
      sm.reconciler.clearLeaderMissing(dbName);
      sm.bootstrapInstalls.settleBootstrapReplacement(dbName);
      resynced++;
    }

    LogManager.instance().log(sm, Level.INFO,
        "Snapshot resync completed: reinstalled %d database(s) from the leader; diverged state cleared", resynced);
    if (notInstalled.isEmpty()) {
      sm.quarantine.clearDivergedState();
      // Every present database now carries the leader's copy, including any the bootstrap overwrite guard
      // had kept (issue #6124) - except one the leader does not hold, whose copy nothing replaced.
      sm.baselines.clearBootstrapUnreconciledExcept(leaderMissing);
    } else {
      // Every database but these carries the leader's copy now: clear the rest, and quarantine these with their own
      // honest floors in the same durable write (issues #6760, #8137).
      final var snapshotInfo = sm.storage.getLatestSnapshot();
      final boolean durable = settleDivergedStateAfterInstall(notInstalled,
          snapshotInfo != null ? snapshotInfo.getIndex() : -1L, false);
      sm.baselines.clearBootstrapUnreconciledExcept(BootstrapBaselines.notReplaced(notInstalled, leaderMissing));
      // Fail closed. The node-wide floor below is the only other thing protecting these copies across a restart, and a
      // quarantine that lives in memory alone is gone after one: the directory would be reopened by the next request
      // and served ready and unclamped. Failing here keeps the floor and re-arms the resync (triggerSnapshotDownload).
      if (!durable)
        throw new IOException("Snapshot resync could not persist the quarantine of database(s) " + notInstalled
            + " it could not reinstall to " + sm.appliedIndexes.getAppliedIndexFile() + "; keeping the stale-snapshot read floor "
            + "(issue #8464)");
    }
    // The databases now carry the leader's state, so a read floor published by a stale marker in
    // reinitialize() is satisfied. Record the marker index as the persisted applied position too:
    // without it the very same gap is re-detected on the next restart and the node re-downloads
    // forever. Then wake the waiters this resync unblocked (issue #6111).
    // A copy the leader does not hold was not brought to the marker either: recording it there would launder it into
    // "applied" as the leader's state (issue #8588), so it keeps the position it genuinely reached, like notInstalled.
    final Set<String> notAtMarker;
    if (leaderMissing.isEmpty())
      notAtMarker = notInstalled;
    else {
      notAtMarker = new HashSet<>(notInstalled);
      notAtMarker.addAll(leaderMissing);
    }
    resolveStaleSnapshotFloorAfterResync(resynced, notAtMarker);
  }

  /**
   * The verdict of a resync on a database the leader answered 404 for (issues #8559, #8588): the cluster's leader does
   * not hold it - closed there too, dropped by the cluster while this node was away, or never created - so there is no
   * committed state to hold this copy to and no install that will ever succeed. It is reported
   * {@link DatabaseReconciler.AcquireState#LEADER_MISSING}, as the auto-acquire reconcile reports it, and any
   * quarantine on it is lifted, as the leader-driven install lifts it for a database its reconcile reports so: a
   * quarantine holds the WHOLE node out of the ready set and would wait for an install the same leader refuses on every
   * retry. A registered copy stays registered and served, which is what the auto-acquire path does with it too.
   * <p>
   * A copy closed on this node is marked first with the durable {@code ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE}
   * (issue #8589): the leader may have closed a database the cluster still has, and this copy may be behind the
   * committed log, so a follower must not reopen it for the next request that names it. The mark is per database, so it
   * does not touch the node's readiness, and it goes with the install that replaces the copy or the drop that removes
   * it. A registered copy is never marked: it is being served, and nothing reopens it.
   *
   * @throws IOException when the mark of a closed copy cannot be written: nothing else has been changed, and the caller
   *                     keeps the copy quarantined rather than leave it reopenable
   */
  void reportDatabaseTheLeaderDoesNotHold(final String dbName, final boolean closedLocally,
      final LeaderDoesNotHoldDatabaseException e) throws IOException {
    if (closedLocally)
      SnapshotInstaller.markUnverifiedClosedCopy(sm.server, dbName);
    LogManager.instance().log(sm, Level.WARNING,
        "Snapshot resync did not reinstall database '%s'%s: the leader does not hold it (%s). Keeping this node's copy "
            + "and not quarantining it, since no resync from this leader can replace it%s. Open it on the leader if the "
            + "cluster should still have it, or remove this node's copy (issues #8559, #8588, #8589)",
        dbName, closedLocally ? ", which is closed on this node" : "", e.getMessage(),
        closedLocally ? ", but it is not reopened while this node is a follower: it may be behind the cluster" : "");
    sm.reconciler.markLeaderMissing(dbName);
    sm.quarantine.clearDivergedDatabase(dbName);
  }

  /**
   * Completes a successful full resync with respect to the stale-snapshot read floor (issue #6111):
   * persists the marker index as the applied position of every present database except {@code notInstalled} - the
   * resync brought the others to it, and leaving the persisted value behind makes {@link ArcadeStateMachine#reinitialize()} re-detect
   * the same gap on the next restart - then clears the floor and wakes the waiters it was holding back. A database
   * in {@code notInstalled} is either quarantined with its own read floor (issue #8464), so the node-wide floor is
   * no longer what protects it, or one the leader does not hold (issue #8588), which no resync can bring to the
   * marker. No-op when no floor was outstanding.
   * <p>
   * {@code resynced} is logged rather than gated on: zero is legitimate for a node with no databases
   * on disk (nothing can be stale). It is worth seeing in the log, because a
   * floor resolved after reinstalling zero databases is the shape an unexpectedly-empty registry would
   * take.
   */
  private void resolveStaleSnapshotFloorAfterResync(final int resynced, final Set<String> notInstalled) {
    if (sm.staleSnapshotAppliedFloor.get() < 0)
      return;
    LogManager.instance().log(sm, Level.INFO,
        "Stale-snapshot read floor resolved after reinstalling %d database(s); reads are unclamped again", resynced);
    final var snapshotInfo = sm.storage.getLatestSnapshot();
    if (snapshotInfo != null && snapshotInfo.getIndex() > sm.readPersistedAppliedIndex()) {
      // A database the resync could not reinstall is not at the marker index (issue #8464): recording it there would
      // make the next restart skip the replay that is the only other thing able to catch it up.
      sm.appliedIndexes.writePersistedAppliedIndexForAllDatabases(snapshotInfo.getIndex(), notInstalled);
      // Accumulate rather than set: this runs on the lifecycleExecutor while the Ratis apply thread may
      // already have replayed past the marker, and the counter must never regress under it.
      sm.lastAppliedIndex.accumulateAndGet(snapshotInfo.getIndex(), Math::max);
      // The resync reinstalled databases at the marker: the entries before it were not applied here one by one
      sm.schemaHistory.schemaHistoryIncompleteUpTo(snapshotInfo.getIndex());
    }
    clearStaleSnapshotFloor();
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA != null)
      raftHA.notifyApplied();
  }

  /**
   * Publishes the stale-snapshot read floor as {@link ArcadeStateMachine#reinitialize()} does on a gap. Test hook only.
   */
  // @VisibleForTesting
  void publishStaleSnapshotFloor(final long floor) {
    // Only reinitialize() raises the floor in production, and a live server cannot be made to restart onto a stale marker
    // from a test (issue #9498)
    sm.staleSnapshotAppliedFloor.set(floor);
  }

  /** The throttle slot of {@link #retryUnfilledSnapshotGap()}: when it last drove a retry, 0 when none since the last reset. */
  // @VisibleForTesting
  AtomicLong staleSnapshotRetryStampForTesting() {
    return sm.lastStaleSnapshotRetryMs;
  }

  /**
   * Drops the stale-snapshot read floor. Called only where a resync has actually restored the local
   * state up to the marker - never when one is merely requested or in flight (issue #6111).
   */
  // @VisibleForTesting
  void clearStaleSnapshotFloor() {
    sm.staleSnapshotAppliedFloor.set(-1);
    sm.lastStaleSnapshotRetryMs.set(0);
  }

  /**
   * Periodic backstop for an unfilled stale-snapshot gap (issue #6111), driven by the
   * {@link HealthMonitor} tick. Re-submits {@link ArcadeStateMachine#triggerSnapshotDownload()} while a read floor is
   * still outstanding and no download is running, so a node whose first attempt failed (leader HTTP
   * port unreachable while Raft gRPC is fine, disk full, leader not yet known) recovers on its own
   * instead of staying clamped and not-ready until the next leader election.
   * <p>
   * The existing stale-follower backstop cannot cover this: it is driven by
   * {@code commitIndex - appliedIndex}, and Ratis derives the applied index from the very marker that
   * is ahead, so a node with an open gap reports zero lag. {@link #recoverFromPersistentLag()} would
   * also refuse anyway, because {@link ArcadeStateMachine#isSnapshotDownloadPending()} stays true while the request is
   * re-armed.
   * <p>
   * Throttled to one attempt per {@link ArcadeStateMachine#computeSnapshotWatchdogTimeoutMs()} so a persistently failing
   * download is not retried on every tick: a full resync pulls every database from the leader, and the
   * HealthMonitor ticks far more often than that costs.
   * <p>
   * <b>A quarantine counts as an unfilled gap too (issue #7735).</b> Since the quarantine is persisted, a node
   * restarts still quarantined, and the {@code triggerDatabaseResync} at the mark belongs to the JVM that raised
   * it - so without this tick a restored quarantine would hold the node out of the ready set with nothing
   * driving the recovery. When a quarantine is the ONLY thing outstanding the retry is a targeted resync per
   * quarantined database rather than the full download, which is what the quarantine path itself does.
   */
  public void retryUnfilledSnapshotGap() {
    retryUnfilledSnapshotGap(false);
  }

  /**
   * As {@link #retryUnfilledSnapshotGap()}; with {@code leaderChanged} the throttle interval is not waited out, because
   * the attempt is driven by {@link ArcadeStateMachine#notifyLeaderChanged} rather than by a tick (issue #11294). Every refusal this method
   * and the resync it starts share - no leader known, no unambiguous address for it, this node leading - can only change
   * when a leader is elected or learnt, so the attempt that a refusal cost belongs to that moment, not to the next tick a
   * whole watchdog interval later. The slot is still taken, so the tick that follows does not repeat the attempt.
   */
  void retryUnfilledSnapshotGap(final boolean leaderChanged) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return;
    if (raftHA.isLeader()) {
      // A leader cannot resync from itself, so what it holds behind the log is healed by moving the leadership
      // instead (issue #8483); this node's next tick as a follower then drives the resync below.
      handOffLeadershipIfHeldBehind(raftHA);
      return;
    }
    if (sm.server == null)
      return;
    final long floor = sm.staleSnapshotAppliedFloor.get();
    // A per-database floor is an unfilled gap too: it is published exactly when an install gave up on a database,
    // which is the case that otherwise never re-arms anything (issue #6760). Same throttle, same single-flight.
    //
    // A quarantine with no floor beside it is the third case (issue #7735). It used to be re-driven only by the
    // triggerDatabaseResync() at the mark, which is a single attempt in this JVM - so a quarantine RESTORED from
    // disk after a restart had nothing driving it at all, and the node would have stayed out of the ready set
    // for good. None of the DivergenceCause values heals by itself, so a quarantine that is still recorded
    // on a tick is always a resync waiting to be retried.
    sm.appliedIndexes.ensureAppliedIndexLoaded();
    final Set<String> quarantined = sm.divergedDatabases.isEmpty() ? Set.of() : new HashSet<>(sm.divergedDatabases.keySet());
    if (floor < 0 && sm.staleDatabaseAppliedFloors.isEmpty() && quarantined.isEmpty())
      return; // no unfilled gap and nothing quarantined
    if (sm.snapshotDownloadInProgress.get())
      return; // one is genuinely running; it will clear the floor or re-arm the request
    // The same three questions resolveSnapshotSource() asks, so this cheap precheck cannot pass a request the
    // resync would then refuse and burn a throttle slot doing it (issue #6202): the leader role is checked at the
    // top of this method, and the resolved address must both identify a single peer and not be our own. Asked
    // through the same two helpers rather than restated, and without resolveSnapshotSource() itself, whose
    // unresolvable-local-address WARNING belongs to an attempt rather than to a HealthMonitor tick. When the
    // local address cannot be resolved isOwnHttpAddress answers false, so the request goes through and that
    // warning is emitted once by the resync, which is where it is actionable.
    final String leaderHttpAddr = raftHA.getUnambiguousPeerHttpAddress(raftHA.getLeaderId());
    if (leaderHttpAddr == null || raftHA.isOwnHttpAddress(leaderHttpAddr))
      return; // nowhere to download from yet; notifyLeaderChanged() drives the first attempt

    // Only a quarantine outstanding: leave out the databases the leader holds quarantined too (issue #9553). Their only
    // possible source refuses to serve them, so a resync of them cannot succeed until an operator acts - the
    // all-voters-quarantined alert says which action - or leadership moves to a node with a usable copy. Checked
    // BEFORE the throttle slot is taken, so once the leader's next capability answer (every few seconds) no longer
    // reports the quarantine, the following tick retries.
    final RaftPeerId leaderId = raftHA.getLeaderId();
    final Set<String> toResync = floor < 0 && sm.staleDatabaseAppliedFloors.isEmpty() ?
        DatabaseQuarantine.resyncableFromLeader(quarantined, leaderId != null ? leaderId.toString() : null,
            raftHA.getPeerCapabilityRegistry()) : quarantined;
    if (floor < 0 && sm.staleDatabaseAppliedFloors.isEmpty() && toResync.isEmpty()) {
      HALog.log(sm, HALog.BASIC,
          "Not retrying the targeted resync of %s: the leader holds each of them quarantined too and cannot serve them "
              + "(issue #9553)", quarantined);
      return;
    }

    final long now = System.currentTimeMillis();
    final long retryIntervalMs = sm.computeSnapshotWatchdogTimeoutMs();
    final long previous = sm.lastStaleSnapshotRetryMs.get();
    if (!leaderChanged && previous != 0 && now - previous < retryIntervalMs)
      return;
    if (!sm.lastStaleSnapshotRetryMs.compareAndSet(previous, now))
      return; // another tick won the throttle slot
    final String trigger = leaderChanged ? "a leader is known again, " : "";

    if (floor < 0 && sm.staleDatabaseAppliedFloors.isEmpty()) {
      // Only a quarantine is outstanding (issue #7735), so a TARGETED resync of each quarantined database is
      // both sufficient and far cheaper than pulling every database on the node. The full download below stays
      // the answer whenever a read floor is outstanding too, because a floor says the node-wide snapshot marker
      // itself is ahead of what was applied.
      LogManager.instance().log(sm, Level.WARNING,
          "Database(s) %s are still quarantined from the committed Raft log with no download in flight: %sretrying "
              + "the targeted resync from the leader (issues #7735, #11294)", toResync, trigger);
      if (toResync.size() < quarantined.size())
        LogManager.instance().log(sm, Level.WARNING,
            "Not retrying the targeted resync of %s: the leader holds them quarantined too and cannot serve them "
                + "(issue #9553)", quarantined.stream().filter(db -> !toResync.contains(db)).sorted().toList());
      for (final String dbName : toResync)
        sm.triggerDatabaseResync(dbName);
      return;
    }

    LogManager.instance().log(sm, Level.WARNING,
        "Local state is still behind what the snapshot marker claims (read floor=%d, databases short of the "
            + "snapshot index=%s) with no download in flight: %sretrying the resync from the leader "
            + "(issues #6111, #6760, #11294)", floor, sm.staleDatabaseAppliedFloors.keySet(), trigger);
    try {
      sm.lifecycleExecutor.submit(sm::triggerSnapshotDownload);
    } catch (final RejectedExecutionException ree) {
      LogManager.instance().log(sm, Level.WARNING,
          "Cannot schedule the stale-snapshot resync retry: executor is shut down", ree);
    }
  }

  /**
   * Highest Raft-log index whose data is genuinely present in the local databases while a flagged
   * stale-snapshot re-download is outstanding, or {@code -1} when none is (the normal case). Consumed by
   * {@link RaftHAServer#getTrustedAppliedIndex()} to clamp the LINEARIZABLE / READ_YOUR_WRITES apply
   * waiters, which would otherwise trust the marker index Ratis reports as applied (issue #6111).
   */
  public long getStaleSnapshotAppliedFloor() {
    return sm.staleSnapshotAppliedFloor.get();
  }

  /**
   * Highest Raft-log index whose data is genuinely present in {@code dbName}, or {@code -1} when this database is
   * not known to be behind (the normal case). Consumed by
   * {@link RaftHAServer#getTrustedAppliedIndex(String)} so a read of a database a snapshot install gave up on is
   * clamped, while its healthy co-located databases are not (issue #6760).
   */
  public long getDatabaseAppliedFloor(final String dbName) {
    if (dbName == null)
      return -1;
    // A floor restored from disk must clamp the very first read after a restart (issue #8137). Latched after the
    // first read, so this costs a volatile field test
    sm.appliedIndexes.ensureAppliedIndexLoaded();
    final Long floor = sm.staleDatabaseAppliedFloors.get(dbName);
    return floor != null ? floor : -1;
  }

  /**
   * Settles the diverged marks and read floors after a leader-driven snapshot install: every database it refreshed is
   * healed, and every one it did NOT bring to {@code snapshotIndex} is marked diverged with a read floor (issue #6760),
   * in one durable write (issue #8137).
   * <p>
   * Each database left behind keeps its diverged mark - so the node does not advertise readiness while it holds a copy it knows is
   * behind - and publishes a read floor at its own honest applied position, so a LINEARIZABLE or read-your-writes
   * read targeting it fails or degrades instead of being served from the stale copy. Recovery is the existing
   * machinery: the mark keeps {@link ArcadeStateMachine#isResyncInProgress()} true, and {@link #retryUnfilledSnapshotGap()} re-drives
   * the resync on the HealthMonitor tick until the database is refreshed for real.
   */
  // @VisibleForTesting
  void settleDivergedStateAfterInstall(final Set<String> notInstalled, final long snapshotIndex) {
    settleDivergedStateAfterInstall(notInstalled, snapshotIndex, false);
  }

  /**
   * {@link #settleDivergedStateAfterInstall} that also records {@code snapshotIndex} as the applied position, globally
   * and for every present database the install refreshed, in the same write - what
   * {@link ArcadeStateMachine#writePersistedAppliedIndexForAllDatabases(long, Set)} records on its own.
   */
  // @VisibleForTesting
  void completeSnapshotInstall(final long snapshotIndex, final Set<String> notInstalled) {
    completeSnapshotInstall(snapshotIndex, notInstalled, Set.of());
  }

  /**
   * Same, except that the databases in {@code notRefreshed} - kept by the install without being refreshed, the ones the
   * leader does not hold (issue #8588) - keep their own applied position too. Unlike {@code notInstalled} they are not
   * quarantined and get no read floor: the leader has no committed state for them to be behind.
   *
   * @return whether the settled state reached disk (issue #11531)
   */
  boolean completeSnapshotInstall(final long snapshotIndex, final Set<String> notInstalled, final Set<String> notRefreshed) {
    return settleDivergedStateAfterInstall(notInstalled, snapshotIndex, true, notRefreshed);
  }

  /**
   * @return whether the settled state reached disk. {@code false} leaves it in memory only, so a restart would come back
   * without the quarantines and floors published here
   */
  boolean settleDivergedStateAfterInstall(final Set<String> notInstalled, final long snapshotIndex,
      final boolean recordAppliedPositions) {
    return settleDivergedStateAfterInstall(notInstalled, snapshotIndex, recordAppliedPositions, Set.of());
  }

  boolean settleDivergedStateAfterInstall(final Set<String> notInstalled, final long snapshotIndex,
      final boolean recordAppliedPositions, final Set<String> notRefreshed) {
    final Map<String, Long> published = new LinkedHashMap<>();
    final boolean durable;
    // The final state - every database the install refreshed healed, every one it gave up on quarantined with its floor -
    // is built under the lock and written ONCE (issue #8137). Clearing everything first and re-marking the give-ups in a
    // second write left a file with neither their quarantine nor their floor in between, and a crash there restarted
    // the node ready and unclamped on the copies the install had given up on. The give-ups are never removed from the
    // in-memory maps either, so no reader sees them unclamped for an instant.
    synchronized (sm.appliedIndexes.appliedIndexFileLock) {
      sm.appliedIndexes.ensureAppliedIndexLoaded();
      if (recordAppliedPositions) {
        sm.appliedIndexes.globalAppliedIndex = snapshotIndex;
        if (sm.server != null)
          for (final String dbName : sm.server.getDatabaseNames())
            if (!notInstalled.contains(dbName) && !notRefreshed.contains(dbName))
              sm.appliedIndexes.appliedIndexByDb.put(dbName, snapshotIndex);
      }
      for (final String dbName : notInstalled) {
        // The install deliberately did NOT advance the persisted position of this database, so it still carries whatever
        // this node genuinely applied. -1 (never recorded) clamps to 0, which is the honest answer for a database
        // nothing is known about.
        final long floor = Math.max(0L, sm.appliedIndexes.readPersistedAppliedIndex(dbName));
        sm.staleDatabaseAppliedFloors.put(dbName, floor);
        // The install is the newest verdict on the database, so its cause replaces an earlier one
        sm.divergedDatabases.put(dbName, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
        published.put(dbName, floor);
      }
      sm.staleDatabaseAppliedFloors.keySet().removeIf(dbName -> !notInstalled.contains(dbName));
      sm.divergedDatabases.keySet().removeIf(dbName -> !notInstalled.contains(dbName));

      durable = sm.appliedIndexes.persistAppliedIndexFile(true);
      if (!durable && !sm.closed && !published.isEmpty())
        LogManager.instance().log(sm, Level.WARNING,
            "Database(s) %s are quarantined (%s) with their read floors but could NOT be written to %s: a restart "
                + "before this is fixed serves them ready and their LINEARIZABLE reads unclamped. Check that the .raft "
                + "directory is writable and has free space (issues #7735, #8137)", published.keySet(),
            DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE, sm.appliedIndexes.getAppliedIndexFile());
    }
    // As clearDivergedState() does after a full resync: whatever was diverged before this install has been reinstalled
    // or re-marked above
    sm.quarantine.lastDivergedResyncLogByDb.clear();
    sm.quarantine.divergedSwallowedErrors.set(0);

    for (final Map.Entry<String, Long> entry : published.entrySet()) {
      final String dbName = entry.getKey();
      final long floor = entry.getValue();
      // The cause is named above, because the alert quotes it: nothing failed while APPLYING anything here, the
      // install is what did not finish the job, and an operator sent to look for an apply error would find none
      // (issue #7741).
      LogManager.instance().log(sm, Level.SEVERE,
          "Snapshot install did not bring database '%s' to snapshotIndex=%d: keeping it marked diverged and "
              + "clamping its LINEARIZABLE / read-your-writes reads at appliedIndex=%d until a resync succeeds. "
              + "The other databases on this node are unaffected. Check the leader's copy of '%s'.",
          dbName, snapshotIndex, floor, dbName);
    }
    return durable;
  }

  /**
   * Hands leadership off when this node is the leader and has just quarantined {@code dbName} (issue #8483). The
   * resync the quarantine triggers is refused on the leader role, and since #8468 this node also refuses to serve the
   * database to the followers, so while it leads nothing can heal the database anywhere. Once a healthy peer leads,
   * this node's next health tick drives the targeted resync from it.
   */
  void handOffLeadershipIfLeader(final String dbName) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA != null && raftHA.isLeader())
      raftHA.handOffLeadershipToResync("quarantined database '" + dbName + "' (" + sm.quarantine.quarantineCause(dbName) + ")");
  }

  /**
   * The health tick's arm for a LEADER (issue #8483): while it holds a quarantine or a read floor, hand leadership off
   * so it can resync from the next leader. The backstop for what the apply paths' immediate handoff does not see - a
   * quarantine restored from disk on a node elected after the restart, a floor an incomplete snapshot install left
   * before the node was elected, a handoff that failed - so it asks on every tick and leaves the pacing to the
   * cooldown in {@link RaftHAServer#handOffLeadershipToResync}.
   */
  private void handOffLeadershipIfHeldBehind(final RaftHAServer raftHA) {
    sm.appliedIndexes.ensureAppliedIndexLoaded();
    final long floor = sm.staleSnapshotAppliedFloor.get();
    if (sm.divergedDatabases.isEmpty() && sm.staleDatabaseAppliedFloors.isEmpty() && floor < 0)
      return;

    final StringBuilder reason = new StringBuilder();
    if (!sm.divergedDatabases.isEmpty())
      reason.append("quarantined database(s) ").append(new LinkedHashMap<>(sm.divergedDatabases));
    if (!sm.staleDatabaseAppliedFloors.isEmpty())
      reason.append(reason.isEmpty() ? "" : " and ").append("database(s) short of the snapshot index ")
          .append(sm.staleDatabaseAppliedFloors.keySet());
    if (floor >= 0)
      reason.append(reason.isEmpty() ? "" : " and ").append("a node-wide read floor at ").append(floor);
    raftHA.handOffLeadershipToResync(reason.toString());
  }

  /**
   * The operator's override of issue #9498: lifts the node-wide stale-snapshot read floor (issue #6111) WITHOUT a resync,
   * accepting that the entries between the floor and the snapshot marker are not on this node, and persists the marker
   * index as the applied position so the next {@link ArcadeStateMachine#reinitialize()} does not detect the same gap again.
   * <p>
   * Every other way out of that floor is a full resync from a peer ({@link #resolveStaleSnapshotFloorAfterResync}), and
   * a leader refuses to resync from itself. On a sole voter no peer can take the leadership, so the floor keeps the node
   * not-ready and every LINEARIZABLE read clamped for good. Lifting it gives up the entries the marker claims and the
   * databases never received - the Raft log no longer holds them - which is why it is an operator decision, logged at
   * WARNING with who made it, the floor and the marker index.
   * <p>
   * A database with its own quarantine or its own read floor keeps both, and its per-database applied position: those
   * are lifted by {@link ArcadeStateMachine#acceptDivergedDatabase}, one database at a time. Every other present database is recorded at
   * the accepted position, never moved backwards.
   * <p>
   * The persistence is NOT best-effort, as in {@link ArcadeStateMachine#acceptDivergedDatabase}: an override that does not reach the disk
   * would be undone at the next restart, so on a failed write the in-memory positions are put back, the floor stays and
   * the caller is told.
   *
   * @return what was lifted, or {@code null} when no node-wide read floor stood
   *
   * @throws IllegalStateException when a snapshot download is running; it either fills the gap or leaves the floor
   * @throws IOException           when the change could not be written to the applied-index file; nothing is lifted then
   */
  StaleSnapshotAcceptance acceptStaleSnapshotFloor(final String acceptedBy) throws IOException {
    if (sm.staleSnapshotAppliedFloor.get() < 0)
      return null;
    // A download that is running owns the floor: it clears it when it lands, or re-arms it when it fails. Lifting the
    // floor under it would let the download's own completion record positions this override did not decide.
    if (sm.snapshotDownloadInProgress.get() || !sm.snapshotDownloadLock.tryLock())
      throw new IllegalStateException("a snapshot download or install is running on this node, and it either fills the "
          + "gap or leaves the read floor in place: retry once it has finished");
    final StaleSnapshotAcceptance acceptance;
    try {
      synchronized (sm.appliedIndexes.appliedIndexFileLock) {
        sm.appliedIndexes.ensureAppliedIndexLoaded();
        final long floor = sm.staleSnapshotAppliedFloor.get();
        if (floor < 0)
          return null; // filled between the check above and here
        final var snapshotInfo = sm.storage.getLatestSnapshot();
        final long snapshotIndex = snapshotInfo != null ? snapshotInfo.getIndex() : -1L;
        // The apply thread keeps advancing the global position past the marker while the floor stands: never regress it
        final long accepted = Math.max(snapshotIndex, sm.appliedIndexes.globalAppliedIndex);

        final long previousGlobal = sm.appliedIndexes.globalAppliedIndex;
        // Only the entries this override moves are remembered, so a rollback puts back exactly those: the per-database
        // map is read without the lock, and clearing it to restore a copy would show those readers an empty map
        final Map<String, Long> previousByDb = new HashMap<>();
        boolean persisted = false;
        try {
          sm.appliedIndexes.globalAppliedIndex = accepted;
          if (sm.server != null)
            for (final String dbName : sm.server.getDatabaseNames())
              if (!sm.divergedDatabases.containsKey(dbName) && !sm.staleDatabaseAppliedFloors.containsKey(dbName)) {
                final Long previousPosition = sm.appliedIndexes.appliedIndexByDb.get(dbName);
                if (previousPosition == null || previousPosition < accepted) {
                  previousByDb.put(dbName, previousPosition != null ? previousPosition : -1L);
                  sm.appliedIndexes.appliedIndexByDb.put(dbName, accepted);
                }
              }
          persisted = sm.appliedIndexes.persistAppliedIndexFile(true);
        } finally {
          if (!persisted) {
            // Whatever stopped the write - a refused rename or anything thrown on the way - nothing is lifted
            sm.appliedIndexes.globalAppliedIndex = previousGlobal;
            for (final Map.Entry<String, Long> entry : previousByDb.entrySet())
              if (entry.getValue() < 0)
                sm.appliedIndexes.appliedIndexByDb.remove(entry.getKey());
              else
                sm.appliedIndexes.appliedIndexByDb.put(entry.getKey(), entry.getValue());
          }
        }
        if (!persisted)
          throw new IOException("the change could not be written to " + sm.appliedIndexes.getAppliedIndexFile()
              + ", so the read floor would come back at the next restart: check that the .raft directory is writable "
              + "and has free space");
        // Accumulate rather than set, as the resync does: the apply thread may already have gone past the marker
        sm.lastAppliedIndex.accumulateAndGet(accepted, Math::max);
        // The queued download the gap armed has nothing left to fill: dropped, or the node stays "resync queued"
        sm.needsSnapshotDownload.set(false);
        clearStaleSnapshotFloor();
        acceptance = new StaleSnapshotAcceptance(floor, snapshotIndex, accepted);
      }
    } finally {
      sm.snapshotDownloadLock.unlock();
    }

    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA != null)
      raftHA.notifyApplied();

    LogManager.instance().log(sm, Level.WARNING,
        "%s lifted the node-wide stale-snapshot read floor WITHOUT a resync (read floor %d, snapshot marker %d, applied "
            + "position now %d). The entries between the floor and the marker are not replayed: if a database is missing "
            + "them, it stays missing. Check the databases (CHECK DATABASE) and restore any damaged one from a backup "
            + "(issue #9498)",
        acceptedBy, acceptance.readFloor(), acceptance.snapshotIndex(), acceptance.acceptedIndex());
    return acceptance;
  }

  /**
   * Re-arms a snapshot download from the leader for a follower that has been persistently lagging
   * without making progress (issue #3893). This covers the narrow window where a follower diverged
   * (apply failure) and its snapshot download also failed on a quiet cluster, so no new log entry
   * arrives to re-trigger recovery and the follower would otherwise stay diverged until restart.
   * <p>
   * Invoked by {@link HealthMonitor} after the lag has persisted for the configured duration.
   * No-op when this node is the leader, when there is no leader/server context, or when a download
   * is already pending or in progress.
   */
  public void recoverFromPersistentLag() {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null || sm.server == null || raftHA.isLeader())
      return;
    if (sm.isSnapshotDownloadPending())
      return;
    LogManager.instance().log(sm, Level.WARNING,
        "Persistent follower lag detected (applied=%d, commit=%d): re-arming snapshot download from leader",
        sm.lastAppliedIndex.get(), raftHA.getFollowerCommitIndex());
    sm.lifecycleExecutor.submit(sm::triggerSnapshotDownload);
  }
}
