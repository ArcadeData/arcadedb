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

import com.arcadedb.server.ArcadeDBServer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The bootstrap passes in progress on this node: a pass the leader announced and has not concluded yet, during which the
 * databases it decides are neither served nor reported as healthy. Together with the installs in flight they make the
 * bootstrap window that the readiness probe and the client gates report.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which applies the entries that announce and conclude a pass.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BootstrapPasses {
  private final ArcadeStateMachine sm;

  /**
   * Databases a first-formation bootstrap pass has told this node it is deciding on, and that the pass has not
   * settled here yet (issue #8368).
   * <p>
   * {@link BootstrapInstaller#bootstrapInstallsInFlight} opens only once the committed baseline reaches this node's apply thread, but
   * the pass starts well before that: the leader probes every peer, elects a source, possibly moves leadership, and
   * only then commits. For that whole stretch a copy the baseline is about to reject was served, and reported
   * ready, with nothing local saying a pass was running. The pass's own probe is that local signal: the leader
   * already reaches this node with it, so holding on it costs no extra round trip, and neither readiness nor the
   * request path ever waits on a peer.
   * <p>
   * Released by whichever comes first: the baseline for the database applied here ({@link
   * #applyBootstrapFingerprintEntry}, which hands over to the install holder when the copy is replaced), the
   * leader's conclusion of the pass for a database it committed no baseline for ({@link #concludeBootstrapPass}),
   * or the entry's deadline - a leader that dies mid-pass sends no conclusion, and a hold nothing releases would wedge
   * the node out of the Service for good.
   */
  final ConcurrentHashMap<String, PendingBootstrapPass> bootstrapPassesPending = new ConcurrentHashMap<>();

  // Upper bound on a single hold, so a misconfigured bootstrap timeout cannot turn the deadline into "never" (and
  // TimeUnit.toNanos saturating near Long.MAX_VALUE cannot overflow nanoTime() + hold into the past).
  private static final long MAX_BOOTSTRAP_PASS_HOLD_MS = TimeUnit.HOURS.toMillis(1);

  BootstrapPasses(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * The databases a first-formation bootstrap pass is still deciding on here, sorted (issue #8408): the arm of
   * {@link #bootstrapWindowReason()} that starts the window, before the baseline reaches this node. A database whose
   * install is already in flight is not listed - {@link ArcadeStateMachine#getBootstrapInstallsInFlight()} has taken over from the pass
   * for it - and neither is one this node does not hold, since there is nothing served in its place to hold back.
   * <p>
   * Package-private, and read by {@code ClusterAlerts.NodeStatus} so {@code GET /api/v1/cluster} publishes it as the
   * {@code bootstrap-deciding-databases} alert and the {@code bootstrapDeciding} member: the readiness body counted
   * these databases and pointed at that document for the other two arms, and left them out for this one.
   */
  List<String> getBootstrapPassesDeciding() {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    // The overwhelmingly common answer, and this is on the readiness-probe path: allocate nothing for it.
    if (bootstrapPassesPending.isEmpty())
      return Collections.emptyList();
    List<String> names = null;
    for (final String dbName : bootstrapPassesPending.keySet())
      if (!sm.bootstrapInstalls.bootstrapInstallsInFlight.containsKey(dbName) && isBootstrapPassPending(dbName) && sm.isDatabasePresentLocally(dbName)) {
        if (names == null)
          names = new ArrayList<>();
        names.add(dbName);
      }
    if (names == null)
      return Collections.emptyList();
    Collections.sort(names);
    return names;
  }

  /**
   * A first-formation bootstrap pass {@code passId} is deciding which copy of {@code dbNames} the cluster keeps
   * (issue #8368): hold each of them until the pass settles it here, for at most {@code holdMs}.
   * <p>
   * Ignored on a node past first formation - the pass never runs there, so an announce can only be a stray - and for
   * a database whose baseline this node already applied, which the pass has settled. The baseline is re-checked
   * after the hold is taken because the apply thread can record it and release the hold between the first check and
   * the put, which would otherwise leave a hold that only the deadline clears.
   * <p>
   * A repeated announce from the same pass replaces the hold and so restarts its deadline. That renewal is bounded by
   * the leader, not here: {@code BootstrapElection.collectRemoteStatesWithRetry} re-probes only a peer that has not
   * answered yet, and only inside its {@code arcadedb.ha.bootstrapTimeoutMs} budget, so the last renewal lands within
   * that budget of the pass starting and the hold ends at most {@code holdMs} after it.
   */
  void announceBootstrapPass(final String passId, final Collection<String> dbNames, final long holdMs) {
    // A negative hold can only come from an overflowed configuration (2 x an absurd bootstrap timeout): fail towards
    // holding, which costs availability, rather than towards not holding, which is the bug this hold exists for.
    final long hold = holdMs < 0 ? MAX_BOOTSTRAP_PASS_HOLD_MS : Math.min(holdMs, MAX_BOOTSTRAP_PASS_HOLD_MS);
    hold(passId, dbNames,
        new PendingBootstrapPass(passId, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(hold), false));
  }

  /**
   * The leader running pass {@code passId} holds its own copies of {@code dbNames} while it decides (issue #8409), with
   * NO deadline. The deadline of {@link #announceBootstrapPass} exists because a leader that dies mid-pass sends its
   * followers no conclusion; this hold is taken and settled by the pass itself - {@code BootstrapElection.runIfEligible}
   * concludes it on every exit, and replaces it with a bounded announce before a transfer - so a deadline, even the
   * one-hour ceiling, could only lapse while the pass is still fingerprinting or collecting, and let a client reach a
   * copy the pass may reject. It is in memory only, so a crash takes it with the process.
   */
  void holdOwnBootstrapPass(final String passId, final Collection<String> dbNames) {
    hold(passId, dbNames, new PendingBootstrapPass(passId, 0L, true));
  }

  private void hold(final String passId, final Collection<String> dbNames, final PendingBootstrapPass pending) {
    if (passId == null || dbNames == null || dbNames.isEmpty() || !sm.hasNeverAppliedApplicationEntry())
      return;
    sm.baselines.ensureBootstrapBaselinesLoaded();
    for (final String dbName : dbNames) {
      if (dbName == null || ArcadeDBServer.isReservedDatabaseName(dbName) || sm.baselines.bootstrapBaselines.containsKey(dbName))
        continue;
      bootstrapPassesPending.put(dbName, pending);
      // No lock spans the two maps, and none is needed: the apply thread records the baseline BEFORE it removes the
      // hold, and each ConcurrentHashMap operation is a happens-before edge. So either the apply's remove comes after
      // this put and clears it, or it came before - in which case the baseline it recorded earlier is visible here.
      if (sm.baselines.bootstrapBaselines.containsKey(dbName))
        bootstrapPassesPending.remove(dbName, pending);
    }
  }

  /**
   * The pass {@code passId} has finished on the leader (issue #8368): release every database it holds here except
   * those it committed a baseline for that this node has not applied yet - that entry is still on its way, and its
   * apply is what releases the hold. A {@code null} {@code passId} concludes whichever pass holds a database, which
   * is what the node running a pass does for its own holds.
   * <p>
   * A hold taken by a DIFFERENT pass is left alone: the conclusion of a pass that lost leadership mid-flight can
   * land after the next leader's pass announced, and must not release what the newer one holds.
   */
  void concludeBootstrapPass(final String passId, final Collection<String> committed) {
    if (bootstrapPassesPending.isEmpty())
      return;
    sm.baselines.ensureBootstrapBaselinesLoaded();
    final Set<String> committedNames = committed == null || committed.isEmpty() ? Set.of() : new HashSet<>(committed);
    for (final Map.Entry<String, PendingBootstrapPass> entry : bootstrapPassesPending.entrySet()) {
      final String dbName = entry.getKey();
      final PendingBootstrapPass pending = entry.getValue();
      if (passId != null && !passId.equals(pending.passId()))
        continue;
      if (!committedNames.contains(dbName) || sm.baselines.bootstrapBaselines.containsKey(dbName))
        bootstrapPassesPending.remove(dbName, pending);
    }
  }

  /**
   * Whether a first-formation bootstrap pass is still deciding on {@code dbName} here (issue #8368) - see
   * {@link #bootstrapPassesPending}. Asked on every client request, so the common answer - no pass at all - costs
   * one map read; a lapsed hold is dropped by the first reader that sees it.
   */
  public boolean isBootstrapPassPending(final String dbName) {
    if (bootstrapPassesPending.isEmpty())
      return false;
    final PendingBootstrapPass pending = bootstrapPassesPending.get(dbName);
    if (pending == null)
      return false;
    if (pending.unbounded() || System.nanoTime() - pending.deadlineNanos() < 0)
      return true;
    bootstrapPassesPending.remove(dbName, pending);
    return false;
  }

  /**
   * Why this node must not be in the load balancer's pool because of the cluster's first-formation bootstrap, or
   * {@code null} when it may be (issue #7519).
   * <p>
   * The question it answers is narrower than "is anything bootstrap-related going on": <b>is this node serving a
   * copy of a database that the cluster's committed baseline did not adopt?</b> Two conditions make that true, and
   * they are the two halves of the same window:
   * <ul>
   *   <li><b>A held copy is being replaced.</b> This node's copy of the database did not match the committed
   *       baseline and is being replaced wholesale from the leader. The copy on disk is the one the cluster decided
   *       against for as long as that takes, and it is open and serving on every protocol for the length of the
   *       download - the node-wide {@code snapshotInstallInProgress} 503 covers only the swap at the end of it,
   *       and only HTTP.</li>
   *   <li><b>A copy was refused and never reconciled.</b> The "local is fresher, refuse to overwrite" branch
   *       kept this node's own copy on purpose, and its file-id space is assigned by an independent history from
   *       that point on (issue #6124). It is not a transient state - it survives restarts in
   *       {@code .raft/bootstrap-baselines} - and until an operator or an automatic remedy replaces the copy,
   *       everything this node serves for that database is data the cluster never adopted.</li>
   * </ul>
   * A third condition covers the start of the same window (issue #8368): <b>a pass is still deciding.</b> The pass
   * reaches this node with its probe long before the baseline reaches its apply thread, and a copy it is about to
   * reject is served in between. See {@link #bootstrapPassesPending} for what opens and closes it. It is published
   * by name like the other two, as the {@code bootstrap-deciding-databases} alert and the {@code bootstrapDeciding}
   * member (issue #8408).
   * <p>
   * <b>A database this node does not hold is none of them</b> (issue #8045). The #7298 replay-skip marks a database it
   * found gone and could not pull back in the same unreconciled set, and reinstalls it through the same install
   * path - but nothing is being served in the cluster's stead, so there is nothing to take out of the Service for.
   * Counting it held a node with nine good databases out of the Service for good over the tenth, under a remedy
   * ("discards the local copy") for a copy that does not exist, while the SEVERE line and the
   * {@code bootstrap-database-missing} alert told the operator the node was serving everything else. Both halves
   * are therefore classified by where the database is NOW, with the same presence test
   * {@link ArcadeStateMachine#getBootstrapUnreconciled(Set)} uses, and only a present copy counts.
   * <p>
   * Both conditions are recoverable, and both are cleared exactly where this node's copy is actually replaced by
   * the cluster's, so a node that recovers rejoins the Service on its own. That is the same shape as
   * {@link ArcadeStateMachine#getRaftLogFailure()}, the other terminal-until-remedied condition the readiness probe consults
   * unconditionally.
   * <p>
   * <b>It counts the databases and does not name them.</b> {@code GET /api/v1/ready} is the one route that
   * answers without authentication ({@code GetReadyHandler.isRequireAuthentication()} returns false), and this
   * string is its response body, so a name here is a database name handed to anything that can reach the port.
   * The authenticated {@code GET /api/v1/cluster} publishes both by name, filtered to the databases the caller may
   * see: the kept copies as the {@code bootstrap-diverged-databases} alert, and the installs as the
   * {@code bootstrap-install-in-progress} alert and the {@code bootstrapInstalls} member (issue #8044 - until then
   * the install half was published nowhere, and this sentence was not true of it), and the databases a pass is still
   * deciding on as the {@code bootstrap-deciding-databases} alert and the {@code bootstrapDeciding} member (issue
   * #8408). The sibling gates make the
   * same distinction without stating it: the log failure reports a log index and the security-convergence gate
   * reports document kinds.
   */
  public String bootstrapWindowReason() {
    // Not getBootstrapUnreconciled(): that allocates two lists and sorts a copy, and this runs on every readiness
    // probe. The empty checks come first so the overwhelmingly common answer stats no file.
    sm.baselines.ensureBootstrapBaselinesLoaded();
    if (sm.bootstrapInstalls.bootstrapInstallsInFlight.isEmpty() && sm.baselines.bootstrapUnreconciledDatabases.isEmpty() && bootstrapPassesPending.isEmpty())
      return null;

    final int replacing = countPresentLocally(sm.bootstrapInstalls.bootstrapInstallsInFlight.keySet());
    final int kept = countPresentLocally(sm.baselines.bootstrapUnreconciledDatabases);
    // Issue #8368: the start of the window, before the baseline reaches this node. A database already counted as
    // being replaced is not counted again - the install holder has taken over from the pass for it.
    final int deciding = getBootstrapPassesDeciding().size();
    if (replacing == 0 && kept == 0 && deciding == 0)
      return null;

    // BOTH are reported when both hold, rather than the first one found (review of PR #7964). They are different
    // databases in different states - an install replacing database A while database B sits unreconciled - and an
    // operator who reads only the install would go on believing the node comes back by itself when the install
    // finishes, which is the one case where it does not.
    final StringBuilder reason = new StringBuilder(256);
    if (deciding > 0)
      reason.append("The cluster's first-formation bootstrap is deciding which copy of ").append(deciding)
          .append(" database(s) on this node the cluster keeps: until its baseline reaches this node, what is on "
              + "disk may be the copy it decides against, so this node must not serve it.");
    if (replacing > 0) {
      if (deciding > 0)
        reason.append(' ');
      reason.append("The cluster's first-formation bootstrap is replacing ").append(replacing)
          .append(" database(s) on this node from the leader's snapshot: what is on disk is the copy the cluster's "
              + "committed baseline decided against, so this node must not serve it.");
    }
    if (kept > 0) {
      if (replacing > 0 || deciding > 0)
        reason.append(' ');
      reason.append(kept)
          .append(" database(s) on this node hold a copy the cluster's committed bootstrap baseline did not adopt, "
              + "and nothing has reconciled them since: their file ids are out of step with every other peer. "
              + "POST /api/v1/cluster/resync/<database> on this node discards the local copy and adopts the "
              + "leader's.");
    }
    // Said once, whichever arm fired: the authenticated route is where the names are, and it is the answer to "which
    // databases" for every condition alike - the deciding arm too since issue #8408, as the bootstrap-deciding-databases
    // alert and the bootstrapDeciding member.
    return reason.append(" GET /api/v1/cluster names them.").toString();
  }

  /**
   * How many of {@code names} this node holds a copy of, by the same test {@link ArcadeStateMachine#getBootstrapUnreconciled(Set)}
   * classifies on (issue #8045). A live view is fine to iterate: both backing collections are concurrent, and a
   * name added or removed mid-count only moves the answer to the next probe.
   */
  private int countPresentLocally(final Iterable<String> names) {
    int count = 0;
    for (final String name : names)
      if (sm.isDatabasePresentLocally(name))
        ++count;
    return count;
  }

  /**
   * The pass holding a database ({@code passId}) and when the hold lapses on its own ({@link System#nanoTime()}), or
   * {@code unbounded} for the hold the leader running the pass takes on its own copies, which only its pass settles
   * (issue #8409).
   */
  private record PendingBootstrapPass(String passId, long deadlineNanos, boolean unbounded) {
  }
}
