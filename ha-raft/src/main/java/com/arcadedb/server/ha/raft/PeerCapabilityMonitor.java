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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.RaftHAServer.CapabilityProber;
import com.arcadedb.server.http.HttpServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Learns what each peer can do and where it answers: probes every peer's capabilities on a schedule, keeps the registry
 * the leader consults before it uses a feature an older peer would not understand, and confirms the HTTP address a peer
 * answers on, so a peer reached only through a derived address is learned from its own answer.
 * <p>
 * Split out of {@link RaftHAServer}, which starts the monitor with the lag monitor and stops it on shutdown.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerCapabilityMonitor {
  private final RaftHAServer ha;

  // Peer-capability discovery (issues #7219, #7549). Runs in EVERY role: a follower writes no optional
  // wire-format section, but it does serve GET /api/v1/cluster and the security compare-and-set gate of #7511,
  // and both have to answer for peers other than itself. Refreshed on its OWN scheduled thread rather than the
  // lag monitor's, because a probe round is a sequential HTTP fan-out that can take peers x PROBE_TIMEOUT_MS,
  // and replica classification must not be delayed by an unreachable peer's connect timeout - the same
  // reasoning that keeps channelRecoveryExecutor off the resync executor below.
  //
  // Guarded by capabilityMonitorLock, NOT by the field's own visibility (review of PR #7941). Since #7549 the
  // start has two callers that can run at once: the startup thread, and the Ratis leader-change callback via
  // startLagMonitor() - raftServer.start() does not wait for an election, so a node that wins one immediately
  // (a single-node bootstrap is the obvious case) reaches both. A plain check-then-act lets both see null,
  // both create an executor, and the second assignment orphan the first - a daemon thread and its probe
  // schedule that stopCapabilityMonitor() can no longer reach, which is the same leak class this file already
  // guards against for the HttpClients in shutdown().
  private          ScheduledExecutorService  capabilityMonitorExecutor;

  /** Serialises start/stop of {@link #capabilityMonitorExecutor}; never held across anything that blocks. */
  private final    Object                    capabilityMonitorLock = new Object();

  // How one capability probe is made (see CapabilityProber). A method reference rather than a lambda reading
  // arcadeServer, which is a blank final the constructor has not assigned yet at this point.
  private volatile CapabilityProber          capabilityProber = this::queryPeerCapabilities;

  // HTTP addresses offered for peers - pushed by the peer itself, relayed by another member - waiting for a probe the peer
  // answers under its own id before one is recorded in httpAddresses (issue #9255)
  private final    PeerHttpAddressCandidates peerHttpAddressCandidates = new PeerHttpAddressCandidates();

  private final Object coldRegistryRound = new Object();

  PeerCapabilityMonitor(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Starts the peer-capability refresh (issue #7219), which since issue #7549 runs on every node rather than only
   * on the leader - see {@code start()} for why, and {@link RaftHAServer#stopLagMonitor} for what stopped stopping it.
   * <p>
   * The first round runs with NO initial delay, because until it lands every peer reads as incapable and the
   * leader ships whole schema documents: correct, but it is the state the whole mechanism exists to leave, and a
   * leader that has just been elected is precisely when a burst of DDL tends to arrive.
   * <p>
   * Idempotent, and that is what keeps the invalidation below honest now that the monitor is normally already
   * running by the time leadership is acquired: {@code startLagMonitor} still calls this, and on a node whose
   * monitor never stopped it returns at the guard without clearing anything. There is nothing to invalidate
   * there - no window in which nobody was asking, which is the window #7301's clear exists to close - so the
   * warm registry is kept and a new leader can write a schema delta on its first DDL instead of waiting out a
   * refresh period.
   */
  // @VisibleForTesting - the invalidation below is asserted through this method, not through the field it clears
  void startCapabilityMonitor() {
    synchronized (capabilityMonitorLock) {
      if (capabilityMonitorExecutor != null)
        return;
      // A fresh term says nothing about what the peers can decode - a build does not change because an election
      // happened - but the advertisements were observed under the previous leadership and their timestamps are
      // what the TTL is measured against, so they are re-asked immediately rather than inherited silently. Until
      // #7301 only the log-throttle shadow was cleared and the advertisements themselves were inherited, so a
      // node that led again believed the previous term's answers - and could write an optional wire-format
      // section on them - for the length of one refresh round.
      ha.peerCapabilities.clear();
      capabilityMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "arcadedb-raft-capability-monitor");
        t.setDaemon(true);
        return t;
      });
      capabilityMonitorExecutor.scheduleWithFixedDelay(this::refreshPeerCapabilities, 0,
          PeerCapabilityRegistry.REFRESH_PERIOD_MS, TimeUnit.MILLISECONDS);
    }
  }

  /** Whether the capability refresh is running, so a test can pin that a role change does not end it (#7549). */
  // @VisibleForTesting
  boolean isCapabilityMonitorRunning() {
    synchronized (capabilityMonitorLock) {
      return capabilityMonitorExecutor != null;
    }
  }

  /**
   * Stops the peer-capability refresh. Called on shutdown - and, since issue #7549, only on shutdown: losing
   * leadership no longer ends it, because every role has a consumer for what it records.
   * <p>
   * The advertisements are deliberately left in place rather than dropped here: they age out on their own, and
   * {@code unknownReasonOf} has a sentence for exactly that window. Whatever survives it is cleared the moment
   * the monitor is started again, which is where the invalidation belongs (issue #7301).
   */
  // @VisibleForTesting
  void stopCapabilityMonitor() {
    synchronized (capabilityMonitorLock) {
      if (capabilityMonitorExecutor != null) {
        capabilityMonitorExecutor.shutdownNow();
        capabilityMonitorExecutor = null;
      }
    }
  }

  /**
   * Asks every peer in the current Raft configuration what it can decode, and records the answers (issue #7219).
   * <p>
   * Sequential on the capability-monitor thread with a short per-peer timeout, so one unreachable peer costs the
   * round {@link PeerCapabilityRegistry#PROBE_TIMEOUT_MS} and nothing else - {@code scheduleWithFixedDelay} (not
   * {@code AtFixedRate}) keeps a slow round from queueing the next one behind it.
   * <p>
   * <b>Every failure forgets rather than keeps, at the moment it fails.</b> A peer that stopped answering may
   * have been replaced by an older build, so continuing to believe its last answer until the TTL expires would be
   * believing it for a reason that no longer holds - and so would continuing to believe it merely until the end
   * of this round, which is what buffering the failure until after the second pass amounted to (issue #7331). The
   * reason is kept alongside, for {@code GET /api/v1/cluster} to report: an absent {@code capabilities} field
   * otherwise reads the same whether the peer runs an older build or was never asked at all, and those have
   * nothing in common as remedies (issue #7256). What IS deferred to the end of the round is only the log line:
   * {@link PeerCapabilityRegistry#suspend} drops the belief without settling the report shadow, and
   * {@link #forgetUnanswered} settles it for whatever the second pass did not recover.
   * <p>
   * One thing outlives an {@link PeerCapabilityRegistry.UnknownKind#UNREACHABLE} failure: the capabilities of the peer's last
   * answer, which the gate of the framed {@code TX_ENTRY} sections still reads (issue #12122). A probe that got no answer says
   * nothing about the build behind the peer id, and a peer that is merely down must not switch the #8686 refusal off for
   * every other node. Every answer replaces them, and a 404 or a refused address drops them.
   * <p>
   * <b>Two passes, because a shared address is still worth asking.</b> On a cluster that declares no {@code http}
   * ports, peers sharing a host have their endpoints derived onto one address (#6202, #6267) and
   * {@link PeerDialAddress} withholds it - correctly, for a request that acts on the peer it addressed. This one
   * does not: it is read-only, and its reply names its own author. So a second pass asks each distinct withheld
   * address ONCE and credits the answer to whichever configured peer actually answered, which is the only peer it
   * can be true of. Everything the second pass does not account for stays a "no", so the mechanism still fails the
   * cheap way; what changes is that such a cluster can reach the negotiated feature at all (issue #7256).
   * <p>
   * On the sharpest form of that shape - every node on ONE host, differing only by port - the collapsed address is
   * this node's own for every peer, and withholding it left the second pass with nothing to ask: the recovery was
   * dead on its own target until {@code getPortOffsetPeerHttpAddress} gave it a candidate to probe there
   * (issue #7332).
   */
  // @VisibleForTesting
  void refreshPeerCapabilities() {
    try {
      // The term this round belongs to, read once and handed to every write it makes. stopCapabilityMonitor()
      // does not wait for a round in flight, so this round can still be dialling when leadership is lost and come
      // back after the next term cleared the registry; the stamp is what stops it recording the previous term's
      // answer over the new one, without blocking the transition on a network timeout (issue #7314 review).
      final long generation = ha.peerCapabilities.generation();
      final List<RaftPeer> peers = ha.configuredPeers();
      final List<String> peerIds = new ArrayList<>(peers.size());
      for (final RaftPeer peer : peers)
        peerIds.add(peer.getId().toString());
      // Bound the registry by the live configuration: a long-lived leader of a cluster that has added and removed
      // peers must not accumulate their advertisements - nor what it last reported about them - for its whole
      // uptime (issue #7301).
      ha.peerCapabilities.retainOnly(generation, peerIds);
      peerHttpAddressCandidates.retainOnly(peerIds);

      final String clusterToken = ha.getClusterToken();
      // Why each peer has no answer yet, and the withheld addresses worth one more question. The belief is
      // already dropped by the time a peer lands in here (issue #7331); what this map defers is the REPORT, so a
      // peer that identifies itself in the second pass is never also WARNED ABOUT for the refusal that sent us
      // looking for it.
      final Map<String, Unanswered> unanswered = new LinkedHashMap<>();
      final Map<PeerDialAddress.SharedEndpoint, List<String>> sharedEndpoints = new LinkedHashMap<>();

      for (final RaftPeer peer : peers) {
        final RaftPeerId peerId = peer.getId();
        if (peerId.equals(ha.localPeerId))
          continue;

        // The guarded address, not the best-effort one (issues #6202, #6267): an address that resolves to the
        // wrong node would answer for a node that was never asked. PeerCapabilityQuery re-checks the peer id in
        // the reply, so the two halves of the guard are independent.
        final PeerDialAddress dial = PeerDialAddress.resolve(ha, peerId, "peer");
        if (dial.refused()) {
          // Same rule as the probe failure below (issue #7331): an address that cannot be dialled is a peer this
          // round has no answer from, so whatever it said last is dropped now rather than after pass 2.
          // ADDRESS_REFUSED, not UNREACHABLE (issue #8655): the reason is this node's configuration, so it says
          // nothing about the peer and nothing about what another node's probe will get.
          // A peer whose address is refused only because it is shared is still asked, at that address, below: the refusal is
          // this node's configuration and not the peer's answer, so what it last answered stays held for the framed sections
          // exactly as for a peer with an address of its own (issue #12122). One with no address to ask at all drops it, or
          // nothing would ever end the hold.
          suspendPeer(generation, unanswered, peerId.toString(), dial.refusal(),
              PeerCapabilityRegistry.UnknownKind.ADDRESS_REFUSED, dial.sharedEndpoint() != null);
          // A SET of the whole endpoint, so N peers collapsed onto one cost one probe and not N identical ones -
          // and two peers whose HTTP halves collide while their declared HTTPS halves do not still get a probe
          // each. Deduplicating on the HTTP address alone would have dropped the second peer's HTTPS endpoint,
          // which on an SSL cluster is the endpoint actually dialled (issue #7256).
          if (dial.sharedEndpoint() != null)
            sharedEndpoints.computeIfAbsent(dial.sharedEndpoint(), k -> new ArrayList<>(2)).add(peerId.toString());
          continue;
        }

        try {
          final PeerCapabilityQuery.Advertisement advertisement = capabilityProber.probe(peerId.toString(),
              dial.httpAddress(), dial.httpsAddress(), clusterToken);
          recordPeerCapabilities(generation, peerId.toString(), advertisement);
          learnFromAnswer(peerId.toString(), dialsPlainHttp(dial.httpsAddress()) ? dial.httpAddress() : null,
              advertisement);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          suspendPeer(generation, unanswered, peerId.toString(), "the capability query was interrupted",
              PeerCapabilityRegistry.UnknownKind.UNREACHABLE);
          forgetUnanswered(generation, unanswered);
          return;
        } catch (final Exception e) {
          // A peer running a build without the capability route answers 404 and lands here, which is exactly the
          // discriminator this mechanism turns on - so this arm is the NORMAL one during a rolling upgrade, not
          // an error.
          //
          // The belief is dropped HERE and not after the second pass (issue #7331). Buffering the whole failure
          // left freshAdvertisementOf() still answering the previous advertisement for the rest of the round, so
          // a peer that had just answered 404 - having been restarted onto an older build, the one scenario this
          // exists for - was still believed capable and could be sent a schema delta it cannot decode, for as
          // long as the second pass took. What stays buffered is only the REPORT: suspend() moves the belief
          // without settling what was last logged, so a peer the second pass identifies at a shared address is
          // neither warned about nor re-announced, which is the log churn the buffering was protecting against.
          //
          // The 404 is kept apart from every other failure (issue #8655): it is the peer's own answer, so the
          // leader's probe gets it too, and a follower reporting it can say so without its client matching the
          // message text. Anything else is this node's view of the path to the peer.
          suspendPeer(generation, unanswered, peerId.toString(), describeProbeFailure(e),
              e instanceof PeerCapabilityQuery.RouteMissingException
                  ? PeerCapabilityRegistry.UnknownKind.ROUTE_MISSING
                  : PeerCapabilityRegistry.UnknownKind.UNREACHABLE);
        }
      }

      if (!probeSharedEndpoints(generation, sharedEndpoints, peerIds, unanswered, clusterToken))
        return;
      if (!probeCandidateAddresses(generation, peers, unanswered, clusterToken))
        return;
      forgetUnanswered(generation, unanswered);
    } catch (final Exception e) {
      // Never let the scheduled task die: scheduleWithFixedDelay cancels the schedule on an escaped throwable,
      // and a cancelled refresh is a leader that silently stops re-checking its peers.
      LogManager.instance().log(ha, Level.WARNING, "Peer-capability refresh round failed: %s", e.getMessage());
    }
  }

  /**
   * Asks each address that {@link PeerDialAddress} withheld as shared, and records the answer against the peer
   * that gave it (issue #7256). Removes every peer it accounted for from {@code unanswered}.
   * <p>
   * Three conditions on the answerer, all of them about not believing an answer for a node that never gave one:
   * it has to name a peer of the current configuration, it must not be this node, and it must not already have
   * answered for itself in the first pass - a peer whose own address is unambiguous is not the peer living behind
   * a collapsed one.
   *
   * @return {@code false} when the round was interrupted and the caller must stand down without settling.
   */
  private boolean probeSharedEndpoints(final long generation,
      final Map<PeerDialAddress.SharedEndpoint, List<String>> sharedEndpoints, final List<String> peerIds,
      final Map<String, Unanswered> unanswered, final String clusterToken) {
    for (final Map.Entry<PeerDialAddress.SharedEndpoint, List<String>> shared : sharedEndpoints.entrySet()) {
      final PeerDialAddress.SharedEndpoint endpoint = shared.getKey();
      try {
        final PeerCapabilityQuery.Advertisement advertisement = capabilityProber.probe(null, endpoint.httpAddress(),
            endpoint.httpsAddress(), clusterToken);
        final String answeringPeer = advertisement.peerId();
        if (answeringPeer.equals(ha.localPeerId.toString()) || !peerIds.contains(answeringPeer)
            || !unanswered.containsKey(answeringPeer))
          continue;
        LogManager.instance().log(ha, Level.FINE,
            "Peer '%s' identified itself at the shared address %s, so its capabilities can be negotiated after all",
            answeringPeer, endpoint.httpAddress());
        unanswered.remove(answeringPeer);
        recordPeerCapabilities(generation, answeringPeer, advertisement);
        // The peer that answered here answers here: an address it no longer shares with any peer it identified (issue #9255)
        learnFromAnswer(answeringPeer, dialsPlainHttp(endpoint.httpsAddress()) ? endpoint.httpAddress() : null,
            advertisement);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        forgetUnanswered(generation, unanswered);
        return false;
      } catch (final Exception e) {
        // Nothing answers at a collapsed address that no peer is actually listening on, and a peer on an older
        // build answers 404 here exactly as it does on the guarded route. Either way the peers behind this address
        // keep the refusal already recorded against them, which is the more useful of the two reasons.
        // describeProbeFailure, not getMessage(): a bare SocketTimeoutException carries no message, and that is
        // exactly the failure the helper was written for - reporting it as "null" here left the second pass
        // saying nothing about the one failure mode that motivated it (issue #7332).
        LogManager.instance().log(ha, Level.FINE,
            "No peer identified itself at the shared address %s: %s", endpoint.httpAddress(),
            describeProbeFailure(e));
        // A 404 there is an answer from a build without the capability route, and it cannot say which peer gave it: every peer
        // behind the address gives up what it last answered, as a 404 on its own address would make it (issue #12122)
        if (e instanceof PeerCapabilityQuery.RouteMissingException)
          for (final String peerId : shared.getValue()) {
            final Unanswered held = unanswered.get(peerId);
            if (held != null && held.holdLastAnswer()) {
              ha.peerCapabilities.suspend(generation, peerId, held.reason(), held.kind(), false);
              unanswered.put(peerId, new Unanswered(held.reason(), held.kind(), false));
            }
          }
      }
    }
    return true;
  }

  /**
   * A probe failure as an operator-facing reason, never {@code null}. Some exceptions carry no message - a bare
   * {@code SocketTimeoutException} among them - and a null there would CLEAR the recorded reason rather than set
   * one, leaving the peer unknown with nothing to say why: the one thing {@code capabilitiesUnknownReason} exists
   * to provide (issue #7256).
   */
  private static String describeProbeFailure(final Exception e) {
    final String message = e.getMessage();
    return message != null && !message.isBlank() ? message : e.getClass().getSimpleName();
  }

  /**
   * Settles the REPORT for every peer this round could not get an answer for, each with the reason it failed. The
   * belief itself was already dropped at the failure (issue #7331), so this is what turns a persistent failure
   * into one log line rather than one per refresh period - {@link #forgetPeerCapabilities} answers on the
   * transition only.
   */
  private void forgetUnanswered(final long generation, final Map<String, Unanswered> unanswered) {
    for (final Map.Entry<String, Unanswered> entry : unanswered.entrySet())
      forgetPeerCapabilities(generation, entry.getKey(), entry.getValue());
  }

  /**
   * Why a peer has no answer yet this round, and whether what it last answered stays held for the framed {@code TX_ENTRY}
   * sections meanwhile (issue #12122): see {@link PeerCapabilityRegistry#forget(long, String, String, PeerCapabilityRegistry.UnknownKind, boolean)}.
   */
  private record Unanswered(String reason, PeerCapabilityRegistry.UnknownKind kind, boolean holdLastAnswer) {
  }

  /** Drops the belief in {@code peerId} now and defers its report to the end of the round (issue #7331). */
  private void suspendPeer(final long generation, final Map<String, Unanswered> unanswered,
      final String peerId, final String reason, final PeerCapabilityRegistry.UnknownKind kind) {
    suspendPeer(generation, unanswered, peerId, reason, kind, PeerCapabilityRegistry.holdsLastAnswer(kind));
  }

  private void suspendPeer(final long generation, final Map<String, Unanswered> unanswered,
      final String peerId, final String reason, final PeerCapabilityRegistry.UnknownKind kind, final boolean holdLastAnswer) {
    ha.peerCapabilities.suspend(generation, peerId, reason, kind, holdLastAnswer);
    unanswered.put(peerId, new Unanswered(reason, kind, holdLastAnswer));
  }

  private void recordPeerCapabilities(final long generation, final String peerId,
      final PeerCapabilityQuery.Advertisement advertisement) {
    if (ha.peerCapabilities.record(generation, peerId, advertisement.capabilities(), advertisement.version(),
        advertisement.serviceGap(), advertisement.quarantined(), advertisement.appliedIndex(),
        advertisement.applyBacklogBudget()))
      LogManager.instance().log(ha, Level.INFO,
          "Peer '%s' (version %s) advertises the cluster capabilities %s", peerId, advertisement.version(),
          new TreeSet<>(advertisement.capabilities()));
  }

  private void forgetPeerCapabilities(final long generation, final String peerId, final Unanswered unknown) {
    final String reason = unknown.reason();
    // Reported on the TRANSITION into the failed state and not once per refresh period: a peer that is
    // permanently on an older build is the steady state of a half-finished rolling upgrade, and a line every five
    // seconds about it would be noise. It is reported the FIRST time as well as on a regression from a known-good
    // answer, because "no peer ever answered" and "a peer stopped answering" are both things an operator who has
    // noticed their entries are not shrinking needs told - and the first of the two is otherwise completely
    // silent, which is the failure mode this whole issue is about.
    // The registry answers that question, because the shadow map deciding it has to be pruned by whatever prunes
    // the entry it shadows - held here, nothing pruned it, and a re-added peer's first advertisement was
    // suppressed against what it said before it left (issue #7301).
    if (ha.peerCapabilities.forget(generation, peerId, reason, unknown.kind(), unknown.holdLastAnswer()))
      LogManager.instance().log(ha, Level.WARNING,
          "Peer '%s' does not advertise any cluster capability (%s); optional wire-format sections will not be written to this "
              + "cluster until it answers again%s", peerId, reason,
          unknown.holdLastAnswer()
              ? ", except the framed transaction sections it advertised in its last answer (issue #12122)"
              : "");
  }

  /**
   * Whether EVERY peer in the current Raft configuration has proved it can decode {@code capability}
   * (issue #7219). False whenever any peer is unknown, unreachable, stale or explicitly without it, which is what
   * makes an optional wire-format section safe to write without an operator sequencing the upgrade by hand.
   */
  public boolean allPeersSupport(final String capability) {
    return ha.peersMissingCapability(capability).isEmpty();
  }

  /**
   * {@link #allPeersSupport} for a section ANY node may emit, which a cold registry must not turn into a "no" (issue #11196).
   * <p>
   * A node that has just started, or has just seen a peer join, has no answer for that peer yet, and the cached read says "no" to
   * it exactly as it does to an older build. The three optional TX_ENTRY sections degrade on a "no", but a degraded commit loses
   * the #8686 refusal, the #10018 counter deltas or the compact float arrays silently, and since #10040 a follower commits a
   * client-managed transaction with its own registry. So a peer that was NEVER ASKED is a third state: one bounded round is run
   * and the answer decides. A peer that answered without the capability, or could not be reached, is a settled "no" that
   * the round records a reason for, so it is not asked again here: this costs one round per cold start, not one per commit.
   * Not the synchronous round of {@link RaftHAServer#peersMissingCapabilityNow}, which would probe on every commit while an old peer is
   * part of the cluster.
   * <p>
   * Single flight: concurrent commits on a cold node wait for the one round and find the registry filled.
   * <p>
   * Worst case: the round is sequential with {@link PeerCapabilityRegistry#PROBE_TIMEOUT_MS} per unreachable peer, so a cold start
   * with N unreachable peers stalls the commits that ask for N times that, once; every probe outcome, failure included, is recorded
   * as a reason, so the next read finds none of them never asked. The same round also runs on the capability monitor and the two can
   * overlap, which is harmless for the reason given on {@link RaftHAServer#peersMissingCapabilityNow}: writes go through the registry's
   * generation guard and a capable verdict needs a peer's own answer.
   * <p>
   * {@code framedSection} reads {@link RaftHAServer#peersMissingFramedSection} instead of {@link RaftHAServer#peersMissingCapability}:
   * true for the two framed sections ({@code tx-prepared-at-index}, {@code tx-fulltext-corpus-deltas}), so a peer that is only DOWN
   * keeps what it last advertised and one node's outage does not switch them off for every other node (issue #12122); false for the
   * record-level float form, which an older build cannot read whatever the framing.
   */
  boolean allPeersSupportOnceAsked(final String capability, final boolean framedSection) {
    List<String> missing = peersMissing(capability, framedSection);
    if (missing.isEmpty())
      return true;
    if (!anyNeverAsked(missing))
      return false;

    synchronized (coldRegistryRound) {
      missing = peersMissing(capability, framedSection);
      if (missing.isEmpty())
        return true;
      if (anyNeverAsked(missing)) {
        final long startedAt = System.currentTimeMillis();
        refreshPeerCapabilities();
        missing = peersMissing(capability, framedSection);
        LogManager.instance().log(ha, Level.FINE,
            "Asked the peers never asked about the '%s' capability before a commit needed it (%d ms); still missing: %s",
            capability, System.currentTimeMillis() - startedAt, missing);
      }
    }
    return missing.isEmpty();
  }

  private List<String> peersMissing(final String capability, final boolean framedSection) {
    return framedSection ? ha.peersMissingFramedSection(capability) : ha.peersMissingCapability(capability);
  }

  private boolean anyNeverAsked(final List<String> peerIds) {
    for (final String peerId : peerIds)
      if (ha.peerCapabilities.neverAsked(peerId))
        return true;
    return false;
  }

  /**
   * Why each peer that has not proved it can decode the framed section {@code capability} has not, for a log line an operator can
   * act on (issue #11196): a peer never asked yet, one that answered without it (an older build), and one that could not be reached
   * are three different remedies. Read as the section gate reads ({@link RaftHAServer#peersMissingFramedSection}, issue #12122), so a
   * peer that is merely down and still counts as capable is not named as the reason the section is withheld. Only the two "section
   * withheld" warnings of {@link RaftReplicatedDatabase} call it; a caller for a non-framed capability must not reuse it as is.
   */
  String explainMissingCapability(final String capability) {
    final List<String> missing = ha.peersMissingFramedSection(capability);
    if (missing.isEmpty())
      return "no peer is missing it";
    final StringBuilder explanation = new StringBuilder();
    for (final String peerId : missing) {
      if (!explanation.isEmpty())
        explanation.append("; ");
      explanation.append(peerId).append(" (");
      final PeerCapabilityRegistry.Unknown unknown = ha.peerCapabilities.unknownOf(peerId);
      if (ha.peerCapabilities.freshAdvertisementOf(peerId) != null)
        explanation.append("answered without it, an older build");
      else if (unknown != null)
        explanation.append(unknown.reason());
      else
        explanation.append("has not been asked yet");
      explanation.append(')');
    }
    return explanation.toString();
  }

  /** Drops the cached {@link RaftHAServer#canStateTxPreparedAt} answer, so the next commit asks again; called when the membership changes. */
  void invalidateTxPreparedAtCapability() {
    ha.txPreparedAtCapability.invalidate();
  }

  /** Drops the cached {@link RaftHAServer#canStateFullTextCorpusDeltas} answer; called when the membership changes. */
  void invalidateFullTextCorpusDeltaCapability() {
    ha.fullTextCorpusDeltaCapability.invalidate();
  }

  /** Drops the cached {@link #canWriteFixedWidthFloatArrays} answer; called when the membership changes. */
  void invalidateFixedWidthFloatArraysCapability() {
    ha.fixedWidthFloatArraysCapability.invalidate();
  }

  /**
   * Whether every peer reads the fixed-width form of a float or double array, so a record may be written in it (issue #10856).
   * Until then the varint form is written, which every build reads: a "no" costs bytes, a wrong "yes" an unreadable record on
   * the peer that predates the form.
   */
  public boolean canWriteFixedWidthFloatArrays() {
    return ha.fixedWidthFloatArraysCapability.isCapable();
  }

  /** Substitutes how a capability probe is made, so the fan-out can be driven without a network. */
  // @VisibleForTesting
  void setCapabilityProber(final CapabilityProber prober) {
    this.capabilityProber = prober;
  }

  /** The production {@link CapabilityProber}: one HTTP call, on the guarded route or the shared-endpoint one. */
  private PeerCapabilityQuery.Advertisement queryPeerCapabilities(final String expectedPeerId,
      final String httpAddress, final String httpsAddress, final String clusterToken)
      throws IOException, InterruptedException {
    final Map<String, String> caller = capabilityRequestHeaders();
    return expectedPeerId != null
        ? PeerCapabilityQuery.fetch(expectedPeerId, httpAddress, httpsAddress, clusterToken,
            PeerCapabilityRegistry.PROBE_TIMEOUT_MS, ha.arcadeServer, ha.capabilityHttpsClients, caller)
        : PeerCapabilityQuery.fetchFromSharedEndpoint(httpAddress, httpsAddress, clusterToken,
            PeerCapabilityRegistry.PROBE_TIMEOUT_MS, ha.arcadeServer, ha.capabilityHttpsClients, caller);
  }

  /**
   * What this node says about itself, in headers, in every capability request (issue #9255): its peer id, the HTTP address it resolves
   * for itself and the port its HTTP listener is bound to. The port is the one fact about its endpoint only this node knows
   * for certain; the peer pairs it with the Raft host it already dials this node on, so the two halves of the address come
   * from where each is known.
   */
  // @VisibleForTesting
  Map<String, String> capabilityRequestHeaders() {
    final Map<String, String> headers = new LinkedHashMap<>(4);
    headers.put(PostCapabilitiesHandler.CALLER_PEER_ID_HEADER, ha.localPeerId.toString());
    final String self = ha.getLocalHttpAddress();
    if (self != null)
      headers.put(PostCapabilitiesHandler.CALLER_HTTP_ADDRESS_HEADER, self);
    final HttpServer httpServer = ha.arcadeServer.getHttpServer();
    final int httpPort = httpServer != null ? httpServer.getPort() : -1;
    if (httpPort > 0)
      headers.put(PostCapabilitiesHandler.CALLER_HTTP_PORT_HEADER, String.valueOf(httpPort));
    return headers;
  }

  /**
   * Offers the HTTP endpoint a peer stated for itself in a capability request (issue #9255): the address it resolves for
   * itself, and its listener's port on the Raft host this node knows it by. Both are candidates only - see
   * {@link #offerPeerHttpAddress}.
   */
  void offerCallerHttpAddress(final String callerPeerId, final String httpAddress, final int httpPort) {
    if (callerPeerId == null || callerPeerId.isEmpty())
      return;
    if (httpAddress != null && !httpAddress.isEmpty())
      offerPeerHttpAddress(callerPeerId, httpAddress);
    if (httpPort > 0 && httpPort <= 65535) {
      // The derive rule every node now shares: the peer's Raft host as the cluster knows it, and the peer's OWN port
      final String fromRaftHost = PeerEndpoints.deriveHttpAddress(configuredRaftAddress(RaftPeerId.valueOf(callerPeerId)), httpPort);
      if (fromRaftHost != null)
        offerPeerHttpAddress(callerPeerId, fromRaftHost);
    }
  }

  /**
   * Offers {@code address} as a candidate HTTP endpoint of {@code peerId}, to be dialled by the next capability round if
   * the peer does not answer on the address this node resolves for it (issue #9255). Dropped when it could change nothing
   * or must not: the peer is this node or not a member of the configuration, the address in force is the one this node's
   * own server list declares (the operator said it here, and nothing heard from another node outranks that - the same rule
   * {@link RaftHAServer#recordAdmittedPeerHttpAddress} applies), the address is not a {@code host:port}, or it is already the one
   * recorded.
   * <p>
   * And dropped when its host is not the peer's Raft host, the host the cluster already talks to it on (a server list
   * declares an HTTP port on that same host). A candidate is probed with the cluster token, and the probe is what proves
   * the address, so without this rule a member could have every node send the token to any {@code host:port} of its
   * choosing. What is learnt is therefore the PORT; an HTTP listener declared on another host through {@code connect
   * cluster} still reaches the node it was typed on and the leader, through the admission's seed request (#8689).
   */
  void offerPeerHttpAddress(final String peerId, final String address) {
    if (peerId == null || address == null || peerId.equals(ha.localPeerId.toString()))
      return;
    final RaftPeerId id = RaftPeerId.valueOf(peerId);
    final String current = ha.httpAddresses.get(id);
    if (address.equals(current) || isServerListAddressInForce(id, current))
      return;
    if (!PeerCapabilityQuery.isPeerAddress(address))
      return;
    final String raftAddress = configuredRaftAddress(id);
    if (raftAddress == null || !isSameHost(PeerEndpoints.extractHost(address), PeerEndpoints.extractHost(raftAddress)))
      return;
    peerHttpAddressCandidates.offer(peerId, address);
  }

  /** Host equality as {@link RaftHAServer#isSameHttpEndpoint} sees it: case-insensitive, and every loopback name is the same host. */
  static boolean isSameHost(final String host, final String other) {
    if (host == null || other == null)
      return false;
    return host.equalsIgnoreCase(other) || (LoopbackHosts.isLoopback(host) && LoopbackHosts.isLoopback(other));
  }

  /**
   * The HTTP addresses this node relays in its capability reply (issue #9255): every entry it holds for another member of
   * the configuration. Each one was declared by an operator - in a server list, a {@code connect cluster}, an admission
   * forwarded here - or confirmed by a probe the member answered: since #9255 nothing else is written there, the
   * admission-time {@code raftPort + offset} guess included.
   */
  Map<String, String> getRelayablePeerHttpAddresses() {
    Map<String, String> relayable = null;
    for (final RaftPeer peer : ha.configuredPeers()) {
      if (peer.getId().equals(ha.localPeerId))
        continue;
      final String address = ha.httpAddresses.get(peer.getId());
      if (address == null)
        continue;
      if (relayable == null)
        relayable = new LinkedHashMap<>();
      relayable.put(peer.getId().toString(), address);
    }
    return relayable != null ? relayable : Map.of();
  }

  /**
   * Records that {@code peerId} answered a capability probe dialled on {@code httpAddress} under its own id, the one proof
   * an address names that peer (issue #9255). From then on this node dials the peer there for everything, exactly as it
   * would a declared address, and relays it to the other members. The address this node's own server list declares is
   * kept while it is the one in force: the operator said it here.
   */
  private void recordConfirmedPeerHttpAddress(final RaftPeerId peerId, final String httpAddress) {
    // An answer over HTTPS confirms nothing about the HTTP address, so the candidates offered for it are kept
    if (httpAddress == null)
      return;
    peerHttpAddressCandidates.confirmed(peerId.toString());
    while (true) {
      final String current = ha.httpAddresses.get(peerId);
      if (httpAddress.equals(current) || isServerListAddressInForce(peerId, current))
        return;
      if (current == null ? ha.httpAddresses.putIfAbsent(peerId, httpAddress) == null
          : ha.httpAddresses.replace(peerId, current, httpAddress)) {
        LogManager.instance().log(ha, Level.INFO, "Peer '%s' answered on HTTP address %s%s; dialling it there from now on",
            peerId, httpAddress, current != null ? " instead of " + current : "");
        return;
      }
    }
  }

  /**
   * Whether {@code current}, the entry this node holds for {@code peerId}, is the one its own server list declares. A peer
   * that left and was admitted again has no entry until something writes one, and then the declaration no longer holds.
   */
  private boolean isServerListAddressInForce(final RaftPeerId peerId, final String current) {
    return current != null && current.equals(ha.serverListHttpAddresses.get(peerId));
  }

  /** The Raft address of {@code peerId} in the current configuration, or {@code null} when it is not a member. */
  private String configuredRaftAddress(final RaftPeerId peerId) {
    for (final RaftPeer peer : ha.configuredPeers())
      if (peer.getId().equals(peerId))
        return peer.getAddress();
    return null;
  }

  /**
   * Whether a probe given these two endpoints dials the plain-HTTP one - the only case in which its answer confirms the
   * HTTP address. The same choice {@link PeerCapabilityQuery#chooseUrl} makes.
   */
  private boolean dialsPlainHttp(final String httpsAddress) {
    return httpsAddress == null || !ha.configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
  }

  /**
   * What a capability answer from {@code peerId} teaches this node beyond the capabilities (issue #9255): that it answers on
   * {@code answeredOnHttp} (null when the probe dialled its HTTPS endpoint, which confirms nothing about the HTTP one), and
   * the addresses it relays for the other members, each offered as a candidate.
   */
  private void learnFromAnswer(final String peerId, final String answeredOnHttp,
      final PeerCapabilityQuery.Advertisement advertisement) {
    recordConfirmedPeerHttpAddress(RaftPeerId.valueOf(peerId), answeredOnHttp);
    for (final Map.Entry<String, String> relayed : advertisement.peerHttpAddresses().entrySet())
      if (!relayed.getKey().equals(peerId))
        offerPeerHttpAddress(relayed.getKey(), relayed.getValue());
  }

  /**
   * The third pass of a capability round (issue #9255): each peer still unanswered is asked on the candidate addresses
   * offered for it - its own push, a member's relay, and the derived address when what failed was a recorded one - under
   * its own id, so only the peer itself can confirm one. The first that answers is recorded as the peer's address.
   *
   * @return {@code false} when the round was interrupted and the caller must stand down without settling
   */
  private boolean probeCandidateAddresses(final long generation, final List<RaftPeer> peers,
      final Map<String, Unanswered> unanswered, final String clusterToken) {
    // A candidate is an HTTP address, and the probe carries the cluster token: on a cluster that asked for SSL the token
    // must not go over plain HTTP to an address nobody has confirmed yet. There the HTTPS endpoints are what the peers
    // dial each other on, and an HTTP address is learnt only from an answer on it, never by probing a candidate
    if (ha.configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL))
      return true;
    for (final RaftPeer peer : peers) {
      final String peerId = peer.getId().toString();
      if (!unanswered.containsKey(peerId))
        continue;
      // A recorded address that stopped answering may be stale (a peer that moved): the address derived from its Raft host
      // is worth one more question, as a candidate like any other
      final String recorded = ha.httpAddresses.get(peer.getId());
      if (recorded != null && !isServerListAddressInForce(peer.getId(), recorded)) {
        final String derived = PeerEndpoints.deriveHttpAddress(peer.getAddress(),
            ha.arcadeServer.getHttpServer() != null ? ha.arcadeServer.getHttpServer().getPort() : -1);
        if (derived != null && !RaftHAServer.isSameHttpEndpoint(ha.getLocalHttpAddress(), derived))
          offerPeerHttpAddress(peerId, derived);
      }
      for (final String candidate : peerHttpAddressCandidates.due(peerId)) {
        try {
          // No HTTPS endpoint is handed over, so the probe dials the candidate over plain HTTP whatever the SSL setting
          // (PeerCapabilityQuery.chooseUrl): its answer confirms the HTTP address and nothing else
          final PeerCapabilityQuery.Advertisement advertisement = capabilityProber.probe(peerId, candidate, null,
              clusterToken);
          unanswered.remove(peerId);
          recordPeerCapabilities(generation, peerId, advertisement);
          learnFromAnswer(peerId, candidate, advertisement);
          break;
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          forgetUnanswered(generation, unanswered);
          return false;
        } catch (final Exception e) {
          peerHttpAddressCandidates.failed(peerId, candidate);
          LogManager.instance().log(ha, Level.FINE, "Peer '%s' did not answer on the candidate HTTP address %s: %s",
              peerId, candidate, describeProbeFailure(e));
        }
      }
    }
    return true;
  }

  /** The candidate HTTP addresses held for {@code peerId}. Test seam. */
  // @VisibleForTesting
  List<String> getPeerHttpAddressCandidates(final String peerId) {
    return peerHttpAddressCandidates.all(peerId);
  }
}
