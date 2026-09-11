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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * What the leader knows about each peer's decoding capabilities, and how long it is entitled to keep believing it
 * (issue #7219).
 * <p>
 * A pure cache: something else does the asking ({@link RaftHAServer#refreshPeerCapabilities} over
 * {@link PeerCapabilityQuery}), and this holds the answers with the timestamp at which each was observed. Keeping
 * the two apart is what lets every arm of the decision be pinned without a cluster - the arms that matter here
 * are the ones that are invisible when they misfire, exactly like {@code RaftReplicatedDatabase.baseIsUsable}.
 * <p>
 * It also owns what was last REPORTED about each peer, which is why {@link #record} and {@link #forget} answer a
 * boolean: the caller logs a transition, and the shadow that decides what counts as one has to be dropped by the
 * same call that drops the entry it shadows (issue #7301). {@link #suspend} is the one write that moves the
 * belief without settling the report, for a failure the same round may still recover from (issue #7331).
 *
 * <h2>Every unknown is a "no"</h2>
 *
 * {@link #peersMissing} treats these as identical, because the consequence of getting any of them wrong is the
 * same silent divergence:
 * <ul>
 *   <li>a peer never probed (just added to the configuration, or its address is ambiguous so
 *       {@link PeerDialAddress} refuses to dial it at all);</li>
 *   <li>a peer whose probe failed - unreachable, or running a build with no capability route, which answers a
 *       non-200 and is the discriminator this whole mechanism turns on;</li>
 *   <li>a peer whose last answer is older than {@link #ttlMs}.</li>
 * </ul>
 *
 * <h2>Why the answer expires</h2>
 *
 * A capability is a property of the build a peer is CURRENTLY running, and that can go backwards: an operator
 * rolling back one pod puts an older build behind an id whose advertisement the leader already believes. The TTL
 * bounds how long that window lasts - the leader stops shipping deltas to it within {@link #ttlMs} of the last
 * successful probe - and it is why an advertisement is never cached indefinitely even though a peer's build
 * changes only on restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PeerCapabilityRegistry {

  /** How often the leader re-asks every peer in its configuration. */
  static final long REFRESH_PERIOD_MS = 5_000L;

  /**
   * Per-peer probe timeout. Short on purpose: the fan-out is sequential on one scheduled thread, so the whole
   * round costs at most {@code peers x this}, and a peer that cannot answer in two seconds is reported unknown -
   * which only ever costs a whole-document schema entry.
   */
  static final long PROBE_TIMEOUT_MS = 2_000L;

  /**
   * How long an advertisement is believed. Four refresh periods, so a peer has to miss several rounds - a GC
   * pause, a brief network blip - before the leader gives up on it, while a genuine downgrade stops being
   * believed within twenty seconds.
   */
  static final long ADVERTISEMENT_TTL_MS = 4 * REFRESH_PERIOD_MS;

  /** One peer's last successful answer. */
  public record Advertisement(Set<String> capabilities, String version, long observedAtMs) {
  }

  /**
   * Sentinel stored in {@link #lastReported} for a peer whose last probe FAILED, so a repeated failure is not a
   * change and is not reported again. Never a real capability set: no advertisement can contain this token.
   */
  private static final Set<String> PROBE_FAILED = Set.of("<probe-failed>");

  private final ConcurrentHashMap<String, Advertisement> advertisements = new ConcurrentHashMap<>();

  /**
   * What was last REPORTED about each peer, so the caller logs a transition rather than a line per refresh round
   * for a state that has not moved. Deliberately owned here rather than by the caller: it shadows
   * {@link #advertisements} entry for entry, and when the caller held it separately nothing pruned it in
   * {@link #retainOnly} - so a peer that was removed and later re-added had its first advertisement suppressed as
   * "unchanged", which is the one advertisement an operator most wants to see (issue #7301).
   */
  private final ConcurrentHashMap<String, Set<String>>   lastReported   = new ConcurrentHashMap<>();

  /**
   * Why each currently-unknown peer is unknown, so the cluster-status endpoint can say it (issue #7256). Purely
   * operator-facing: {@link #peersMissing} never reads it, because every unknown is a "no" whatever produced it.
   */
  private final ConcurrentHashMap<String, String>        unknownReasons = new ConcurrentHashMap<>();
  private final long                                     ttlMs;

  /**
   * Which leadership term's answers this registry currently holds, bumped by {@link #clear()}.
   * <p>
   * {@code stopCapabilityMonitor} ends a refresh with {@code shutdownNow()} and does not wait for the round in
   * flight, so a probe that was already dialling when leadership was lost can come back AFTER the next term's
   * {@link #clear()} and record the previous term's answer over it - re-opening precisely the window the clear
   * exists to close. Every write is stamped with the generation its round started in and is dropped when that no
   * longer matches, which closes it without blocking a leadership transition on a network timeout. Read and
   * compared under {@link #writeLock}, so the check and the write cannot be separated by a clear.
   */
  private long generation;

  /** Serialises the generation check against the writes it guards; never held across anything that blocks. */
  private final Object writeLock = new Object();

  // Injectable clock for deterministic tests; defaults to the wall clock. Volatile because a test thread writes
  // it while the capability-refresh thread reads it (consistent with ClusterMonitor's clock).
  private volatile LongSupplier clock = System::currentTimeMillis;

  public PeerCapabilityRegistry() {
    this(ADVERTISEMENT_TTL_MS);
  }

  // @VisibleForTesting
  PeerCapabilityRegistry(final long ttlMs) {
    this.ttlMs = ttlMs;
  }

  // @VisibleForTesting
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  /**
   * Records what {@code peerId} just answered. The capability set is copied and frozen, so a caller reusing its
   * parsing buffer cannot mutate a recorded answer.
   *
   * @param generation the value {@link #generation()} gave when this round of probing started; an answer from an
   *                   ended leadership term is dropped rather than recorded.
   *
   * @return true when this answer differs from the last one reported for {@code peerId} - including the first
   * answer of all, and the first after a removal - so the caller can log a transition rather than a line every
   * refresh period for a state that has not moved. False also when the answer was dropped as out of term, which
   * is not a transition worth logging either.
   */
  public boolean record(final long generation, final String peerId, final Set<String> capabilities,
      final String version) {
    final Set<String> frozen = Set.copyOf(capabilities);
    synchronized (writeLock) {
      if (generation != this.generation)
        return false;
      advertisements.put(peerId, new Advertisement(frozen, version, clock.getAsLong()));
      unknownReasons.remove(peerId);
      return !frozen.equals(lastReported.put(peerId, frozen));
    }
  }

  /**
   * Drops what was known about {@code peerId}, so it counts as incapable from the next question on. Called when a
   * probe fails: a peer that stopped answering may have been replaced by an older build, and continuing to
   * believe its last answer until the TTL runs out would be believing it for the wrong reason.
   *
   * @param generation the value {@link #generation()} gave when this round of probing started; a failure from an
   *                   ended leadership term is dropped rather than recorded.
   *
   * @return true on the TRANSITION into the failed state, false while it persists or when the round is out of
   * term. Reported the first time as well as on a regression from a known-good answer, because "no peer ever
   * answered" and "a peer stopped answering" are both things an operator needs told.
   */
  public boolean forget(final long generation, final String peerId, final String reason) {
    synchronized (writeLock) {
      if (generation != this.generation)
        return false;
      advertisements.remove(peerId);
      if (reason == null)
        unknownReasons.remove(peerId);
      else
        unknownReasons.put(peerId, reason);
      return lastReported.put(peerId, PROBE_FAILED) != PROBE_FAILED;
    }
  }

  /**
   * Drops what was known about {@code peerId} WITHOUT settling what was last reported about it, so the caller can
   * stop believing a failed probe at the moment it fails and still decide, later in the same round, whether the
   * failure is worth a log line (issue #7331).
   * <p>
   * The split exists because the two halves of {@link #forget} answer to different clocks. The advertisement has
   * to go the instant a probe fails - a peer that stopped answering may have been replaced by an older build, and
   * {@link #freshAdvertisementOf} is what decides whether a schema delta is written to it, so holding the previous
   * answer for the rest of the round is exactly the window this mechanism exists to close. The report shadow, on
   * the other hand, has to survive until the round has finished looking: a peer the second pass reaches at a
   * shared address answered after all, and settling the shadow on its behalf in pass 1 would make the round log a
   * "does not advertise" warning and a re-advertisement every five seconds, forever, on a cluster whose whole
   * configuration dials through one collapsed address.
   *
   * @param generation the value {@link #generation()} gave when this round of probing started; a failure from an
   *                   ended leadership term is dropped rather than recorded.
   */
  public void suspend(final long generation, final String peerId, final String reason) {
    synchronized (writeLock) {
      if (generation != this.generation)
        return;
      advertisements.remove(peerId);
      if (reason == null)
        unknownReasons.remove(peerId);
      else
        unknownReasons.put(peerId, reason);
    }
  }

  /**
   * Forgets everything, with nothing left to report as unchanged. Called when this node ACQUIRES leadership
   * (issue #7301): the advertisements in here were observed under a previous leadership term and their
   * timestamps are what the TTL is measured against, so a node that leads again would otherwise believe the
   * previous term's answers until its first refresh round completes - the one window in which an optional
   * wire-format section could be written to a peer whose capabilities have not been re-confirmed. A build does
   * not change because an election happened, so nothing here is lost that the first round does not restore; what
   * is dropped is the entitlement to act on it before that round has run.
   */
  public void clear() {
    synchronized (writeLock) {
      generation++;
      advertisements.clear();
      unknownReasons.clear();
      lastReported.clear();
    }
  }

  /**
   * The generation a round of writes belongs to, read once when the round starts and handed back to every
   * {@link #record}, {@link #forget} and {@link #retainOnly} it makes. A round whose generation has moved on was
   * started by a leadership term that has ended, and its answers are not this term's to believe.
   */
  public long generation() {
    synchronized (writeLock) {
      return generation;
    }
  }

  /**
   * Why {@code peerId} counts as incapable, or {@code null} when it has a fresh answer or was never asked at all.
   * <p>
   * The one thing an operator can act on when a capability never arrives. A peer whose address is ambiguous is the
   * case this exists for: it fails the safe way and silently, so an absent {@code capabilities} field on
   * {@code GET /api/v1/cluster} reads identically to "this peer runs an older build", and the remedy for the two
   * is nothing alike (issue #7256).
   * <p>
   * Covers the THIRD unknown as well as the two {@link #forget} records. An answer that simply aged out with no
   * failed probe behind it means the leader stopped asking rather than the peer stopped answering - a node that
   * lost leadership and regained it has a window of exactly that shape, because {@code stopCapabilityMonitor}
   * ends the refresh while the advertisements it took stay in this map. Reporting nothing there would leave the
   * one arm of "every unknown is a no" that no reason describes.
   */
  public String unknownReasonOf(final String peerId) {
    if (freshAdvertisementOf(peerId) != null)
      return null;
    final String reason = unknownReasons.get(peerId);
    if (reason != null)
      return reason;
    return advertisements.containsKey(peerId)
        ? "this peer's last advertisement is older than the " + ttlMs + "ms one is believed for, and no probe has "
            + "refreshed it since"
        : null;
  }

  /**
   * Forgets every peer outside {@code peerIds}, so a cluster that has removed and re-added peers over a long
   * uptime does not accumulate their advertisements for the life of the leader.
   */
  public void retainOnly(final long generation, final Collection<String> peerIds) {
    final Set<String> retained = new LinkedHashSet<>(peerIds);
    synchronized (writeLock) {
      if (generation != this.generation)
        // A previous term's configuration is not this term's, and pruning by it could drop a peer this term has
        // already asked about.
        return;
      advertisements.keySet().retainAll(retained);
      unknownReasons.keySet().retainAll(retained);
      // The report shadow goes with them, or a re-added peer's first advertisement is suppressed as "unchanged"
      // against what it said before it left (issue #7301).
      lastReported.keySet().retainAll(retained);
    }
  }

  /** {@code peerId}'s last answer if it is still within the TTL, {@code null} when unknown or expired. */
  public Advertisement freshAdvertisementOf(final String peerId) {
    final Advertisement advertisement = advertisements.get(peerId);
    if (advertisement == null)
      return null;
    return clock.getAsLong() - advertisement.observedAtMs() <= ttlMs ? advertisement : null;
  }

  /**
   * The peers of {@code peerIds} that have NOT proved they support {@code capability}, in the order given, so the
   * caller can both decide and say which peer decided it. Empty means every peer is covered - including the
   * vacuous case of a cluster with no other peer, where there is nobody left who could fail to understand the
   * bytes.
   */
  public List<String> peersMissing(final Collection<String> peerIds, final String capability) {
    final List<String> missing = new ArrayList<>();
    for (final String peerId : peerIds) {
      final Advertisement advertisement = freshAdvertisementOf(peerId);
      if (advertisement == null || !advertisement.capabilities().contains(capability))
        missing.add(peerId);
    }
    return missing.isEmpty() ? Collections.emptyList() : missing;
  }

  /** Whether every peer of {@code peerIds} has proved it supports {@code capability}. */
  public boolean allPeersSupport(final Collection<String> peerIds, final String capability) {
    return peersMissing(peerIds, capability).isEmpty();
  }
}
