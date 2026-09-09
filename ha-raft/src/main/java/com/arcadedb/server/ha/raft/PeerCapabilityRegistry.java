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

  private final ConcurrentHashMap<String, Advertisement> advertisements = new ConcurrentHashMap<>();

  /**
   * Why each currently-unknown peer is unknown, so the cluster-status endpoint can say it (issue #7256). Purely
   * operator-facing: {@link #peersMissing} never reads it, because every unknown is a "no" whatever produced it.
   */
  private final ConcurrentHashMap<String, String>        unknownReasons = new ConcurrentHashMap<>();
  private final long                                     ttlMs;

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
   */
  public void record(final String peerId, final Set<String> capabilities, final String version) {
    advertisements.put(peerId, new Advertisement(Set.copyOf(capabilities), version, clock.getAsLong()));
    unknownReasons.remove(peerId);
  }

  /**
   * Drops what was known about {@code peerId}, so it counts as incapable from the next question on. Called when a
   * probe fails: a peer that stopped answering may have been replaced by an older build, and continuing to
   * believe its last answer until the TTL runs out would be believing it for the wrong reason.
   */
  public void forget(final String peerId, final String reason) {
    advertisements.remove(peerId);
    if (reason == null)
      unknownReasons.remove(peerId);
    else
      unknownReasons.put(peerId, reason);
  }

  /**
   * Why {@code peerId} counts as incapable, as last recorded by {@link #forget}, or {@code null} when it has an
   * answer or was never asked.
   * <p>
   * The one thing an operator can act on when a capability never arrives. A peer whose address is ambiguous is the
   * case this exists for: it fails the safe way and silently, so an absent {@code capabilities} field on
   * {@code GET /api/v1/cluster} reads identically to "this peer runs an older build", and the remedy for the two
   * is nothing alike (issue #7256).
   */
  public String unknownReasonOf(final String peerId) {
    return freshAdvertisementOf(peerId) != null ? null : unknownReasons.get(peerId);
  }

  /**
   * Forgets every peer outside {@code peerIds}, so a cluster that has removed and re-added peers over a long
   * uptime does not accumulate their advertisements for the life of the leader.
   */
  public void retainOnly(final Collection<String> peerIds) {
    final Set<String> retained = new LinkedHashSet<>(peerIds);
    advertisements.keySet().retainAll(retained);
    unknownReasons.keySet().retainAll(retained);
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
