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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The HTTP addresses this node has been offered for each peer, waiting to be confirmed by a capability probe that the
 * peer answers under its own id (issue #9255).
 * <p>
 * A peer's HTTP endpoint is declared in {@code arcadedb.ha.serverList} or it is not, and when it is not every node used
 * to fill the gap its own way: the node that served an admission wrote a {@code raftPort + offset} guess, the leader
 * derived the peer's Raft host plus its own HTTP port, and a declaration made on one node reached at most one other.
 * Two nodes could therefore dial a member on two different addresses, and one of them was wrong (issues #9229, #9230).
 * Every node now learns the same thing from the same source: the peer states its own HTTP port and address in every
 * capability request it sends (a push), and every node relays the addresses it has confirmed or was told by an operator
 * in its capability reply. Neither is believed on its own word. An offered address is only a candidate, held here
 * until a probe dialled on it is answered by the peer it was offered for - the binding {@link PeerCapabilityQuery}
 * already enforces - and only then is it recorded as that peer's address.
 * <p>
 * Bounded on both axes, because the offers arrive on every capability round from every peer: at most
 * {@link #MAX_PER_PEER} addresses per peer (the oldest goes first), and a candidate whose probe failed is not dialled
 * again for {@link #RETRY_AFTER_FAILURE_MS}, whatever re-offers it - a relay repeats the same stale address every
 * round, and a down peer would otherwise cost a probe timeout per candidate per round.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerHttpAddressCandidates {

  /** The most candidates held for one peer: its own push (two forms) and a relay or two. */
  static final int  MAX_PER_PEER           = 4;
  /** How long a candidate whose probe failed is left alone. */
  static final long RETRY_AFTER_FAILURE_MS = 60_000L;

  // peer id -> candidate address -> the time from which it may be dialled (0 = never tried). Each per-peer map is only
  // touched inside ConcurrentHashMap.compute for its key, which is what makes the LinkedHashMap safe to mutate
  private final ConcurrentHashMap<String, LinkedHashMap<String, Long>> candidates = new ConcurrentHashMap<>();
  private volatile LongSupplier clock = System::currentTimeMillis;

  // @VisibleForTesting
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  /**
   * Offers {@code address} for {@code peerId}. An address already held keeps its back-off: a relay re-offers the same
   * one every round, and resetting it would undo the bound on failed probes.
   */
  void offer(final String peerId, final String address) {
    candidates.compute(peerId, (id, held) -> {
      final LinkedHashMap<String, Long> map = held != null ? held : new LinkedHashMap<>();
      if (!map.containsKey(address)) {
        map.put(address, 0L);
        if (map.size() > MAX_PER_PEER) {
          final Iterator<String> eldest = map.keySet().iterator();
          eldest.next();
          eldest.remove();
        }
      }
      return map;
    });
  }

  /** The candidates for {@code peerId} that may be dialled now, oldest offer first. */
  List<String> due(final String peerId) {
    final List<String> due = new ArrayList<>(2);
    final long now = clock.getAsLong();
    candidates.computeIfPresent(peerId, (id, map) -> {
      for (final Map.Entry<String, Long> entry : map.entrySet())
        if (entry.getValue() <= now)
          due.add(entry.getKey());
      return map;
    });
    return due.isEmpty() ? Collections.emptyList() : due;
  }

  /** The probe dialled on {@code address} was not answered by {@code peerId}: leave it alone for a while. */
  void failed(final String peerId, final String address) {
    final long retryAt = clock.getAsLong() + RETRY_AFTER_FAILURE_MS;
    candidates.computeIfPresent(peerId, (id, map) -> {
      map.computeIfPresent(address, (a, previous) -> retryAt);
      return map;
    });
  }

  /** {@code peerId} answered on an address this node now records: nothing offered for it is needed any more. */
  void confirmed(final String peerId) {
    candidates.remove(peerId);
  }

  /** Forgets every peer outside {@code peerIds}, so a peer that left takes its candidates with it. */
  void retainOnly(final Collection<String> peerIds) {
    final Set<String> retained = peerIds instanceof Set<String> set ? set : new HashSet<>(peerIds);
    candidates.keySet().retainAll(retained);
  }

  /** Every candidate held for {@code peerId}, due or not. For tests and for the cluster report. */
  List<String> all(final String peerId) {
    final List<String> all = new ArrayList<>();
    candidates.computeIfPresent(peerId, (id, map) -> {
      all.addAll(map.keySet());
      return map;
    });
    return all;
  }
}
