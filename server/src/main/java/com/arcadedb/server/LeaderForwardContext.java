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
package com.arcadedb.server;

/**
 * Records, for the duration of one HTTP request, that a cluster peer already redirected this request to
 * what it believed to be the leader. A node serving a marked request must execute it or refuse it, never
 * redirect it a second time (issue #6191).
 * <p>
 * Every follower-to-leader redirect resolves the leader's HTTP endpoint, and that endpoint is only as
 * good as the cluster configuration behind it: when {@code arcadedb.ha.serverList} declares no {@code http}
 * port the address is <em>derived</em> from the peer's Raft host plus <em>this</em> node's HTTP port, which
 * on a cluster whose nodes differ by port rather than by host resolves every peer - the leader included - to
 * the address of the node doing the resolving. The redirect then lands back on a follower, which resolves
 * the same wrong address and redirects again. Nothing in the exchange itself says the request has been here
 * before, so the cycle is bounded only by HTTP timeouts and the size of the worker pool.
 * <p>
 * The marker travels as the {@link #FORWARDED_TO_LEADER_HEADER} request header, set by every
 * follower-to-leader redirect. {@code AbstractServerHttpHandler} publishes it onto this thread-local at the
 * request boundary and clears it in a finally block, because one of the redirect decisions is taken deep
 * inside the engine ({@code RaftReplicatedDatabase.command}) where the HTTP exchange is no longer in reach.
 * <p>
 * It is honored <em>only</em> on a request carrying a valid {@code X-ArcadeDB-Cluster-Token}, because the
 * marker is a statement one node makes to another. Trusting it from an ordinary client request would cost
 * nothing in safety - its only possible effect is a refusal, never execution on a node that is not the leader
 * - but it would let any caller (or a proxy that copies unknown {@code X-ArcadeDB-*} headers through) turn
 * its own transparent forward-to-leader into a {@code ServerIsNotTheLeaderException}.
 * <p>
 * That gate is on the cluster token as a <em>proof of hop</em>, not on the token having replaced the caller's
 * identity. The distinction is what issue #7516 turns on: a server command relayed with a client's own
 * Basic-auth or API-token header keeps those headers - resolving the user by name on the leader would discard
 * the scopes an API token carries - and so sends the cluster token beside them rather than instead of them.
 * Before that split it sent no token, and therefore no marker, which left the dial-side self-address check as
 * its only bound. That check catches the reachable misconfiguration of issue #6191 (an undeclared {@code http}
 * port, where every peer derives to this node's own address) but not a hand-written or stale
 * {@code arcadedb.ha.serverList} in which two peers name each other: there each node dials the <em>other</em>
 * one, so no node ever recognizes its own address and the request ping-pongs.
 * <p>
 * {@code LeaderProxy} is written to enforce a one-hop rule of its own for the requests it relays, reading the
 * exchange directly since it still has one - but nothing constructs it ({@code grep -rn "new LeaderProxy"}
 * has no hit under {@code src/main}), so no request has ever travelled that path. Do not count it as one of
 * the places the rule is enforced; issue #7551 tracks reviving or deleting it.
 * <p>
 * The rule generalizes past the follower-to-leader direction, and so does the marker: the cluster-verify
 * endpoint's leader-to-peer fan-out sets it too (issue #6221), because a peer address that names the wrong node
 * sends a fan-out back to a leader that fans it out again - the same cycle, multiplying by (N-1) per level
 * instead of by one, with a full CRC of the database at every hop. What the marker states is what every one of
 * these paths needs to know: a cluster peer already relayed this request, so this node answers it or refuses it,
 * and never relays it on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LeaderForwardContext {
  /** Request header a node sets when it redirects a request to the leader on a client's behalf. */
  public static final String FORWARDED_TO_LEADER_HEADER = "X-ArcadeDB-Forwarded-To-Leader";

  private static final ThreadLocal<Boolean> ALREADY_FORWARDED = new ThreadLocal<>();

  private LeaderForwardContext() {
  }

  /** Declares that the request being served on this thread was already redirected to the leader by a peer. */
  public static void markAlreadyForwarded() {
    ALREADY_FORWARDED.set(Boolean.TRUE);
  }

  /**
   * True when the request being served on this thread arrived already redirected to the leader, so
   * redirecting it again would send it round a cycle instead of to a node that can execute it.
   */
  public static boolean isAlreadyForwarded() {
    return Boolean.TRUE.equals(ALREADY_FORWARDED.get());
  }

  /** Clears the marker. Must run in a finally block: HTTP worker threads are pooled and reused. */
  public static void clear() {
    ALREADY_FORWARDED.remove();
  }
}
