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
 * The places that enforce the rule are the ones listed above and below, and that list is now complete. It used
 * to name a fourth, {@code LeaderProxy} - a transparent follower-to-leader HTTP proxy that read the exchange
 * directly - and that sentence was what a reader of this design consulted to convince themselves the rule held
 * everywhere. Nothing ever constructed the class, so no request had travelled that path (issue #7528); it has
 * been deleted rather than left standing as a fourth enforcement point that never ran.
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

  /**
   * Request header naming the Raft peer id of the node the forwarding peer believed was the leader when it dialled
   * (issue #7603). Travels beside {@link #FORWARDED_TO_LEADER_HEADER} and is honoured under the same cluster-token
   * gate. It is what lets the node that refuses a second hop tell its two causes apart - see {@link Refusal}.
   */
  public static final String FORWARDED_LEADER_ID_HEADER = "X-ArcadeDB-Forwarded-Leader-Id";

  /**
   * Request header carrying the {@code arcadedb.command.timeout} budget, in milliseconds, the forwarding node resolved
   * for the command and waits for (issue #8313). The receiving node enforces it in place of its own database setting,
   * so the two sides of a forward agree on the budget. Honoured under the same cluster-token gate as the headers
   * above: from a client it would let a request lift the budget its database imposes.
   */
  public static final String FORWARDED_COMMAND_TIMEOUT_HEADER = "X-ArcadeDB-Command-Timeout";

  /**
   * Why a request that arrived already forwarded to the leader landed on a node that is not the leader
   * (issue #7603). The one-hop refusal used to answer both causes the same way - HTTP 400 blaming
   * {@code arcadedb.ha.serverList} - which told every client not to retry a routine election.
   */
  public enum Refusal {
    /**
     * The forwarding peer dialled the node it meant to - this one - and this node is no longer the leader:
     * leadership moved while the request was in flight. Transient; the same request retried re-resolves the
     * leader from scratch.
     */
    LEADERSHIP_MOVED,
    /**
     * The forwarding peer meant another node and its address for that node reached this one instead: the HTTP
     * address it resolved for the leader does not identify the leader. A configuration fault, not transient.
     */
    ADDRESS_DOES_NOT_IDENTIFY_LEADER,
    /**
     * Either peer could not say - the forwarding peer sent no leader id (an older node during a rolling upgrade,
     * or leadership changing while it resolved the address) or this node cannot name itself. Answered as before.
     */
    UNDETERMINED
  }

  private static final ThreadLocal<Boolean> ALREADY_FORWARDED  = new ThreadLocal<>();
  private static final ThreadLocal<String>  INTENDED_LEADER_ID = new ThreadLocal<>();

  private LeaderForwardContext() {
  }

  /** Declares that the request being served on this thread was already redirected to the leader by a peer. */
  public static void markAlreadyForwarded() {
    markAlreadyForwarded(null);
  }

  /**
   * As {@link #markAlreadyForwarded()}, also recording which node the forwarding peer believed was the leader.
   *
   * @param intendedLeaderId the Raft peer id the peer dialled as the leader, or null when it did not say
   */
  public static void markAlreadyForwarded(final String intendedLeaderId) {
    ALREADY_FORWARDED.set(Boolean.TRUE);
    if (intendedLeaderId != null && !intendedLeaderId.isBlank())
      INTENDED_LEADER_ID.set(intendedLeaderId.trim());
    else
      INTENDED_LEADER_ID.remove();
  }

  /** The Raft peer id the forwarding peer meant to reach as the leader, or null when it did not say. */
  public static String intendedLeaderId() {
    return INTENDED_LEADER_ID.get();
  }

  /**
   * Why the already-forwarded request being served on this thread cannot be executed here, given this node's own
   * Raft peer id.
   *
   * @param localPeerId this node's Raft peer id, or null when it cannot name itself
   */
  public static Refusal classifyRefusal(final String localPeerId) {
    final String intended = INTENDED_LEADER_ID.get();
    if (intended == null || localPeerId == null || localPeerId.isBlank())
      return Refusal.UNDETERMINED;
    return intended.equals(localPeerId) ? Refusal.LEADERSHIP_MOVED : Refusal.ADDRESS_DOES_NOT_IDENTIFY_LEADER;
  }

  /**
   * The value of {@link #FORWARDED_LEADER_ID_HEADER} a forwarding node can send: the leader's peer id read before
   * and after the leader's address was resolved, when the two agree. When they differ leadership changed in
   * between, so the address and the id may name different nodes, and saying nothing is the only honest answer -
   * the receiver then falls back to {@link Refusal#UNDETERMINED} rather than blaming the configuration.
   */
  public static String stableLeaderId(final String leaderIdBefore, final String leaderIdAfter) {
    if (leaderIdBefore == null || leaderIdBefore.isBlank() || !leaderIdBefore.equals(leaderIdAfter))
      return null;
    return leaderIdBefore;
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
    INTENDED_LEADER_ID.remove();
  }
}
