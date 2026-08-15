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
 * It is honored whoever sent it - a follower forwards under the cluster token, but the server-command path
 * also relays a client's own Basic/API-token credentials - because its only effect on a receiving node is to
 * refuse the redirect: a client setting the header on its own request can have that request refused, never
 * executed on a node that is not the leader.
 * <p>
 * {@code LeaderProxy} enforces the same one-hop rule for the requests it relays, reading the exchange
 * directly since it still has one.
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
