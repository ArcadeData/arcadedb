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

import java.net.http.HttpRequest;

/**
 * The credential every peer-to-peer dial in this module presents, in one place.
 * <p>
 * It is two headers that only mean anything together. {@code X-ArcadeDB-Cluster-Token} proves the HOP - it is
 * the shared secret only cluster members hold - and that is all it proves; {@code X-ArcadeDB-Forwarded-User}
 * names the principal, and {@code AbstractServerHttpHandler} reads it only inside the branch a valid token
 * opens. Sent apart, the first is a hop with no identity and the second is a claim with no proof.
 * <p>
 * <b>Why a helper rather than two lines at each site.</b> Issue #7837 was exactly one such site written by
 * hand and written wrong: the remote shutdown presented the token as {@code Authorization: Bearer}, which no
 * handler authenticates, so the POST was answered 401 while the operator was told the peer had stopped. There
 * were six other dials spelling the pair out correctly at the time, which is what made the wrong one hard to
 * see. A dial that calls this cannot get it wrong, and a new dial that does not call it is visible in review
 * (code review on PR #7854).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerCredentials {

  private PeerCredentials() {
  }

  /**
   * Attaches the pair to {@code request}, as the root principal every cluster-internal RPC forwards as.
   * <p>
   * A blank or absent token attaches NEITHER header. That is the correct request for a cluster configured
   * without one - it carries no credentials at all, which a server that does not require authentication
   * accepts and one that does refuses - rather than a principal nobody can verify.
   *
   * @param clusterToken this node's cluster token, or {@code null}/blank when the cluster has none
   *
   * @return {@code request}, for chaining
   */
  static HttpRequest.Builder attach(final HttpRequest.Builder request, final String clusterToken) {
    return attachAs(request, clusterToken, RaftHAServer.FORWARDED_ROOT_USER);
  }

  /**
   * {@link #attach} naming a principal other than root, for the one dial that relays the caller's own identity
   * rather than acting for the cluster ({@code PostVerifyDatabaseHandler}'s fan-out).
   */
  static HttpRequest.Builder attachAs(final HttpRequest.Builder request, final String clusterToken,
      final String forwardedUser) {
    if (clusterToken != null && !clusterToken.isBlank()) {
      request.header("X-ArcadeDB-Cluster-Token", clusterToken);
      request.header("X-ArcadeDB-Forwarded-User", forwardedUser);
    }
    return request;
  }
}
