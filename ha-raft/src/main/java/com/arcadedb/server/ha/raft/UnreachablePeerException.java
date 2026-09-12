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

/**
 * Thrown when an add-peer request names a Raft address that accepts no connection (issue #7514).
 * <p>
 * An {@link IllegalArgumentException} because that is the type both shared error mappers already key on
 * to answer a client error - {@code AbstractServerHttpHandler} maps it to HTTP 400 and
 * {@code ArcadeDbGrpcAdminService} to gRPC {@code INVALID_ARGUMENT} - and a client error is what this is:
 * the request named a server that is not there, which the server cannot fix by being asked again. The
 * previous behaviour, a {@code ConfigurationException} after the membership-change retry budget, reached
 * the caller as HTTP 500 and told them the opposite.
 * <p>
 * A distinct subtype rather than a bare {@code IllegalArgumentException} so the class name the wire
 * contract carries ({@code exception} in the HTTP body, the {@code arcadedb-exception-class} trailer over
 * gRPC) says which argument was rejected and why.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class UnreachablePeerException extends IllegalArgumentException {

  /**
   * @param peerId    the id the peer would have joined under
   * @param address   its Raft address, named because an operator with several joins in flight has to be
   *                  able to tell which one was refused
   * @param reason    what the probe saw, from {@link PeerReachability#unreachableReason}
   * @param probedBy  the node that ran the probe, or {@code null} when it is not known. Worth naming
   *                  because the probe runs wherever the request landed, and neither add-peer route is
   *                  leader-routed: a request that reached a FOLLOWER is answered from that follower's
   *                  view of the network, which is not necessarily the leader's. Without the node in the
   *                  message, "but the peer is up" and "but it works from the leader" are the same
   *                  sentence
   * @param timeoutMs the probe budget, named alongside the setting that owns it so the one case this can
   *                  get wrong - a network slower than the budget - is self-diagnosing
   */
  public UnreachablePeerException(final String peerId, final String address, final String reason,
      final String probedBy, final long timeoutMs) {
    super("Cannot add peer '" + peerId + "': nothing answered at its Raft address " + address + " (" + reason
        + ")" + (probedBy != null ? ", probed from node '" + probedBy + "'" : "")
        + ". The server there has to be running before it can join, because Raft does not commit the"
        + " membership change until the new peer has caught up with the leader's log. Start it, then reissue"
        + " the request. If the address is reachable but slower than the " + timeoutMs + " ms probe budget,"
        + " raise " + GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT.getKey() + " (0 disables the probe).");
  }
}
