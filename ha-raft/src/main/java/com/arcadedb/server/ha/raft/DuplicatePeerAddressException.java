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

/**
 * Thrown when an add-peer request would put a <b>second Raft peer id on an address the configuration already
 * holds</b> (issue #7515).
 * <p>
 * The membership change compares peers by id, and only by id: {@code RaftClusterManager.buildAddArgs} skips the
 * add when the id is already a member, and issues a {@code Mode.ADD} otherwise. Nothing on that path looks at the
 * address, so a request naming an existing member's address under a new id is not recognized as a duplicate - it
 * COMMITS, and the configuration then carries two entries for one process. Those two entries vote separately,
 * which inflates the quorum a proposal needs while only one process is there to answer, and no later membership
 * change can tell which of them is real.
 * <p>
 * This is the general form of what {@link SelfJoinNotSupportedException} refuses for this node in particular. The
 * self case gets its own type and its own message because it has a cause worth naming - an operator reaching for
 * "make this node join that cluster" - while this one is an id/address pair that simply does not describe a new
 * server. Both are {@link IllegalArgumentException}s, so both reach the caller as HTTP 400 / gRPC
 * {@code INVALID_ARGUMENT} through the mappers already keyed on that type.
 * <p>
 * <b>Textual, deliberately.</b> The comparison is {@code RaftHAServer.isSameHttpEndpoint} - exact host match or
 * two loopback spellings, with equal ports - and it does not resolve host names. Resolving them would refuse
 * legitimate joins wherever distinct nodes share a resolved address as seen from the node doing the resolving,
 * which is ordinary on Kubernetes and behind NAT, and it would put an unbounded resolver call on an HTTP worker
 * thread - the held-thread regression issue #7562 removed from this very path, where a name is now resolved only
 * inside the probe's own budget. What this catches is therefore the reachable mistake: the same address written
 * the same way, or written {@code localhost} where the configuration says {@code 127.0.0.1}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DuplicatePeerAddressException extends IllegalArgumentException {

  /**
   * @param peerId         the id the request asked to admit
   * @param address        the Raft address it named
   * @param existingPeerId the id the configuration already holds that address under
   */
  public DuplicatePeerAddressException(final String peerId, final String address, final String existingPeerId) {
    super("Cannot add peer '" + peerId + "' at " + address + ": the cluster already holds that address as peer '"
        + existingPeerId + "'. A Raft configuration identifies a server by its id, so admitting one address twice"
        + " would leave two entries for one process: they would vote separately, so a proposal would need a"
        + " majority of a membership larger than the set of servers that can answer it, and no later removal"
        + " could say which entry is the real one. If you meant to re-add '" + existingPeerId + "', re-issue the"
        + " request with that id - the membership change is idempotent for a peer that is already a member. If"
        + " you meant a different server, give it its own address: two servers cannot share one Raft port.");
  }
}
