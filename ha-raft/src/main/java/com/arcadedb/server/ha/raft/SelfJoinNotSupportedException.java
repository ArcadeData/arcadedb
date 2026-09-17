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
 * Thrown when an add-peer request names <b>this very node</b> (issue #7515).
 * <p>
 * The membership verbs act on the cluster this node already belongs to: {@code connect cluster <address>} and
 * {@code POST /api/v1/cluster/peer} both <em>add the server at the address</em> to it. An operator reaching for
 * the opposite - "make this running node join the cluster over there" - reaches for the same words, and the
 * address they then type is their own node's.
 * <p>
 * Before this, that request was accepted and did nothing: {@code RaftClusterManager.buildAddArgs} sees the id
 * already in the committed configuration and returns {@code null}, which the retry loop treats as success, so the
 * operator was answered {@code Peer ... added} and the cluster they meant to join never heard of them. The
 * idempotence that produces is deliberate and is pinned by {@code Issue7401ConnectClusterJoinsPeerIT} - but it is
 * meaningful only for <em>another</em> peer, where re-issuing a join that already committed must not fail. Adding
 * this node to its own configuration is not a join that already happened; it is a different request that has no
 * meaning, and answering it {@code 200} is what let the mistake pass as done.
 * <p>
 * <b>Why the other direction is not offered instead.</b> A running node has a Raft log of its own, and inserting
 * it into another group - whose {@code RaftGroupId} it usually shares, since the id is derived from
 * {@code arcadedb.ha.clusterName} and both sides typically leave that at the default - puts two histories under
 * one group identity, which is the split-brain the leader-side membership change exists to prevent.
 * {@code KubernetesAutoJoin} does self-insert and is safe only because it runs from {@code start()}, before this
 * node has committed anything of its own and only while it knows no leader. That is the decisive part, and it is
 * not a matter of adding a flag: a membership change may only be issued by the <em>target</em> cluster's leader,
 * so a self-join has to authenticate against a cluster this node holds no credentials for. The verb has no field
 * for them and could not safely acquire one - which is why the answer is a refusal that names the request that
 * does work, rather than a destructive "reset and bootstrap against this address" mode.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelfJoinNotSupportedException extends IllegalArgumentException {

  /**
   * @param peerId  this node's own Raft peer id, which is the id the request derived from the address it named
   * @param address the Raft address the request named, repeated because an operator with several joins in flight
   *                has to be able to tell which one was refused
   */
  public SelfJoinNotSupportedException(final String peerId, final String address) {
    super("Cannot add peer '" + peerId + "' at " + address + ": that is this node itself. " + howToActuallyJoin(address));
  }

  /**
   * The same refusal for a request that named this node's Raft <b>address</b> under a different id.
   * <p>
   * A separate message because the mistake is a different one and so is the damage. The id-matched case is a
   * no-op: {@code RaftClusterManager.buildAddArgs} finds the id already in the configuration and skips the
   * change. This case is not - no component on the path compares addresses, so the {@code Mode.ADD} COMMITS and
   * leaves the configuration holding two entries for one process, which is the split-brain the derived-id rule
   * of {@code RaftPeerAddressResolver.peerIdForAddress} exists to prevent. Naming the submitted id is what makes
   * that legible: the operator sees the id they chose next to the node it actually resolves to.
   *
   * @param peerId          this node's own Raft peer id
   * @param address         the Raft address the request named, which is this node's own
   * @param submittedPeerId the id the request asked to admit that address under
   */
  public SelfJoinNotSupportedException(final String peerId, final String address, final String submittedPeerId) {
    super("Cannot add peer '" + submittedPeerId + "' at " + address + ": that address is this node's own Raft"
        + " address, which the cluster already knows as '" + peerId + "'. Admitting it again under a second id"
        + " would leave the Raft configuration holding two entries for one process, and the two would vote"
        + " separately. " + howToActuallyJoin(address));
  }

  /**
   * The actionable half, shared by both refusals: what the verb does, and the two things that do work. Kept in
   * one place so the two messages cannot drift into telling an operator different stories.
   */
  private static String howToActuallyJoin(final String address) {
    return "Adding a peer grows THIS node's cluster with the server named by the address; it never makes this"
        + " node join another one. To make this node a member of a cluster it is not configured for, issue the"
        + " request from a server that is ALREADY a member of that cluster - 'connect cluster " + address + "',"
        + " or POST /api/v1/cluster/peer there - or declare the cluster in "
        + GlobalConfiguration.HA_SERVER_LIST.getKey() + " and restart this node. There is deliberately no runtime"
        + " self-join: a membership change is issued by the target cluster's leader, so this node cannot commit"
        + " one into a cluster it is not yet part of, and a running node carries a Raft log of its own that"
        + " inserting it into a foreign group would place under a second history.";
  }
}
