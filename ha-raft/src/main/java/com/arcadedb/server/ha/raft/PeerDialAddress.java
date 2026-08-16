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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.logging.Level;

/**
 * The endpoint one cluster node may dial another on, or the reason there is none. Either {@link #httpAddress()}
 * is set - with {@link #httpsAddress()} alongside it when an encrypted one is available and passes the same
 * checks - or {@link #refusal()} is, never both.
 * <p>
 * A resolved peer address is only as good as the configuration behind it. With no {@code http} port declared in
 * {@link GlobalConfiguration#HA_SERVER_LIST}, a peer's HTTP endpoint is <em>derived</em> as its Raft host plus
 * <em>this</em> node's HTTP port (see {@code RaftHAServer.resolveHttpAddress}), so on a cluster whose nodes
 * differ by port rather than by host every peer collapses onto the address of the node doing the resolving, and
 * on one with mixed ports it can name a real peer that is not the one meant. Every caller that acts on such an
 * address unattended has to ask the same two questions before it dials - "does this address identify the peer I
 * mean?" and "is it my own?" - and they were being answered separately at each call site, in versions that had
 * already drifted apart (issues #6191, #6202, #6204, #6221).
 * <p>
 * They are answered here once, next to the resolution, so a new caller inherits the rule instead of restating
 * it: {@code ArcadeStateMachine} before a snapshot resync, and {@code PostVerifyDatabaseHandler} before it fans
 * a consistency check out to a peer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record PeerDialAddress(String httpAddress, String httpsAddress, String refusal) {

  /** A refusal carrying {@code reason}, phrased to be appended to a caller's "refusing to ..." log line. */
  public static PeerDialAddress refuse(final String reason) {
    return new PeerDialAddress(null, null, reason);
  }

  /** True when there is no address this node may dial, and {@link #refusal()} says why. */
  public boolean refused() {
    return httpAddress == null;
  }

  /**
   * The address {@code peerId} may be dialled on, or the reason it may not be dialled at all.
   * <p>
   * Four refusals, in the order that keeps the message the most specific:
   * <ul>
   * <li><b>There is no peer.</b> A {@code null} id - an unknown leader, an empty group - has no address, and
   * "unresolvable" would read as a configuration fault rather than as "nobody has been elected yet".</li>
   * <li><b>The peer is this node.</b> Dialling it would come straight back here. Cheap, exact, and independent
   * of how the address resolves.</li>
   * <li><b>No address identifies the peer on its own.</b> {@link RaftHAServer#getUnambiguousPeerHttpAddress}
   * withholds an address that two peers both resolve to, because two listening sockets cannot both own one
   * {@code host:port}: it identifies at most one of them and the resolver has no way to say which.</li>
   * <li><b>The address is this node's own.</b> The backstop for the second refusal, which the peer id alone
   * cannot cover: a <em>declared</em> address outranks a derived one, so a peer that declares the endpoint this
   * node actually listens on is handed out as unambiguous and still resolves to us.</li>
   * </ul>
   * The address is compared through {@link RaftHAServer#isSameHttpEndpoint}, the same comparison the
   * write-forwarding path makes, so the two cannot drift apart.
   * <p>
   * A caller that prefers an encrypted endpoint gets one in {@link #httpsAddress()} - {@code null} when SSL is
   * off, when no HTTPS endpoint resolves, or when the one that does fails these same checks. It is asked
   * separately rather than inferred from the HTTP verdict, because the two are read from independent fields of
   * {@link GlobalConfiguration#HA_SERVER_LIST} (the 3rd and the 5th) with independent derive fallbacks: a cluster
   * that declares distinct {@code http} ports and omits the {@code https} one passes the HTTP check with every
   * peer's HTTPS endpoint still collapsed onto this node's own. Withheld, it reads to the caller exactly like an
   * absent one, so the dial falls back to the guarded plain-HTTP endpoint - the listener that is always there -
   * rather than being refused outright (issue #6221).
   *
   * @param role how the peer is named in the refusal - {@code "leader"} where the peer is being dialled as one,
   *             {@code "peer"} otherwise. Operator-facing text only; it changes no decision.
   */
  public static PeerDialAddress resolve(final RaftHAServer raft, final RaftPeerId peerId, final String role) {
    if (peerId == null)
      return refuse("the " + role + " is unknown");

    if (peerId.equals(raft.getLocalPeerId()))
      return refuse("the " + role + " " + peerId + " is this node itself, and dialling it would come straight back here");

    final String httpAddress = raft.getUnambiguousPeerHttpAddress(peerId);
    if (httpAddress == null)
      return refuse("no HTTP address identifies " + role + " " + peerId + " on its own - it is either unresolvable "
          + "or shared with another peer, and a request sent to the wrong node answers for a node that was never "
          + "asked. Declare each node's 'http' port explicitly in " + GlobalConfiguration.HA_SERVER_LIST.getKey()
          + " (issue #6202)");

    // Resolved once and compared once: reading it twice would let the two comparisons disagree.
    final String localHttpAddress = raft.getLocalHttpAddress();
    if (localHttpAddress == null)
      // The resolver degrades to null when this node's own HTTP endpoint cannot be resolved right now (the HTTP
      // listener is not up yet). Say so rather than letting the backstop no-op invisibly.
      LogManager.instance().log(PeerDialAddress.class, Level.WARNING,
          "Cannot resolve this node's own HTTP address; the self-dial check is inactive for this attempt and only "
              + "the peer-id check guards it (issue #6191)");
    else if (RaftHAServer.isSameHttpEndpoint(localHttpAddress, httpAddress))
      return refuse("the resolved " + role + " address " + httpAddress + " is this node's own, so dialling it would "
          + "come straight back here. Declare each node's 'http' port explicitly in "
          + GlobalConfiguration.HA_SERVER_LIST.getKey() + " (issue #6191)");

    return new PeerDialAddress(httpAddress, encryptedEndpointOf(raft, peerId), null);
  }

  /**
   * The peer's HTTPS endpoint when it passes the same two checks the HTTP one just did, {@code null} otherwise.
   * <p>
   * Silent where the HTTP arm logs: this one degrading to {@code null} is not a refusal - the caller falls back to
   * the plain-HTTP endpoint it was already handed - and a line per attempt for an SSL cluster that simply has not
   * declared its {@code https} ports would say the same thing the resolver's own one-time INFO already says.
   */
  private static String encryptedEndpointOf(final RaftHAServer raft, final RaftPeerId peerId) {
    final String httpsAddress = raft.getUnambiguousPeerHttpsAddress(peerId);
    if (httpsAddress == null)
      return null;
    final String localHttpsAddress = raft.getLocalHttpsAddress();
    return localHttpsAddress != null && RaftHAServer.isSameHttpEndpoint(localHttpsAddress, httpsAddress)
        ? null : httpsAddress;
  }
}
