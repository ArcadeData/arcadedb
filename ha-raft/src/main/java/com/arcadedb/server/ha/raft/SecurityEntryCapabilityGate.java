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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;

import java.util.List;
import java.util.logging.Level;

/**
 * Refuses to submit a Raft log entry whose TYPE BYTE a peer of the cluster cannot decode (issue #7511).
 *
 * <h2>Why a refusal and not a fallback</h2>
 *
 * {@link PeerCapabilities} was built for #7219, where the thing being negotiated is an optional trailing section
 * of {@code SCHEMA_ENTRY}: a peer that cannot read it still reads the entry, so the leader has somewhere to
 * degrade to - it ships the whole document and logs. An entry TYPE has no such fallback. Either the entry is
 * written, and {@code ArcadeStateMachine} on the older peer halts the node rather than skip a committed entry it
 * cannot decode (the #4798 rule, and the right rule: skipping would diverge the cluster's security state
 * silently), or the entry is not written at all.
 * <p>
 * So this gate is the only place the choice can be made, and it makes it before anything is submitted: on a
 * refusal nothing reaches the Raft log, nothing halts, and the caller's own state is untouched - the request is
 * reissued unchanged once the last node is upgraded.
 *
 * <h2>Which types need a token</h2>
 *
 * {@link #capabilityFor} is an exhaustive {@code switch} over {@link RaftLogEntryType} with no {@code default},
 * on purpose: adding a ninth constant does not compile until whoever adds it has decided whether a peer has to
 * advertise something before the leader may write one. That is the half of #7511 that outlives #7373 - the issue
 * asked for a mechanism that "would cover the next entry type too", and a switch that fails the build is a
 * stronger guarantee than a sentence in a document.
 *
 * <h2>Every unknown is a no, and what that costs</h2>
 *
 * {@link RaftHAServer#peersMissingCapabilityNow} treats unreachable, never-probed, stale and explicitly-incapable
 * as one answer, because at the transport an old build and an unreachable node ARE one answer: a 404 or a
 * timeout. That is what makes the gate safe, and it is also what makes it strict - a group change or a token
 * revocation is refused while any node is DOWN, not only while any node is OLD.
 * {@code arcadedb.ha.securityEntryCapabilityGate} is the escape hatch for an operator who knows, from outside the
 * cluster, that the node that cannot be probed does understand the entry; the refusal names it.
 * <p>
 * The second cost is latency, and it is wider than the gated operations. Every caller of this gate holds the
 * {@code ServerSecurity} monitor across it, and that monitor is shared with USER administration - so an
 * unreachable peer makes the probe round delay the next {@code createUser} as much as the next group change (PR
 * #7555 review). Bounded by {@link PeerCapabilityRegistry#PROBE_TIMEOUT_MS} per peer, paid only when the cached
 * answer is not already a full "yes", and logged at FINE by {@link RaftHAServer#peersMissingCapabilityNow} so the
 * stall is attributable rather than mysterious.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SecurityEntryCapabilityGate {

  private SecurityEntryCapabilityGate() {
    // utility class
  }

  /**
   * The capability a peer must advertise before this cluster may write {@code type}, or {@code null} when the
   * type needs none.
   * <p>
   * {@code null} is returned for the types that predate the mechanism, each for a reason that was checked rather
   * than assumed:
   * <ul>
   *   <li>{@code TX_ENTRY}, {@code SCHEMA_ENTRY}, {@code INSTALL_DATABASE_ENTRY}, {@code DROP_DATABASE_ENTRY}
   *       and {@code SECURITY_USERS_ENTRY} (ids 1-5) came in with the Raft HA work itself, so no build that has
   *       ever spoken this protocol fails to decode them. {@code SCHEMA_ENTRY}'s optional delta section is
   *       negotiated separately, by {@link PeerCapabilities#SCHEMA_DELTA}, and degrades rather than refuses.</li>
   *   <li>{@code BOOTSTRAP_FINGERPRINT_ENTRY} (id 6) shipped in 26.5.1 and is written only at first cluster
   *       formation, by {@code BootstrapElection}. It is deliberately NOT gated: refusing it would stop the
   *       cluster forming rather than protect a peer, which is the opposite of what this gate is for.</li>
   * </ul>
   */
  // @VisibleForTesting
  static String capabilityFor(final RaftLogEntryType type) {
    return switch (type) {
      case SECURITY_GROUPS_ENTRY -> PeerCapabilities.SECURITY_GROUPS_ENTRY;
      case SECURITY_API_TOKENS_ENTRY -> PeerCapabilities.SECURITY_API_TOKENS_ENTRY;
      case TX_ENTRY, SCHEMA_ENTRY, INSTALL_DATABASE_ENTRY, DROP_DATABASE_ENTRY, SECURITY_USERS_ENTRY,
          BOOTSTRAP_FINGERPRINT_ENTRY -> null;
    };
  }

  /**
   * Throws unless every peer of the current Raft configuration has proved it can decode {@code type}.
   *
   * @param server the local server, read for {@code arcadedb.ha.securityEntryCapabilityGate}. A {@code null}
   *               server reads the setting's default, which is on: a gate that fails open because a field was
   *               not wired is the failure this class exists to prevent.
   * @param raft   the Raft server whose configuration is asked. Never {@code null} here - the caller has already
   *               refused the submit without one.
   * @param type   the entry type about to be written.
   * @param what   what the caller is replicating, named the way an operator would name it ("group document"), for
   *               the refusal message.
   *
   * @throws ClusterCapabilityNotReadyException when at least one peer has not advertised the capability
   */
  static void requireEveryPeerCanDecode(final ArcadeDBServer server, final RaftHAServer raft,
      final RaftLogEntryType type, final String what) {
    final String capability = capabilityFor(type);
    if (capability == null)
      return;

    if (!gateEnabled(server)) {
      // Throttling would be wrong here: this is not a periodic condition, it is one line per admin operation
      // deliberately performed with the interlock off, and each one is a change that may halt a node.
      LogManager.instance().log(SecurityEntryCapabilityGate.class, Level.WARNING,
          "Replicating the %s with the cluster-capability interlock disabled (%s=false): any peer that cannot "
              + "decode a %s HALTS when it applies this entry, and stays halted until it is upgraded (issue #7511)",
          what, GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey(), type);
      return;
    }

    final List<String> missing = raft.peersMissingCapabilityNow(capability);
    if (missing.isEmpty())
      return;

    // The peers and the token travel as FIELDS as well as in the message: production mode conceals the message
    // (AbstractServerHttpHandler.buildErrorBody drops 'detail'), and a refusal that could not name the lagging
    // node is the silence this issue exists to end (PR #7555 review).
    throw new ClusterCapabilityNotReadyException(
        refusal(what, type, capability, missing, raft.getPeerCapabilityRegistry()), capability, missing);
  }

  /**
   * The refusal an operator reads, built from the peers that withheld the answer and the reason each is unknown.
   * <p>
   * Pure, and separated from the decision above, so the sentence an operator is left holding can be pinned by a
   * test: it is the entire remedy this issue delivers, and a message that named neither the peer nor the setting
   * would leave them exactly where the silent halt did.
   */
  // @VisibleForTesting
  static String refusal(final String what, final RaftLogEntryType type, final String capability,
      final List<String> missingPeers, final PeerCapabilityRegistry registry) {
    final StringBuilder message = new StringBuilder(512);
    message.append("Refusing to replicate the ").append(what)
        .append(": peer(s) ").append(missingPeers)
        .append(" have not advertised the '").append(capability)
        .append("' capability, so they cannot decode a ").append(type)
        .append(" and would HALT on applying it rather than skip a committed entry (issue #7511). Nothing was "
            + "submitted, so nothing has changed anywhere in the cluster.");

    for (final String peer : missingPeers) {
      final String reason = registry != null ? registry.unknownReasonOf(peer) : null;
      if (reason != null)
        message.append(" Peer '").append(peer).append("': ").append(reason).append('.');
    }

    message.append(" Finish the rolling upgrade - or restore contact with those peers - and reissue this request; "
            + "it succeeds unchanged once every node advertises the capability, with no sequencing by hand. "
            + "Set ").append(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey())
        .append("=false only to submit it anyway, knowing that a peer which genuinely cannot decode it will halt.");

    return message.toString();
  }

  /** Reads the interlock off the SERVER configuration, the scope the setting is declared in. */
  private static boolean gateEnabled(final ArcadeDBServer server) {
    if (server == null || server.getConfiguration() == null)
      return GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getValueAsBoolean();
    return server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE);
  }
}
