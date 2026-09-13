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

import java.util.Set;

/**
 * The vocabulary of wire-format capabilities one cluster node advertises to another, and the set THIS build
 * advertises (issue #7219).
 * <p>
 * A Raft log entry carries no version field - the type byte is the whole envelope (see the module's
 * {@code CLAUDE.md}) - so a leader cannot tell from the protocol whether a follower understands an optional
 * section it is about to write. Before this existed, the answer was an operator instruction: "upgrade every node
 * first, then turn {@code arcadedb.ha.schemaDelta} on". Get the order wrong and a pre-delta follower applied an
 * empty schema change and diverged with no signal.
 * <p>
 * A capability is a short, stable token. It names what a receiver can DECODE, never what it prefers or what it is
 * configured to do: the question the leader asks is "would this peer understand the bytes I am about to write",
 * and a peer that can read a section must advertise it whether or not it would ever produce one.
 * <p>
 * <b>Tokens are permanent.</b> A token that has shipped keeps its exact spelling for as long as any supported
 * version might advertise it; renaming one makes every older peer read as incapable, which is safe but silently
 * turns the feature off across a whole cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PeerCapabilities {

  /**
   * This node decodes the trailing schema-delta section of {@code SCHEMA_ENTRY} (issue #6989).
   * <p>
   * Decoding it has been unconditional since the section existed, so advertising this token is a statement about
   * the BUILD and not about {@code arcadedb.ha.schemaDelta}: a node with the setting off still decodes a delta
   * happily, and must say so, or a cluster could never turn the feature on one node at a time.
   */
  public static final String SCHEMA_DELTA = "schema-delta";

  /**
   * This node reads the compare-and-set precondition section of the three node-scoped security entries - the user
   * list, the group document and the API-token document (issue #7509).
   * <p>
   * Advertising it says this node REFUSES a security document whose precondition no longer matches what is in
   * force. That is what makes the section unsafe to write to a peer without it: such a peer skips the section and
   * installs the document unconditionally, so the losing entry would be refused on the upgraded nodes and applied
   * on the older one - a divergence of the security state, where an ungated pre-#7509 cluster at least lost the
   * same change everywhere. A leader that cannot see this token on every peer therefore writes no precondition and
   * keeps the pre-#7509 behaviour uniformly.
   */
  public static final String SECURITY_PRECONDITION = "security-precondition";

  /**
   * This node decodes {@code SECURITY_GROUPS_ENTRY}, the Raft log entry type (id 7) that replicates the whole
   * {@code server-groups.json} document (issue #7373).
   * <p>
   * Unlike {@link #SCHEMA_DELTA} this token does not guard an OPTIONAL section of an entry a peer can otherwise
   * read - it guards the entry's type byte itself. A peer that lacks it does not misread the entry, it cannot read
   * it at all, and {@code ArcadeStateMachine} halts rather than skip a committed entry (issue #4798). So the
   * consumer of this token refuses the operation instead of degrading it: there is no whole-document fallback to
   * degrade to. See {@link SecurityEntryCapabilityGate} and issue #7511.
   */
  public static final String SECURITY_GROUPS_ENTRY = "security-groups-entry";

  /**
   * This node decodes {@code SECURITY_API_TOKENS_ENTRY}, the Raft log entry type (id 8) that replicates the whole
   * {@code server-api-tokens.json} document (issue #7373). Same contract as {@link #SECURITY_GROUPS_ENTRY}.
   */
  public static final String SECURITY_API_TOKENS_ENTRY = "security-api-tokens-entry";

  /**
   * Everything this build can decode. Immutable, and deliberately a whitelist written out by hand rather than
   * derived from anything: a capability is a promise about the wire format, and the only thing that can make it
   * true is a human having checked that the decoder is present.
   */
  public static final Set<String> LOCAL = Set.of(SCHEMA_DELTA, SECURITY_PRECONDITION, SECURITY_GROUPS_ENTRY,
      SECURITY_API_TOKENS_ENTRY);

  private PeerCapabilities() {
    // utility class
  }
}
