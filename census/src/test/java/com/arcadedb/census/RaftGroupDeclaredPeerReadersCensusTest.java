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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11203: {@code raftGroup.getPeers()} is the server list as declared at startup, never the membership, and twice a fix to
 * what "replica" means was incomplete because a reporter kept reading it (#7040). The rule of {@code ha-raft/CLAUDE.md} used to be an
 * enumeration of the legitimate direct readers in prose, which went stale: it named three and the module had a dozen, each justified
 * at its own site. The enumeration is held here instead, by method, with the reason the method wants the declared list: a new
 * direct reader fails this test until it either reads the reconciled membership ({@code configuredPeers()}, {@code knownPeers()})
 * or is added below with its reason, and a reader that goes away takes its entry with it.
 * <p>
 * The census ranges over the authority, not over a spelling (#11270): every call of Ratis {@code RaftGroup.getPeers()}, or method
 * reference to it, in the compiled classes of every module ({@link CompiledClasses}). The source-text census it replaces matched
 * two spellings on one line in one module, so a read through a local, a call split across two lines or a read inside a nested
 * class passed it. A call on a group that is not this node's own, such as the group of a peer's reply, is a call site too, and is
 * listed with the reason it is not a read of the declared list: the bytecode cannot tell one group from another, and a reviewer
 * can.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftGroupDeclaredPeerReadersCensusTest {
  /** The authority: every read of a group's declared server list is a call of this method. */
  private static final String AUTHORITY = "org/apache/ratis/protocol/RaftGroup";
  private static final String READ      = "getPeers";

  private static final String RECONCILIATION_INPUT = "an input of the reconciliation with the live configuration";
  private static final String MISDIRECTED_PATH     = "the misdirected-request path must not read the division (#10088)";

  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry("server/ha/raft/RaftHAServer#getConfiguredServers", "the declared list, by name: how many servers the configuration declares"),
      Map.entry("server/ha/raft/RaftHAServer#getDeclaredPeers", "the declared list, by name: RaftClusterStatusExporter reconciles it itself"),
      Map.entry("server/ha/raft/RaftHAServer#configuredPeers", RECONCILIATION_INPUT),
      Map.entry("server/ha/raft/RaftHAServer#knownPeers", RECONCILIATION_INPUT),
      Map.entry("server/ha/raft/GetClusterHandler#execute", RECONCILIATION_INPUT),
      Map.entry("server/ha/raft/RaftHAServer#getLivePeers", "the fallback when the live configuration cannot be read"),
      Map.entry("server/ha/raft/RaftHAServer#peerRaftAddress", "the fallback after the live configuration: a declared peer it dropped still resolves"),
      Map.entry("server/ha/raft/RaftHAServer#isMemberWithoutReadingTheDivision", MISDIRECTED_PATH),
      Map.entry("server/ha/raft/RaftHAServer#isMisdirectedPeerMember", "the misdirected-request path must not read the division first (#10088)"),
      Map.entry("server/ha/raft/RaftHAServer#resolvePeerAddresses", MISDIRECTED_PATH),
      Map.entry("server/ha/raft/KubernetesAutoJoin#findLeader", "before joining, the declared peers are the only ones to probe for a leader"),
      Map.entry("server/ha/raft/KubernetesAutoJoin#joinFrom", "before joining, this node's own declared definition is what it asks to be added as"),
      Map.entry("server/ha/raft/KubernetesAutoJoin$RaftClusterProbe#ask",
          "not this node's group: the group of a peer's GroupInfoReply, the membership as that peer configures it (#5275, #10760)"),
      Map.entry("server/ha/raft/RaftHAPlugin#resolveShutdownTarget", "an operator named a server: the answer is the peer the name declares")));

  @Test
  void everyDirectReaderOfTheDeclaredGroupIsReviewed() throws Exception {
    final Map<String, Set<String>> callSites = CompiledClasses.callSitesOf(CompiledClasses.ofEveryModule(),
        (owner, name, desc) -> AUTHORITY.equals(owner) && READ.equals(name));
    assertThat(callSites).as("the census must reach the readers it is known to cover, in ha-raft")
        .containsKeys("server/ha/raft/RaftHAServer#configuredPeers");

    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as(
        "direct readers of RaftGroup.getPeers(), the DECLARED server list: read configuredPeers() or knownPeers() instead, or add the "
            + "method to REVIEWED with the reason it wants the declared list (ha-raft/CLAUDE.md)").isEmpty();
    assertThat(CompiledClasses.stale(callSites, REVIEWED)).as("REVIEWED entries that no longer read the declared list: remove them")
        .isEmpty();
  }
}
