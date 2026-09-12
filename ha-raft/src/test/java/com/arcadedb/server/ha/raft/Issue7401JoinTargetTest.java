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

import com.arcadedb.server.ha.raft.RaftPeerAddressResolver.JoinTarget;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7401: {@code connect cluster <address>} takes one entry of {@code arcadedb.ha.serverList} and
 * turns it into the peer to add.
 * <p>
 * That is the whole of the argument mapping the verb was missing, and the property worth pinning is not
 * that the mapping produces <em>a</em> peer id but that it produces <b>the same</b> id the joining server
 * derives for itself. A command that invents an id the peer does not answer to leaves the cluster with a
 * configuration entry for a process that will never appear, which no amount of Raft retrying can fix -
 * so {@link #theJoinPathAndTheServerListPathAgreeOnTheSameAddress()} compares the two derivations
 * directly rather than asserting a literal on one of them.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7401JoinTargetTest {

  /** {@code GlobalConfiguration.HA_RAFT_PORT}'s default, the port an entry without one inherits. */
  private static final int DEFAULT_RAFT_PORT = 2434;

  private static JoinTarget parse(final String address) {
    return RaftPeerAddressResolver.parseJoinTarget(address, DEFAULT_RAFT_PORT, "");
  }

  @Test
  void hostAndRaftPortYieldTheUnderscoredPeerId() {
    final JoinTarget target = parse("db2:2435");

    assertThat(target.peer().getId().toString()).isEqualTo("db2_2435");
    assertThat(target.peer().getAddress()).isEqualTo("db2:2435");
    assertThat(target.httpAddress()).isNull();
    assertThat(target.name()).isNull();
  }

  /**
   * A bare host inherits {@code arcadedb.ha.raftPort}, exactly as the same entry would in the server
   * list. The derived id therefore carries the default port, which is what the joining node - reading
   * the same default - calls itself.
   */
  @Test
  void aBareHostInheritsTheDefaultRaftPort() {
    final JoinTarget target = parse("db2");

    assertThat(target.peer().getAddress()).isEqualTo("db2:" + DEFAULT_RAFT_PORT);
    assertThat(target.peer().getId().toString()).isEqualTo("db2_" + DEFAULT_RAFT_PORT);
  }

  /**
   * The positional form's HTTP port is carried through, because the address {@code RaftClusterManager}
   * would otherwise derive is the Raft port plus <em>the adding node's</em> HTTP offset - right only for
   * a homogeneous cluster.
   */
  @Test
  void anExplicitHttpPortIsCarriedThrough() {
    final JoinTarget target = parse("db2:2435:2481");

    assertThat(target.peer().getAddress()).isEqualTo("db2:2435");
    assertThat(target.httpAddress()).isEqualTo("db2:2481");
  }

  @Test
  void theNamePrefixBecomesTheDisplayName() {
    final JoinTarget target = parse("frankfurt@db2:2435");

    assertThat(target.name()).isEqualTo("frankfurt");
    assertThat(target.peer().getId().toString()).isEqualTo("db2_2435");
  }

  @Test
  void theObjectFormIsAcceptedToo() {
    final JoinTarget target = parse("db2:{raft:2435,http:2481,priority:7}");

    assertThat(target.peer().getAddress()).isEqualTo("db2:2435");
    assertThat(target.httpAddress()).isEqualTo("db2:2481");
    assertThat(target.peer().getPriority()).isEqualTo(7);
  }

  /**
   * The assertion this class exists for: join and startup must derive one identity from one address.
   * Comparing the two derivations is what a literal cannot do - a literal passes just as well against
   * two rules that happen to agree on {@code db2:2435} and part company on the object form or on a
   * bare host.
   */
  @Test
  void theJoinPathAndTheServerListPathAgreeOnTheSameAddress() {
    for (final String entry : new String[] { "db2", "db2:2435", "db2:2435:2481",
        "frankfurt@db2:2435", "db2:{raft:2435,http:2481}" }) {
      final JoinTarget joined = parse(entry);
      final RaftPeerAddressResolver.ParsedPeerList declared =
          RaftPeerAddressResolver.parsePeerList(entry, DEFAULT_RAFT_PORT, "");

      assertThat(joined.peer().getId())
          .as("connect cluster '%s' must derive the id the server list derives", entry)
          .isEqualTo(declared.peers().getFirst().getId());
      assertThat(joined.peer().getAddress()).isEqualTo(declared.peers().getFirst().getAddress());
    }
  }

  /** The Kubernetes DNS suffix is applied here as it is at startup, or the short pod name resolves nowhere. */
  @Test
  void theKubernetesDnsSuffixIsApplied() {
    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget("arcadedb-2", DEFAULT_RAFT_PORT,
        ".arcadedb.default.svc.cluster.local");

    assertThat(target.peer().getAddress())
        .isEqualTo("arcadedb-2.arcadedb.default.svc.cluster.local:" + DEFAULT_RAFT_PORT);
  }

  /**
   * A blank argument is the caller's to fix, not a server precondition: {@code IllegalArgumentException}
   * is what HTTP answers 400 and gRPC {@code INVALID_ARGUMENT} for. A bare {@code connect cluster} over
   * HTTP arrives here as {@code ""} from {@code extractTarget}, which is why the empty string is tested
   * and not only {@code null}.
   */
  @Test
  void aBlankAddressIsRejectedAsACallerError() {
    assertThatThrownBy(() -> parse(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the address");

    assertThatThrownBy(() -> parse("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the address");

    assertThatThrownBy(() -> parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the address");
  }

  /**
   * The server list is comma-separated and this argument is one entry of it, so a list has to be
   * refused rather than silently joining only its first element - the failure mode an operator would
   * not notice until the cluster was one node short.
   */
  @Test
  void aCommaSeparatedListIsRefusedRatherThanTruncated() {
    assertThatThrownBy(() -> parse("db2:2435,db3:2436"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one server at a time");
  }

  /**
   * A malformed entry reaches the caller as its own mistake. The underlying parser raises
   * {@code ServerException}, which the transports would answer 500 / {@code INTERNAL}: converting it
   * here is what keeps a typo from reading as a server fault.
   */
  @Test
  void aMalformedAddressIsReportedAsACallerErrorNotAServerFault() {
    assertThatThrownBy(() -> parse("db2:2435:2481:0:2491:extra"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid server address");

    assertThatThrownBy(() -> parse("@db2:2435"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid server address");
  }
}
