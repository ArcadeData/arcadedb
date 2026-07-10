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

import com.arcadedb.server.ha.raft.BootstrapElection.PeerState;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BootstrapElection#electSourceNode(Map, RaftPeerId)} - the deterministic,
 * cluster-wide bootstrap source election introduced for issue #4807.
 * <p>
 * The previous {@code decideAndAct} loop iterated a {@code HashMap} and returned {@code TRANSFERRED}
 * on the first database whose source was a remote peer, abandoning the remaining databases. Under
 * ArcadeDB's one-leader-per-cluster model the correct behaviour is to elect a single source node
 * (the node holding the freshest copy of the most databases) and bootstrap every database from it.
 * These tests pin the election to be deterministic and order-independent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BootstrapElectionSourceSelectionTest {

  private static final RaftPeerId A = RaftPeerId.valueOf("nodeA");
  private static final RaftPeerId B = RaftPeerId.valueOf("nodeB");
  private static final RaftPeerId C = RaftPeerId.valueOf("nodeC");

  private static PeerState state(final RaftPeerId peer, final String db, final long lastTxId) {
    return new PeerState(peer, db, "fp-" + peer + "-" + db + "-" + lastTxId, lastTxId);
  }

  @Test
  void identicalCopiesPreferTheLocalPeer() {
    // The customer happy path: every node staged identical backups. Every database is a full tie,
    // so prefer-self must keep leadership on the current leader (no unnecessary transfer).
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", 10), state(B, "db1", 10), state(C, "db1", 10)),
        "db2", List.of(state(A, "db2", 10), state(B, "db2", 10), state(C, "db2", 10)),
        "db3", List.of(state(A, "db3", 10), state(B, "db3", 10), state(C, "db3", 10))));

    assertThat(BootstrapElection.electSourceNode(states, A)).isEqualTo(A);
    assertThat(BootstrapElection.electSourceNode(states, B)).isEqualTo(B);
    assertThat(BootstrapElection.electSourceNode(states, C)).isEqualTo(C);
  }

  @Test
  void freshestNodeWinsAcrossAllDatabases() {
    // Normal single-leader shutdown: the last leader (A) is fresher than the followers on every
    // database. A must be elected regardless of which node runs the election.
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", 20), state(B, "db1", 18), state(C, "db1", 15)),
        "db2", List.of(state(A, "db2", 30), state(B, "db2", 30), state(C, "db2", 25)),
        "db3", List.of(state(A, "db3", 12), state(B, "db3", 9), state(C, "db3", 12))));

    // A is freshest (or tied-freshest) on all three databases -> 3 votes; nobody else reaches that.
    assertThat(BootstrapElection.electSourceNode(states, B)).isEqualTo(A);
    assertThat(BootstrapElection.electSourceNode(states, C)).isEqualTo(A);
    assertThat(BootstrapElection.electSourceNode(states, A)).isEqualTo(A);
  }

  @Test
  void pluralityElectsASingleSourceInsteadOfAbandoningDatabases() {
    // The reporter's scenario: db1/db2 freshest on A, db3 freshest on B. The old loop could
    // transfer to B for db3 and abandon db1/db2 (or vice-versa, depending on HashMap order). The
    // election picks the plurality winner (A, 2 databases) deterministically. A single node then
    // commits ALL databases, so nothing is abandoned.
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", 50), state(B, "db1", 40)),
        "db2", List.of(state(A, "db2", 50), state(B, "db2", 40)),
        "db3", List.of(state(A, "db3", 40), state(B, "db3", 50))));

    // Plurality: A wins 2 databases, B wins 1. Result is identical no matter who runs it.
    assertThat(BootstrapElection.electSourceNode(states, A)).isEqualTo(A);
    assertThat(BootstrapElection.electSourceNode(states, B)).isEqualTo(A);
    assertThat(BootstrapElection.electSourceNode(states, C)).isEqualTo(A);
  }

  @Test
  void aggregateBeatsPreferSelfOnVoteTie() {
    // Construct an exact vote tie (A and B each freshest on the same set of databases) where A has
    // a strictly higher aggregate lastTxId. Even when the local peer is B, A must be elected.
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", 10), state(B, "db1", 10)), // tie -> A,B vote
        "db2", List.of(state(A, "db2", 10), state(B, "db2", 10)), // tie -> A,B vote
        // db3: C is freshest (both A and B only contribute to aggregate, not votes), and A > B here.
        "db3", List.of(state(A, "db3", 8), state(B, "db3", 4), state(C, "db3", 20))));

    // Votes: A=2, B=2, C=1. Aggregate: A=28, B=24 -> A wins the tie. Local=B must not flip it.
    assertThat(BootstrapElection.electSourceNode(states, B)).isEqualTo(A);
    assertThat(BootstrapElection.electSourceNode(states, A)).isEqualTo(A);
  }

  @Test
  void lexicographicIdBreaksFullTies() {
    // A perfect tie on votes and aggregate, and the local peer is not a candidate: the
    // lexicographically-lowest peer id wins for repeatability.
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", 7), state(B, "db1", 7)),
        "db2", List.of(state(A, "db2", 7), state(B, "db2", 7))));

    final RaftPeerId outsider = RaftPeerId.valueOf("nodeZ");
    assertThat(BootstrapElection.electSourceNode(states, outsider)).isEqualTo(A);
  }

  @Test
  void noDataAnywhereReturnsNull() {
    // Brand-new cluster: every database is empty (lastTxId=-1). The offline-bootstrap path has
    // nothing to do, so no source is elected.
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", -1), state(B, "db1", -1)),
        "db2", List.of(state(A, "db2", -1), state(B, "db2", -1))));

    assertThat(BootstrapElection.electSourceNode(states, A)).isNull();
  }

  @Test
  void databasesWithoutDataDoNotVote() {
    // db1 has data only on B; db2 is empty everywhere. Only db1 contributes a vote, so B wins even
    // though it is not the local peer.
    final Map<String, List<PeerState>> states = new HashMap<>(Map.of(
        "db1", List.of(state(A, "db1", -1), state(B, "db1", 5)),
        "db2", List.of(state(A, "db2", -1), state(B, "db2", -1))));

    assertThat(BootstrapElection.electSourceNode(states, A)).isEqualTo(B);
  }
}
