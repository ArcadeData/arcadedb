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

import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7820: the embedded {@code HAServerPlugin.addPeer} API admitted a peer and told its
 * caller nothing about the cluster security seed.
 * <p>
 * {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json} live under
 * {@code <server-root>/config/}, outside the database directory, so no Raft snapshot install carries them. Since
 * issue #7531 the leader seeds them whenever a peer enters the committed configuration, which covers the
 * embedded API's <i>delivery</i> - but the two operator-facing admission paths also carry a <b>report</b> of
 * what the seed could not commit ({@code POST /api/v1/cluster/peer} answers 503 with {@code failedSeeds},
 * issue #7521; {@code connect cluster} returns {@code ConnectClusterResult.failedSeeds}, issue #7532), and the
 * embedded one returned {@code void}. An embedding application therefore had no signal at all when the seed
 * left a document uncommitted - and an embedding application is precisely where nobody is reading a SEVERE
 * line, which is what the issue reported.
 * <p>
 * This pins the report reaching the embedded caller, and the two rules that make it safe to add: the
 * membership change runs first and is never undone by a seed failure, and a failure to admit is never followed
 * by a seed.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7820EmbeddedAddPeerSeedReportTest {

  /**
   * A plugin whose membership change and leader-seed request are both stand-ins, so the assertions are about
   * what {@link RaftHAPlugin#addPeerAndReportSeed} does with them rather than about a live Ratis cluster.
   * <p>
   * Both seams are the real methods the production code calls: {@code admitPeer} is the whole of the membership
   * change, and {@code seedSecurityStateForAdmission} is the interface method that reaches the leader's single
   * seeder (issue #7834). Nothing between them is replaced.
   */
  private static class RecordingPlugin extends RaftHAPlugin {
    /** What happened, in the order it happened: the ordering assertion is the point of recording it. */
    final List<String>  steps      = new ArrayList<>();
    /** What the leader reports back, when it reports at all. */
    List<String>        failedSeeds = List.of();
    /** Raised by the leader-seed request instead of answering. */
    Exception           seedFailure;
    /** Raised by the membership change instead of admitting. */
    RuntimeException    admitFailure;

    @Override
    void admitPeer(final String peerId, final String address, final String name) {
      steps.add("admit " + peerId + " at " + address + " as " + name);
      if (admitFailure != null)
        throw admitFailure;
    }

    @Override
    public Optional<List<String>> seedSecurityStateForAdmission(final String admittedPeer) throws IOException {
      steps.add("seed " + admittedPeer);
      if (seedFailure instanceof IOException io)
        throw io;
      if (seedFailure instanceof RuntimeException runtime)
        throw runtime;
      return Optional.of(failedSeeds);
    }
  }

  // -------------------------------------------------------------------------------------------
  // The defect: the embedded caller learns what the seed could not commit
  // -------------------------------------------------------------------------------------------

  /**
   * The reported case. An embedding application admits a peer, the leader's seed leaves two of the three
   * documents uncommitted, and the application has to be able to branch on that - the peer is already a
   * cluster member enforcing its own copy of them.
   */
  @Test
  void theEmbeddedAdmissionReportsTheDocumentsThatDidNotCommit() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.failedSeeds = List.of("groups", "API tokens");

    assertThat(plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", null))
        .as("an embedding application cannot act on a log line")
        .containsExactly("groups", "API tokens");
  }

  /** And the success answer, which is what an embedder reads as "this peer is consistent with the cluster". */
  @Test
  void anAdmissionWhoseSeedCommittedEverythingReportsNoFailures() {
    assertThat(new RecordingPlugin().addPeerAndReportSeed("arcadedb-3", "localhost:2447", "node3")).isEmpty();
  }

  /** The name overload carries the name down to the membership change, as the void one always has. */
  @Test
  void theHumanReadableNameStillReachesTheMembershipChange() {
    final RecordingPlugin plugin = new RecordingPlugin();

    plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", "node3");

    assertThat(plugin.steps.get(0)).contains("arcadedb-3").contains("localhost:2447").contains("node3");
  }

  // -------------------------------------------------------------------------------------------
  // The two rules that make adding the seed to this path safe
  // -------------------------------------------------------------------------------------------

  /**
   * The membership change first. A seed that ran before the peer was a committed member would submit the
   * documents to a configuration that does not contain it, which is the one ordering that delivers nothing.
   */
  @Test
  void thePeerIsAdmittedBeforeTheSeedIsAskedFor() {
    final RecordingPlugin plugin = new RecordingPlugin();

    plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", null);

    assertThat(plugin.steps).hasSize(2);
    assertThat(plugin.steps.get(0)).startsWith("admit ");
    assertThat(plugin.steps.get(1)).isEqualTo("seed arcadedb-3");
  }

  /**
   * A membership change that failed is not a peer, so there is nothing to seed and the caller is told about the
   * failure it can act on. This is what keeps issue #7514's fail-fast on an unreachable peer fail-fast: the
   * exception leaves through {@code addPeer} exactly as it did before.
   */
  @Test
  void anAdmissionThatFailedIsNotFollowedByASeed() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.admitFailure = new RuntimeException("peer 'arcadedb-3' is not reachable");

    assertThatThrownBy(() -> plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", null))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("not reachable");

    assertThat(plugin.steps).as("nothing joined, so nothing is seeded").containsExactly(
        "admit arcadedb-3 at localhost:2447 as null");
  }

  /**
   * The converse, and the harder half: once the membership change has committed, NOTHING about the seed may be
   * reported as a failed admission. A caller that read an exception here would retry a join that already
   * happened.
   */
  @Test
  void aSeedThatCouldNotBeRunNeverFailsTheAdmission() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.seedFailure = new IOException("no route to the leader");

    assertThat(plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", null))
        .as("unknown is reported as all three failing, not as none: it tells the embedder to reissue")
        .containsExactlyElementsOf(RaftHAPlugin.ALL_SEEDED_SECURITY_DOCUMENTS);
  }

  /**
   * {@code IllegalStateException} as well as {@code IOException}: it is what the seed request raises when it
   * ran on this node - the leader path is a direct call rather than an HTTP one - and could not read the
   * outcome. Same case, same answer.
   */
  @Test
  void aSeedWhoseOutcomeCouldNotBeReadIsReportedAsAllThreeFailing() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.seedFailure = new IllegalStateException("the seed did not report within the timeout");

    assertThat(plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", null))
        .containsExactlyElementsOf(RaftHAPlugin.ALL_SEEDED_SECURITY_DOCUMENTS);
  }

  /**
   * And anything else the seed request raises, which is the width {@code ServerControlPlane.connectCluster}
   * already catches for the same reason: the two named exception types are what the request is KNOWN to raise,
   * while the rule is that nothing raised while seeding may be read as a join that did not happen.
   */
  @Test
  void anUnexpectedFailureWhileSeedingStillDoesNotFailTheAdmission() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.seedFailure = new IllegalArgumentException("something nobody predicted");

    assertThat(plugin.addPeerAndReportSeed("arcadedb-3", "localhost:2447", null))
        .containsExactlyElementsOf(RaftHAPlugin.ALL_SEEDED_SECURITY_DOCUMENTS);
  }

  /** The void overload keeps its signature and its contract: a residual seed failure is logged, not thrown. */
  @Test
  void theVoidOverloadStillDoesNotThrowOnAResidualSeedFailure() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.failedSeeds = List.of("users");

    plugin.addPeer("arcadedb-3", "localhost:2447");

    assertThat(plugin.steps).as("the admission and the seed both ran").hasSize(2);
  }

  /** And it seeds too, which is the half that used to be missing entirely from every embedded call. */
  @Test
  void theVoidOverloadAsksForTheSeedAsWell() {
    final RecordingPlugin plugin = new RecordingPlugin();

    plugin.addPeer("arcadedb-3", "localhost:2447", "node3");

    assertThat(plugin.steps).containsExactly("admit arcadedb-3 at localhost:2447 as node3", "seed arcadedb-3");
  }

  // -------------------------------------------------------------------------------------------
  // The interface default, for an HA implementation that is not Raft
  // -------------------------------------------------------------------------------------------

  /**
   * An implementation predating this method keeps the behaviour it was written against: the peer is admitted
   * through the overload it does implement, and the empty report is honest because such an implementation has
   * no cluster security documents that could have failed to seed.
   */
  @Test
  void anImplementationWithoutAClusterSecuritySeedStillAdmitsThePeer() {
    final LegacyHAPlugin legacy = new LegacyHAPlugin();

    assertThat(legacy.addPeerAndReportSeed("arcadedb-3", "localhost:2447", "node3"))
        .as("an implementation with no cluster security documents has none that could fail to seed")
        .isEmpty();
    assertThat(legacy.admitted).as("the membership change is not skipped by the default").containsExactly("arcadedb-3");
  }

  /**
   * An HA implementation written before issue #7820, overriding only the two-argument {@code addPeer} the
   * interface has always had. Implemented rather than mocked so the default method under test is the real one.
   */
  private static class LegacyHAPlugin implements HAServerPlugin {
    final List<String> admitted = new ArrayList<>();

    @Override
    public void addPeer(final String peerId, final String address) {
      admitted.add(peerId);
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public String getLeaderName() {
      return null;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 1;
    }

    @Override
    public String getLeaderAddress() {
      return null;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }
}
