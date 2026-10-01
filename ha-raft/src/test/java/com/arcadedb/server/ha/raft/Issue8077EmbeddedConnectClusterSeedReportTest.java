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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8077: the embedded {@code HAServerPlugin.connectCluster} API joined a server and told
 * its caller nothing about the cluster security seed, while {@code ServerControlPlane.connectCluster} (issue #7532)
 * and the embedded {@code addPeer} API (issue #7820) both report it.
 * <p>
 * {@code RaftHAPlugin.connectClusterAndReportSeed} is the reporting form. This pins that it reports, that the join
 * runs first and is never undone or failed by the seed, and that a join that failed is never followed by a seed.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8077EmbeddedConnectClusterSeedReportTest {

  private static final String ADDRESS = "node3@localhost:2447";

  /**
   * A plugin whose join and leader-seed request are both stand-ins. Both are the real methods the production code
   * calls: {@code connectCluster} is the whole of the membership change, and {@code seedSecurityStateForAdmission}
   * reaches the leader's single seeder (issue #7834). Nothing between them is replaced.
   */
  private static class RecordingPlugin extends RaftHAPlugin {
    final List<String> steps       = new ArrayList<>();
    List<String>       failedSeeds = List.of();
    Exception          seedFailure;
    RuntimeException   joinFailure;

    @Override
    public void connectCluster(final String serverAddress) {
      steps.add("join " + serverAddress);
      if (joinFailure != null)
        throw joinFailure;
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

  /** The reported case: the embedder learns which documents the leader's seed could not commit. */
  @Test
  void theEmbeddedJoinReportsTheDocumentsThatDidNotCommit() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.failedSeeds = List.of("groups", "API tokens");

    assertThat(plugin.connectClusterAndReportSeed(ADDRESS)).as("an embedding application cannot act on a log line")
        .contains(List.of("groups", "API tokens"));
  }

  /** A clean join reports an empty list - never an empty Optional, which would make the control plane seed again. */
  @Test
  void aCleanJoinReportsAPresentEmptyList() {
    final Optional<List<String>> report = new RecordingPlugin().connectClusterAndReportSeed(ADDRESS);

    assertThat(report).isPresent();
    assertThat(report.get()).isEmpty();
  }

  /** The join first, then exactly one seed request, for the address that was joined. */
  @Test
  void theServerIsJoinedBeforeTheSeedIsAskedForAndTheSeedIsAskedOnce() {
    final RecordingPlugin plugin = new RecordingPlugin();

    plugin.connectClusterAndReportSeed(ADDRESS);

    assertThat(plugin.steps).containsExactly("join " + ADDRESS, "seed " + ADDRESS);
  }

  /** A join that failed is not a member, so nothing is seeded and the failure reaches the caller. */
  @Test
  void aJoinThatFailedIsNotFollowedByASeed() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.joinFailure = new IllegalArgumentException("cannot join this node to itself");

    assertThatThrownBy(() -> plugin.connectClusterAndReportSeed(ADDRESS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("itself");

    assertThat(plugin.steps).containsExactly("join " + ADDRESS);
  }

  /** The leader cannot be reached: the join stands and the outcome is reported as unknown, i.e. all three. */
  @Test
  void aSeedThatCouldNotBeRunNeverFailsTheJoin() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.seedFailure = new IOException("no route to the leader");

    assertThat(plugin.connectClusterAndReportSeed(ADDRESS)).contains(RaftHAPlugin.ALL_SEEDED_SECURITY_DOCUMENTS);
  }

  /** Any unchecked failure while seeding is the same case, including an UnsupportedOperationException. */
  @Test
  void anUncheckedFailureWhileSeedingStillDoesNotFailTheJoin() {
    final RecordingPlugin plugin = new RecordingPlugin();
    plugin.seedFailure = new UnsupportedOperationException("something nobody predicted");

    assertThat(plugin.connectClusterAndReportSeed(ADDRESS)).contains(RaftHAPlugin.ALL_SEEDED_SECURITY_DOCUMENTS);
  }
}
