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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.runner.BenchRunner;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.serializer.json.JSONObject;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Network;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code ha} workload against a real 3-node Raft cluster of the image under test, 3 s repetitions at XS: every case passes
 * its checks after each repetition (every acknowledged id on every node, the digest of every type equal on all three), and
 * nothing the cases started is left behind.
 */
class HaWorkloadIT {
  private static final Map<String, String> SESSION = Map.of(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL,
      DockerClientFactory.SESSION_ID);

  @Test
  void threeCasesOnARealClusterPassTheirChecksAndLeaveNothingBehind() {
    final DockerClient docker = DockerClientFactory.instance().client();
    final Set<String> containersBefore = containers(docker);
    final Set<String> networksBefore = networks(docker);

    final HaWorkload workload = new HaWorkload(null, Duration.ofSeconds(3));
    final WorkloadContext ctx = new WorkloadContext(null, Scale.XS, 42L, new JSONObject(),
        new WorkloadContext.ServerSpec(ITSupport.IMAGE, "", 6, "2g"));
    final List<CaseResult> results = new BenchRunner(2, 1, Duration.ofMinutes(5)).run(workload, ctx, n -> true);

    assertThat(results).extracting(CaseResult::caseName)
        .containsExactly(HaWorkload.THROUGHPUT_1W, HaWorkload.THROUGHPUT_8W, HaWorkload.LAG);
    for (final CaseResult r : results) {
      // OK means the checks after each repetition passed: no acknowledged id missing on any node, and every type's digest the
      // same on all three, so the digest carries nothing node-specific
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      assertThat(ITSupport.metric(r, "unknown_writes").values()).as(r.caseName()).hasSize(2).containsOnly(0.0);
      // no write failed, so no writer asked for the leader again: none can have found a new one
      assertThat(ITSupport.metric(r, "leader_changes").values()).as(r.caseName()).hasSize(2).containsOnly(0.0);
    }
    for (final CaseResult r : results.subList(0, 2))
      assertThat(ITSupport.metric(r, "records_per_s").median()).as(r.caseName()).isGreaterThan(0);
    final CaseResult lag = results.get(2);
    // a lag is clamped at 0 (a follower may show an id before the leader's answer reaches the client): no lower bound here
    assertThat(ITSupport.metric(lag, "lag_p50_ms").values()).hasSize(2);
    assertThat(ITSupport.metric(lag, "lag_max_ms").median()).isGreaterThanOrEqualTo(ITSupport.metric(lag, "lag_p50_ms").median());

    final JSONObject config = workload.serverConfig();
    assertThat(config.getInt("nodes")).isEqualTo(3);
    assertThat(config.getJSONArray("members").length()).isEqualTo(3);
    assertThat(workload.imageInfo()).isNotNull();

    // the runner tore every case down: no member and no network of the run is left
    final Set<String> containersLeft = containers(docker);
    containersLeft.removeAll(containersBefore);
    assertThat(containersLeft).as("containers of the run left after teardown").isEmpty();
    final Set<String> networksLeft = networks(docker);
    networksLeft.removeAll(networksBefore);
    assertThat(networksLeft).as("networks of the run left after teardown").isEmpty();
  }

  /**
   * The statements and reads of the real cluster, below what a passing case shows: the members share one network, the leader is
   * named, the records written through the leader carry the positional parameters in place, the schema lists the type, and the
   * digest is the same on all three nodes whether it reads the type in one page or in pages of 7, which pages through the
   * {@code @rid} keyset query (a case at XS writes fewer records than one page of 20,000).
   */
  @Test
  void theRealClusterAnswersTheStatementsTheWorkloadUses() throws Exception {
    final DockerClient docker = DockerClientFactory.instance().client();
    final Set<String> containersBefore = containers(docker);
    final Set<String> networksBefore = networks(docker);
    final int records = 50;
    try (final HaWorkload.RaftCluster cluster = (HaWorkload.RaftCluster) HaWorkload.RaftCluster.start(
        new WorkloadContext.ServerSpec(ITSupport.IMAGE, "", 6, "2g"), Map.of())) {
      assertThat(cluster.nodes()).isEqualTo(3);
      final int leader = cluster.leader();
      assertThat(leader).isBetween(0, 2);
      assertThat(cluster.refreshLeader()).isEqualTo(leader);
      assertThat(cluster.deathReason()).isNull();

      // the three members, on one network of their own besides nothing else
      final Set<String> started = containers(docker);
      started.removeAll(containersBefore);
      assertThat(started).hasSize(3);
      final Set<String> memberNetworks = new HashSet<>();
      for (final Container c : docker.listContainersCmd().withLabelFilter(SESSION).exec())
        if (started.contains(c.getId() + " " + c.getImage())) {
          assertThat(c.getNetworkSettings().getNetworks()).hasSize(1);
          memberNetworks.addAll(c.getNetworkSettings().getNetworks().keySet());
        }
      assertThat(memberNetworks).hasSize(1);
      // the scan of the session's networks reaches the cluster's: without this, "no network left" below and in the other test
      // would hold for a network the scan never sees
      final Set<String> runNetworks = networks(docker);
      runNetworks.removeAll(networksBefore);
      assertThat(runNetworks).as("the session's networks that the cluster added").hasSize(1);
      assertThat(runNetworks.iterator().next()).endsWith(" " + memberNetworks.iterator().next());

      try (final HaWorkload.Writer writer = cluster.writer()) {
        for (long id = 0; id < records; id++)
          writer.insert(id, 5);
      }
      for (int n = 0; n < 3; n++)
        awaitCount(cluster, n, records);

      assertThat(cluster.types()).contains(HaWorkload.TYPE);
      final List<JSONObject> first = cluster.page(leader, HaWorkload.TYPE, null, 7);
      assertThat(first).hasSize(7);
      final JSONObject record = first.getFirst();
      assertThat(record.getInt("writer")).isEqualTo(5);
      assertThat(record.getString("payload")).hasSize(HaWorkload.PAYLOAD_CHARS);
      assertThat(record.getString("@rid")).matches("#\\d+:\\d+");
      final List<JSONObject> second = cluster.page(leader, HaWorkload.TYPE, first.getLast().getString("@rid"), 7);
      assertThat(second).hasSize(7);
      assertThat(second.getFirst().getString("@rid")).isNotEqualTo(first.getLast().getString("@rid"));

      for (int n = 0; n < 3; n++) {
        assertThat(cluster.count(n)).as("ha-" + n).isEqualTo(records);
        assertThat(cluster.ids(n, 0, records)).as("ha-" + n).hasSize(records).startsWith(0L, 1L).endsWith(records - 1L);
      }
      final String digest = cluster.digest(leader, HaWorkload.TYPE);
      for (int n = 0; n < 3; n++) {
        assertThat(cluster.digest(n, HaWorkload.TYPE)).as("ha-" + n + " in one page").isEqualTo(digest);
        assertThat(cluster.digest(n, HaWorkload.TYPE, 7)).as("ha-" + n + " in pages of 7").isEqualTo(digest);
      }
      // the digest covers the records: one more changes it
      try (final HaWorkload.Writer writer = cluster.writer()) {
        writer.insert(records, 5);
      }
      assertThat(cluster.digest(leader, HaWorkload.TYPE)).isNotEqualTo(digest);
    }
    final Set<String> left = containers(docker);
    left.removeAll(containersBefore);
    assertThat(left).as("containers left after close").isEmpty();
    final Set<String> networksLeft = networks(docker);
    networksLeft.removeAll(networksBefore);
    assertThat(networksLeft).as("networks left after close").isEmpty();
  }

  private static void awaitCount(final HaWorkload.Cluster cluster, final int node, final long target) throws InterruptedException {
    for (int i = 0; i < 240 && cluster.count(node) < target; i++)
      Thread.sleep(250);
  }

  /** The containers of this session, running or not, but the resource reaper's, which lives as long as the JVM. */
  private static Set<String> containers(final DockerClient docker) {
    final Set<String> ids = new HashSet<>();
    for (final Container c : docker.listContainersCmd().withShowAll(true).withLabelFilter(SESSION).exec())
      if (c.getImage() == null || !c.getImage().contains("ryuk"))
        ids.add(c.getId() + " " + c.getImage());
    return ids;
  }

  private static Set<String> networks(final DockerClient docker) {
    final Set<String> ids = new HashSet<>();
    for (final Network n : docker.listNetworksCmd().withFilter("label",
        List.of(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL + "=" + DockerClientFactory.SESSION_ID)).exec())
      ids.add(n.getId() + " " + n.getName());
    return ids;
  }
}
