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
package com.arcadedb.bench.server;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterUnderTestTest {
  /** A member without a container: records its close, answers what it is told. */
  private static final class FakeNode implements ClusterUnderTest.Node {
    final List<String> events;
    final String       name;
    final String       reason;

    FakeNode(final List<String> events, final String name, final String reason) {
      this.events = events;
      this.name = name;
      this.reason = reason;
    }

    @Override
    public ServerUnderTest server() {
      return null;
    }

    @Override
    public String deathReason() {
      return reason;
    }

    @Override
    public String clusterAnswer() {
      return null;
    }

    @Override
    public void close() {
      events.add("closed " + name);
    }
  }

  @Test
  void leaderOfFindsTheLeader() {
    final List<String> answers = Arrays.asList("{\"isLeader\":false,\"leaderReady\":false}", null,
        "{\"isLeader\":true,\"leaderReady\":true,\"x\":1}", "{\"isLeader\":true,\"leaderReady\":true}");
    assertThat(ClusterUnderTest.leaderOf(answers)).isEqualTo(2);
    // the parsed field, not a substring of the text: spacing as a pretty printer writes it still counts
    assertThat(ClusterUnderTest.leaderOf(List.of("{ \"isLeader\" : true, \"leaderReady\" : true }"))).isZero();
    // an image older than the leaderReady field: the raw role is all it says
    assertThat(ClusterUnderTest.leaderOf(List.of("{\"isLeader\":true}"))).isZero();
  }

  @Test
  void leaderOfNoneIsMinusOne() {
    assertThat(ClusterUnderTest.leaderOf(Arrays.asList("{\"isLeader\":false}", null))).isEqualTo(-1);
    assertThat(ClusterUnderTest.leaderOf(List.of())).isEqualTo(-1);
    // a text that is no JSON object is a member that said nothing useful
    assertThat(ClusterUnderTest.leaderOf(List.of("<html>\"isLeader\":true</html>"))).isEqualTo(-1);
  }

  @Test
  void aLeaderNotReadyYetIsNoLeader() {
    // just elected, its current-term entry not applied yet: DDL sent to it now fails with LeaderNotReadyException
    assertThat(ClusterUnderTest.leaderOf(List.of("{\"isLeader\":true,\"leaderReady\":false}", "{\"isLeader\":false}")))
        .isEqualTo(-1);
  }

  @Test
  void deathReasonNamesTheNode() {
    assertThat(ClusterUnderTest.deathReason(Arrays.asList(null, "OOMKilled", "exit code 1"))).isEqualTo("ha-1: OOMKilled");
    assertThat(ClusterUnderTest.deathReason(Arrays.asList(null, null))).isNull();
  }

  @Test
  void serverListNamesEveryNodeWithItsPorts() {
    assertThat(ClusterUnderTest.serverList(3)).isEqualTo("ha-0:2434:2480,ha-1:2434:2480,ha-2:2434:2480");
  }

  @Test
  void memberSettingsCarryTheRaftSettingsAndTheName() {
    final Map<String, String> settings = ClusterUnderTest.memberSettings(Map.of("arcadedb.foo", "bar"), 1, 3);
    assertThat(settings).containsEntry("arcadedb.ha.enabled", "true")
        .containsEntry("arcadedb.ha.quorum", "majority")
        .containsEntry("arcadedb.server.readinessRequiresHA", "true")
        .containsEntry("arcadedb.server.name", "ha-1")
        .containsEntry("arcadedb.ha.serverList", "ha-0:2434:2480,ha-1:2434:2480,ha-2:2434:2480")
        .containsEntry("arcadedb.foo", "bar");
  }

  @Test
  void aClusterThatNeverElectsIsClosed() {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    assertThatThrownBy(() -> ClusterUnderTest.start("img", "0-5", 6, 3, Map.of(), (i, cpuset, memGb, heap, settings) -> {
      events.add("started " + i);
      return new FakeNode(events, "ha-" + i, null);
    }, nodes -> -1, () -> events.add("network closed"))).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("leader");
    assertThat(events).contains("closed ha-0", "closed ha-1", "closed ha-2");
    assertThat(events.getLast()).isEqualTo("network closed");
  }

  @Test
  void membersThatAllTimeOutAreClosedWithTheNetwork() {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    assertThatThrownBy(() -> ClusterUnderTest.start("img", "0-5", 6, 3, Map.of(), (i, cpuset, memGb, heap, settings) -> {
      throw new IllegalStateException("Timed out waiting for container port to open");
    }, nodes -> 0, () -> events.add("network closed"))).hasMessageContaining("Timed out");
    assertThat(events).containsExactly("network closed");
  }

  @Test
  void aMemberThatFailsToStartClosesTheOthers() {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    assertThatThrownBy(() -> ClusterUnderTest.start("img", "0-5", 6, 3, Map.of(), (i, cpuset, memGb, heap, settings) -> {
      if (i == 1)
        throw new IllegalStateException("boom");
      return new FakeNode(events, "ha-" + i, null);
    }, nodes -> 0, () -> events.add("network closed"))).hasMessageContaining("boom");
    assertThat(events).contains("closed ha-0", "closed ha-2");
    assertThat(events.getLast()).isEqualTo("network closed");
  }

  @Test
  void startedClusterReportsLeaderAndConfig() throws Exception {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final List<String> seen = Collections.synchronizedList(new ArrayList<>());
    try (final ClusterUnderTest cluster = ClusterUnderTest.start("img", "0-5", 6, 3, Map.of(),
        (i, cpuset, memGb, heap, settings) -> {
          seen.add(i + ":" + cpuset + ":" + memGb + ":" + heap);
          return new FakeNode(events, "ha-" + i, i == 2 ? "exit code 137" : null);
        }, nodes -> 1, () -> events.add("network closed"))) {
      assertThat(cluster.leaderIndex()).isEqualTo(1);
      assertThat(cluster.deathReason()).isEqualTo("ha-2: exit code 137");
      assertThat(seen).containsExactlyInAnyOrder("0:0-1:2:1g", "1:2-3:2:1g", "2:4-5:2:1g");
      final JSONObject config = cluster.config();
      assertThat(config.getString("image")).isEqualTo("img");
      assertThat(config.getString("cpusets")).isEqualTo("split");
      assertThat(config.getInt("nodeMemGb")).isEqualTo(2);
      assertThat(config.getString("heap")).isEqualTo("1g");
      assertThat(config.getJSONArray("members").length()).isEqualTo(3);
    }
    assertThat(events).contains("closed ha-0", "closed ha-1", "closed ha-2");
  }
}
