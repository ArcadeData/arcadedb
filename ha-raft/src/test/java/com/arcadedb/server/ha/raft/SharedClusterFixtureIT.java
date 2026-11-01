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

import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of {@link SharedClusterRaftHATest}: one cluster boot serves every test of the class, a test that
 * leaves the cluster degraded is followed by a reboot rather than by a broken test, and {@link #uniqueName(String)}
 * hands every test its own names. The methods run in a fixed order because the later ones assert on what the earlier
 * ones left behind.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SharedClusterFixtureIT extends SharedClusterRaftHATest {
  private final Set<ArcadeDBServer> seenServer0 = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<String>         seenNames   = new HashSet<>();
  private       ArcadeDBServer[]    serversBeforeTheStop;
  private       int                 stoppedFollower = -1;

  private static final String MARKER_TYPE = "SharedClusterMarker";
  private              int    hookRuns    = 0;

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Per-class setup as a subclass is told to write it: a type every test relies on, created through the leader.
   */
  @Override
  protected void onSharedClusterStarted() {
    hookRuns++;
    final int leader = findLeaderIndex();
    getServerDatabase(leader, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + MARKER_TYPE + " IF NOT EXISTS");
    waitForAllServers();
  }

  @Test
  @Order(1)
  void firstTestSeesTheSharedCluster() {
    recordWhatThisTestSees();
    assertThat(hookRuns).isEqualTo(1);
    assertMarkerOnEveryServer();
  }

  @Test
  @Order(2)
  void secondTestSeesTheSameCluster() {
    recordWhatThisTestSees();
  }

  @Test
  @Order(3)
  void thirdTestSeesTheSameClusterAndNamesNeverRepeat() {
    recordWhatThisTestSees();
    // (a) one boot for the three tests: a per-test fixture would have built a new server 0 for each
    assertThat(seenServer0).hasSize(1);
    // (c) uniqueName() handed each test a name no other test got
    assertThat(seenNames).hasSize(3);
  }

  @Test
  @Order(4)
  void testThatStopsAFollowerAndLeavesItDown() {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    stoppedFollower = firstFollowerOf(leader);
    serversBeforeTheStop = getServers().clone();

    getServer(stoppedFollower).stop();
    assertThat(getServer(stoppedFollower).isStarted()).isFalse();
  }

  @Test
  @Order(5)
  void nextTestRunsOnARebootedFullCluster() {
    // (b) the follower test 4 left down forced ensureClusterIsHealthy() to reboot the whole cluster before this test
    assertThat(stoppedFollower).isGreaterThanOrEqualTo(0);
    assertThat(getServers()).hasSize(3);
    for (int i = 0; i < getServerCount(); i++) {
      assertThat(getServer(i).isStarted()).isTrue();
      assertThat(getServer(i)).isNotSameAs(serversBeforeTheStop[i]);
    }

    int leaders = 0;
    for (int i = 0; i < getServerCount(); i++)
      if (getRaftPlugin(i).isLeader())
        leaders++;
    assertThat(leaders).isEqualTo(1);

    // the reboot recreated the database, so the per-class setup ran again: initial boot + one reboot
    assertThat(hookRuns).isEqualTo(2);
    assertMarkerOnEveryServer();

    // the rebooted cluster is a working one: a write on the leader reaches every replica
    final int leader = findLeaderIndex();
    final String type = uniqueName("AfterReboot");
    getServerDatabase(leader, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + type);
    getServerDatabase(leader, getDatabaseName()).transaction(
        () -> getServerDatabase(leader, getDatabaseName()).newDocument(type).set("id", 1).save());
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(countOn(i, type)).isEqualTo(1L);
  }

  private void assertMarkerOnEveryServer() {
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(MARKER_TYPE)).as("marker type on server " + i)
          .isTrue();
  }

  private void recordWhatThisTestSees() {
    seenServer0.add(getServer(0));
    seenNames.add(uniqueName("T"));
  }
}
