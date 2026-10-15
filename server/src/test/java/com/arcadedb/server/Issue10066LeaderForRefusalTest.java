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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10066: a refusal that names the leader must not name the node that is refusing. On a cluster whose peers share a host
 * and declare no HTTP port (issue #6191) a follower resolves every peer's HTTP address, the leader's included, to its OWN, so the
 * address {@link HAServerPlugin#getLeaderAddress()} answers sends the client back to the node that just refused it.
 * {@link HAServerPlugin#getLeaderForRefusal()} is the one place that decides what a refusal names instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10066LeaderForRefusalTest {

  @Test
  void aLeaderAddressThatIsNotThisNodesOwnIsNamedAsItIs() {
    assertThat(new Cluster("db0:2480", false, "ArcadeDB_0").getLeaderForRefusal()).isEqualTo("db0:2480");
  }

  @Test
  void aLeaderAddressThatIsThisNodesOwnIsReplacedByTheLeadersServerName() {
    assertThat(new Cluster("db1:2480", true, "ArcadeDB_0").getLeaderForRefusal()).isEqualTo("ArcadeDB_0");
  }

  @Test
  void noLeaderNamesNobody() {
    assertThat(new Cluster(null, false, null).getLeaderForRefusal()).isNull();
    assertThat(new Cluster("", false, "ArcadeDB_0").getLeaderForRefusal()).isNull();
  }

  @Test
  void anOwnAddressWithNoKnownLeaderNameNamesNobodyRatherThanTheRefusingNode() {
    assertThat(new Cluster("db1:2480", true, null).getLeaderForRefusal()).isNull();
  }

  /** A cluster view reduced to the three answers {@link HAServerPlugin#getLeaderForRefusal()} is made of. */
  private record Cluster(String leaderAddress, boolean addressIsOwn, String leaderName) implements HAServerPlugin {
    @Override
    public String getLeaderAddress() {
      return leaderAddress;
    }

    @Override
    public boolean isOwnHttpAddress(final String address) {
      return addressIsOwn;
    }

    @Override
    public String getLeaderName() {
      return leaderName;
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public void startService() {
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
      return 3;
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
