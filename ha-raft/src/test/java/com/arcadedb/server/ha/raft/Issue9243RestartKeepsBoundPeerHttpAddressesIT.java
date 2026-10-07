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
package com.arcadedb.server.ha.raft;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9243: a server of a {@link BaseRaftHATest} cluster that is stopped and started again
 * must address its peers at the HTTP ports they bound, not at the hints of {@link #getServerAddresses()}.
 * <p>
 * The fixture declares each peer's HTTP address as a hint and patches the real ports in once the cluster is up. A
 * restarted node builds a fresh {@link RaftHAServer} from the hints, so the patch is lost on it unless the restart
 * re-applies it. The default hints ({@code 2480 + i}) are right whenever nothing else holds 2480, which would let
 * this test pass without the fix; here they name a port no server binds, the same wrongness a stranger on 2480
 * produces, but on every machine.
 */
class Issue9243RestartKeepsBoundPeerHttpAddressesIT extends BaseRaftHATest {

  /** Never bound by a fixture server, which draws from the 2480-2489 range. */
  private static final int UNBOUND_HINT_PORT = 1;

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected String getServerAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:").append(raftPort(i)).append(":").append(UNBOUND_HINT_PORT);
    }
    return sb.toString();
  }

  @Test
  @Timeout(120)
  void restartedServerAddressesItsPeersAtTheirBoundPorts() {
    assertEveryNodeAddressesEveryPeerAtItsBoundPort("after the cluster started");

    final int restarted = 1;
    getServer(restarted).stop();
    startServer(restarted);

    assertThat(getRaftPlugin(restarted)).as("server %d must be running again", restarted).isNotNull();
    assertEveryNodeAddressesEveryPeerAtItsBoundPort("after server " + restarted + " restarted");
  }

  private void assertEveryNodeAddressesEveryPeerAtItsBoundPort(final String when) {
    for (int i = 0; i < getServerCount(); i++) {
      final Map<RaftPeerId, String> httpAddresses = getRaftPlugin(i).getRaftHAServer().getHttpAddresses();
      for (int j = 0; j < getServerCount(); j++)
        assertThat(httpAddresses.get(RaftPeerId.valueOf(peerIdForIndex(j))))
            .as("%s, node %d must address peer %d at the HTTP port it bound", when, i, j)
            .isEqualTo("localhost:" + getServerHttpPort(j));
    }
  }
}
