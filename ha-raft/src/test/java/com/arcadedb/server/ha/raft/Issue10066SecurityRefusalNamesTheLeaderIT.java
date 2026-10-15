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

import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #10066: the refusal of a {@code SecurityManager} user mutation on a follower named the leader by
 * {@code HAServerPlugin.getLeaderAddress()}, unvetted. On a cluster whose peers share a host and declare no HTTP port (issue
 * #6191) that is the follower's OWN address, so the refusal sent the client back to the node that had just refused it. It now
 * names the leader like the schema refusal does (issue #10060): its HTTP address when that identifies it, its server name when
 * it does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10066SecurityRefusalNamesTheLeaderIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(120)
  void aSecurityRefusalOnAFollowerThatCannotTellItsPeersApartNamesTheLeaderByName() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try {
      final ServerIsNotTheLeaderException refusal = securityRefusalOn(follower, "issue10066ambiguous");
      final String leaderName = raft.getLeaderName();
      assertThat(leaderName).as("the follower must know which node leads").isNotBlank();
      assertThat(raft.isOwnHttpAddress(refusal.getLeaderAddress()))
          .as("the refusal must not send the client back to the follower that refused it (named '%s')", refusal.getLeaderAddress())
          .isFalse();
      assertThat(refusal.getLeaderAddress()).isEqualTo(leaderName);
    } finally {
      restore.run();
    }
  }

  @Test
  @Timeout(120)
  void aSecurityRefusalOnAFollowerOfAWellConfiguredClusterNamesTheLeadersHttpAddress() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final String leaderHttpAddress = getRaftPlugin(follower).getRaftHAServer().getLeaderHttpAddress();

    final ServerIsNotTheLeaderException refusal = securityRefusalOn(follower, "issue10066configured");
    assertThat(refusal.getLeaderAddress()).isEqualTo(leaderHttpAddress);
  }

  private ServerIsNotTheLeaderException securityRefusalOn(final int follower, final String user) {
    final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
        () -> getServer(follower).getSecurity().createUser(user, "issue10066password"));
    assertThat(refusal).as("a user mutation on a follower must be refused as a leader refusal").isNotNull();
    assertThat(refusal.getMessage()).contains("must run on the cluster leader");
    return refusal;
  }
}
