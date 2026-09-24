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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8270, sibling path: high availability requested implicitly - a {@code ha.serverList} and no
 * {@code ha.enabled} - starts the Raft plugin, and the plugin installs its database wrapper, but the server applied
 * that wrapper to a database it opened or created only when {@code ha.enabled} was set. A database created on such a
 * cluster after startup was therefore the plain local one: the create never reached the other nodes, and neither did
 * any write to it.
 */
class Issue8270ImplicitHADatabaseWrappedIT extends BaseRaftHATest {

  private static final String CREATED = "implicit8270";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
  }

  @Test
  void aDatabaseCreatedOnAnImplicitlyEnabledClusterIsReplicated() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    assertThat(getServer(leaderIndex).getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isFalse();

    final ServerDatabase created = new ServerControlPlane(getServer(leaderIndex)).createDatabase(CREATED);
    assertThat(created.getWrappedDatabaseInstance()).isInstanceOf(RaftReplicatedDatabase.class);

    final int followerIndex = (leaderIndex + 1) % getServerCount();
    Awaitility.await("database created on the follower").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
        .until(() -> getServer(followerIndex).existsDatabase(CREATED));

    created.command("sql", "CREATE DOCUMENT TYPE Doc");
    created.command("sql", "INSERT INTO Doc SET n = 1");
    waitForAllServers();

    Awaitility.await("write replicated to the follower").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
        .ignoreExceptions()
        .until(() -> getServer(followerIndex).getDatabase(CREATED).countType("Doc", true) == 1L);
  }
}
