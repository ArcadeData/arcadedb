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
package com.arcadedb.server.ha;

import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that RemoteDatabase with FIXED connection strategy works correctly in an HA cluster.
 * Writes go to a specific server, which proxies to the leader if needed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicationServerFixedClientConnectionIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void testFixedConnectionWritesViaProxy() {
    // Connect to a follower (not the leader) with FIXED strategy
    // Writes should be proxied to the leader transparently
    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (getServer(i).getHA() != null && !getServer(i).getHA().isLeader()) {
        followerIndex = i;
        break;
      }
    if (followerIndex < 0)
      followerIndex = 1; // fallback

    final String address = getServer(followerIndex).getHttpServer().getListeningAddress();
    final String[] addressParts = HostUtil.parseHostAddress(address, HostUtil.CLIENT_DEFAULT_PORT);
    final RemoteDatabase db = new RemoteDatabase("http://" + addressParts[0], Integer.parseInt(addressParts[1]),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);

    LogManager.instance().log(this, Level.FINE, "Writing 50 vertices via FIXED connection to follower (server %d)...", followerIndex);

    long counter = 0;
    for (int tx = 0; tx < 10; ++tx) {
      for (int i = 0; i < 5; ++i) {
        final ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter,
            "fixed-connection-test");

        assertThat(resultSet.hasNext()).isTrue();
        final Result result = resultSet.next();
        assertThat(result).isNotNull();
        final Set<String> props = result.getPropertyNames();
        assertThat(props).contains("id", "name");
        assertThat(result.<Long>getProperty("id")).isEqualTo(counter);
      }
    }

    LogManager.instance().log(this, Level.FINE, "Written %d vertices successfully via FIXED connection", counter);

    // Verify data on all servers
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final long count = getServer(i).getDatabase(getDatabaseName()).countType(VERTEX1_TYPE_NAME, true);
      // 1 from setup + 50 from test
      assertThat(count).isGreaterThanOrEqualTo(51);
    }
  }

  @Override
  protected int[] getServerToCheck() {
    return new int[] { 0, 1, 2 };
  }
}
