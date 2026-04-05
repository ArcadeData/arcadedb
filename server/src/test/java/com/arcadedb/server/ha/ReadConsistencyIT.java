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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests read consistency levels (EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE) on HA clusters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReadConsistencyIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void testReadYourWritesConsistency() {
    final ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();

    // Find a follower HTTP port
    int followerPort = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer s = getServer(i);
      if (s != null && s.isStarted() && s.getHA() != null && !s.getHA().isLeader()) {
        followerPort = s.getHttpServer().getPort();
        break;
      }
    }
    assertThat(followerPort).isGreaterThan(0);

    // Create a RemoteDatabase with READ_YOUR_WRITES consistency, connected to the follower
    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", followerPort, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    db.setReadConsistency(Database.READ_CONSISTENCY.READ_YOUR_WRITES);

    // Write some data (this goes to the leader via proxy)
    for (int i = 0; i < 10; i++)
      db.command("SQL", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", (long) (50000 + i), "ryw-test");

    // The bookmark should have been updated from write responses
    assertThat(db.getLastCommitIndex()).isGreaterThanOrEqualTo(0);

    // Read the data - with READ_YOUR_WRITES, the follower should wait until it has applied
    // at least up to the bookmark before executing the query
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME + " WHERE name = 'ryw-test'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(10L);

    db.close();
  }

  @Test
  void testEventualConsistencyDefault() {
    // With no read consistency set (EVENTUAL default), reads should still work on followers
    final ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();

    // Write via server API
    for (int i = 0; i < 5; i++) {
      final int idx = i;
      leader.getDatabase(getDatabaseName()).transaction(() ->
          leader.getDatabase(getDatabaseName()).newVertex(VERTEX1_TYPE_NAME)
              .set("id", (long) (60000 + idx)).set("name", "eventual-test").save()
      );
    }
    CodeUtils.sleep(2000);

    // Find follower and query with EVENTUAL (default)
    int followerPort = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer s = getServer(i);
      if (s != null && s.isStarted() && s.getHA() != null && !s.getHA().isLeader()) {
        followerPort = s.getHttpServer().getPort();
        break;
      }
    }
    assertThat(followerPort).isGreaterThan(0);

    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", followerPort, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    // No read consistency set - defaults to EVENTUAL

    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME + " WHERE name = 'eventual-test'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    // After 2s sleep, replication should have caught up even with EVENTUAL
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(5L);

    db.close();
  }

  @Test
  void testLinearizableConsistency() {
    final ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();

    int followerPort = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer s = getServer(i);
      if (s != null && s.isStarted() && s.getHA() != null && !s.getHA().isLeader()) {
        followerPort = s.getHttpServer().getPort();
        break;
      }
    }
    assertThat(followerPort).isGreaterThan(0);

    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", followerPort, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    db.setReadConsistency(Database.READ_CONSISTENCY.LINEARIZABLE);

    // Write via the same RemoteDatabase (proxied to leader)
    for (int i = 0; i < 5; i++)
      db.command("SQL", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", (long) (70000 + i), "linear-test");

    // With LINEARIZABLE, the follower contacts the leader for the current commit index
    // and waits before executing the read - should see all writes immediately
    final ResultSet rs = db.query("SQL", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME + " WHERE name = 'linear-test'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(5L);

    db.close();
  }
}
