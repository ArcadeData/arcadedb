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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code ALTER TYPE ... BUCKET +b}, sent as a statement, left the index of the new bucket without its header page on every
 * follower: the page rode the transaction the statement ran in, which the leader published after the schema entry that creates
 * the file. A follower then loaded an index file it found empty. A UNIQUE index made it quarantine the database and re-install the
 * whole of it from the leader; any other index stayed invalid on the follower, which answered from a scan and left the records of
 * the new bucket out of it.
 */
class RaftAddBucketIndexIT extends BaseRaftHATest {
  private static final String   UNIQUE_TYPE  = "AddBucketUnique";
  private static final String   PLAIN_TYPE   = "AddBucketPlain";
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(60);

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aBucketAddedByAStatementKeepsItsIndexValidOnTheFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    for (final String type : new String[] { UNIQUE_TYPE, PLAIN_TYPE }) {
      leader.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 2").close();
      leader.command("sql", "CREATE PROPERTY " + type + ".k INTEGER").close();
      leader.command("sql", "CREATE INDEX ON " + type + " (k) " + (type.equals(UNIQUE_TYPE) ? "UNIQUE" : "NOTUNIQUE")).close();
      for (int i = 0; i < 20; i++)
        leader.command("sql", "INSERT INTO " + type + " SET k = " + i).close();
      leader.command("sql", "CREATE BUCKET " + type + "Extra").close();
    }
    waitForAllServers();

    // What each node holds before the change: the handle a reinstall of the database from the leader replaces
    final Database[] before = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); i++)
      before[i] = getServerDatabase(i, getDatabaseName());

    final HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    try {
      for (final String type : new String[] { UNIQUE_TYPE, PLAIN_TYPE }) {
        final HttpResponse<String> alter = send(client, leaderIndex, "ALTER TYPE " + type + " BUCKET +" + type + "Extra");
        assertThat(alter.statusCode()).as("ALTER TYPE on %s: %s", type, alter.body()).isEqualTo(200);
        for (int i = 20; i < 30; i++) {
          final HttpResponse<String> insert = send(client, leaderIndex, "INSERT INTO BUCKET:" + type + "Extra SET k = " + i);
          assertThat(insert.statusCode()).as("insert into the new bucket of %s: %s", type, insert.body()).isEqualTo(200);
        }
      }
    } finally {
      client.close();
    }
    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final Database node = getServerDatabase(i, getDatabaseName());
      assertThat(node).as("server %d: the database must not have been re-installed from the leader", i).isSameAs(before[i]);
      for (final String type : new String[] { UNIQUE_TYPE, PLAIN_TYPE }) {
        assertThat(node.getSchema().getType(type).getPolymorphicIndexByProperties("k").isValid())
            .as("server %d: the index of %s must hold a bucket index for the new bucket", i, type).isTrue();
        try (final ResultSet rs = node.query("sql", "SELECT count(*) AS n FROM index:`" + type + "[k]`")) {
          assertThat(rs.next().<Number>getProperty("n").longValue()).as("server %d: entries of the index of %s", i, type)
              .isEqualTo(30L);
        }
      }
    }
  }

  /** The statement posted to the {@code /command} endpoint of the server {@code serverIndex}, as the root user. */
  private HttpResponse<String> send(final HttpClient client, final int serverIndex, final String command) throws Exception {
    return client.send(HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(serverIndex, "/api/v1/command/" + getDatabaseName())))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", command).toString()))
        .build(), HttpResponse.BodyHandlers.ofString());
  }
}
