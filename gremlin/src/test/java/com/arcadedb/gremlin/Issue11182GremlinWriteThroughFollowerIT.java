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
package com.arcadedb.gremlin;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11182: one Gremlin write sent to an HA follower through {@code POST /api/v1/command} was applied six times and still
 * answered a {@code ConcurrentModificationException}. The follower analyses a command to decide whether to forward it to the leader,
 * and analysing a Gremlin script ran it: the write was applied on the follower, forwarded and applied again on the leader, the
 * follower's own commit then conflicted, and the handler's retry of a single command repeated all of it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11182GremlinWriteThroughFollowerIT extends BaseRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void oneGremlinWriteThroughAFollowerIsAppliedOnce() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);

    assertThat(post(leader, "command", "sql", "CREATE VERTEX TYPE Probe").statusCode()).isEqualTo(200);
    waitForAllServers();

    final HttpResponse<String> response = post(follower, "command", "gremlin",
        "g.addV('Probe').property('via','follower').iterate()");
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitProbes(i, 1L)).as("server %d (leader %d, write sent to follower %d)", i, leader, follower)
          .isEqualTo(1L);
  }

  @Test
  @Timeout(180)
  void aGremlinReadEndingInATerminalStepRunsOnAFollower() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);

    assertThat(post(leader, "command", "sql", "CREATE VERTEX TYPE Probe").statusCode()).isEqualTo(200);
    assertThat(post(leader, "command", "sql", "CREATE VERTEX Probe SET via = 'leader'").statusCode()).isEqualTo(200);
    waitForAllServers();
    assertThat(awaitProbes(follower, 1L)).isEqualTo(1L);

    // ITS ANALYSIS USED TO REFUSE IT AS A PARSING ERROR: next() ANSWERS A VALUE, NOT A TRAVERSAL
    final HttpResponse<String> response = post(follower, "command", "gremlin", "g.V().hasLabel('Probe').count().next()");
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
    assertThat(new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("result")).isEqualTo(1L);
  }

  /** The records of {@code Probe} on server {@code serverIndex} once replication settled on {@code expected}, or the last count read. */
  private long awaitProbes(final int serverIndex, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> getServerDatabase(serverIndex, getDatabaseName()).countType("Probe", true));
  }

  private HttpResponse<String> post(final int serverIndex, final String endpoint, final String language, final String command)
      throws Exception {
    final String body = new JSONObject().put("language", language).put("command", command).toString();
    final HttpRequest request = HttpRequest.newBuilder(
            URI.create(getServerHttpUrl(serverIndex, "/api/v1/" + endpoint + "/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(60))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString(body)).build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
