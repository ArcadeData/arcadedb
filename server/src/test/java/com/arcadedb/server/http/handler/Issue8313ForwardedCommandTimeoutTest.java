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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandTimeoutOverride;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8313, the leader's half: a follower's forward carries the {@code arcadedb.command.timeout} budget it waits for
 * in {@link LeaderForwardContext#FORWARDED_COMMAND_TIMEOUT_HEADER}, and the node that receives it enforces that budget
 * in place of its own database setting - under a valid cluster token only. From a client the header would let a
 * request lift the budget its database imposes, so without the token it is ignored.
 * <p>
 * The statement is a self-join over {@link #NODES} vertices, far more than the 1 ms budget; the database here has no
 * budget of its own, so a request that is not bounded by the header runs it to completion.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8313ForwardedCommandTimeoutTest extends BaseGraphServerTest {
  private static final String     CLUSTER_TOKEN = "issue8313-cluster-secret-token";
  private static final int        NODES         = 600;
  private static final String     QUERY         =
      "MATCH {type: Issue8313Node, as: a}, {type: Issue8313Node, as: b, where: (v + $matched.a.v = -1)} RETURN count(*) AS c";
  private static final HttpClient HTTP          = HttpClient.newHttpClient();

  @BeforeEach
  void setUpData() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType("Issue8313Node")) {
      database.getSchema().createVertexType("Issue8313Node");
      database.transaction(() -> {
        for (int i = 0; i < NODES; i++)
          database.newVertex("Issue8313Node").set("v", i).save();
      });
    }
  }

  @AfterEach
  void clearClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
  }

  @Test
  void aForwardedBudgetIsEnforcedUnderTheClusterToken() throws Exception {
    final HttpResponse<String> response = command(b -> b
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .header("X-ArcadeDB-Forwarded-User", "root")
        .header(LeaderForwardContext.FORWARDED_COMMAND_TIMEOUT_HEADER, "1"));

    assertThat(response.statusCode()).as("body: %s", response.body()).isNotEqualTo(200);
    assertThat(response.body()).contains(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 1ms");
    assertThat(CommandTimeoutOverride.get()).as("the budget is scoped to the request, never left on a thread").isEqualTo(-1L);
  }

  @Test
  void aClientCannotSetTheBudgetWithoutTheClusterToken() throws Exception {
    final HttpResponse<String> response = command(b -> b
        .header("Authorization", basic("root", DEFAULT_PASSWORD_FOR_TESTS))
        .header(LeaderForwardContext.FORWARDED_COMMAND_TIMEOUT_HEADER, "1"));

    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
    assertThat(response.body()).doesNotContain(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
  }

  private HttpResponse<String> command(final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + getServer(0).getHttpServer().getPort()
            + "/api/v1/command/" + getDatabaseName()))
        .header("Content-Type", "application/json");
    builder = decorate.apply(builder);
    final String body = "{\"language\":\"sql\",\"command\":\"" + QUERY.replace("\"", "\\\"") + "\"}";
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
