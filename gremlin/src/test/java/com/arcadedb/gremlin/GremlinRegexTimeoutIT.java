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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.gremlin.AbstractGremlinServerIT;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The regular expressions of a Gremlin traversal are evaluated within {@code arcadedb.command.regexTimeout} on every way a
 * traversal reaches the server: the HTTP command endpoint and the bytecode the Gremlin Server receives from a driver. See
 * {@link GremlinRegexTimeoutTest} for the traversals and the patterns.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinRegexTimeoutIT extends AbstractGremlinServerIT {
  private static final String SLOW    = "(.*a){20}$";
  private static final String INPUT   = "a".repeat(26) + "!";
  private static final String SETTING = "arcadedb.command.regexTimeout";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  void limitTheRegexTime() {
    getServerDatabase(0, getDatabaseName()).getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 100L);
  }

  @Test
  void aSlowPatternSentToTheHttpEndpointStopsAtTheTimeout() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort() + "/api/v1/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "gremlin")
          .put("command", "g.inject('" + INPUT + "').is(TextP.regex('" + SLOW + "'))");
      try (final PrintWriter writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        writer.write(payload.toString());
      }

      final int status = connection.getResponseCode();
      final String response = status < 400 ? readResponse(connection) : readError(connection);
      assertThat(status).as("the search is stopped, not answered: %s", response).isGreaterThanOrEqualTo(400);
      assertThat(response).contains(SETTING);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void aSlowPatternSentAsBytecodeStopsAtTheTimeout() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, new ContextConfiguration());
        final ArcadeGraph graph = ArcadeGraph.open(database)) {
      assertThat(graph.traversal().inject("abc").is(TextP.regex("^a")).toList()).containsExactly("abc");

      assertThat(graph.getCluster()).as("the traversal must go through the Gremlin driver").isNotNull();
      assertThatThrownBy(() -> graph.traversal().inject(INPUT).is(TextP.regex(SLOW)).toList()).hasStackTraceContaining(SETTING);
    }
  }
}
