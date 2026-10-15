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
package com.arcadedb.mcp;

import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.QueryMetricsRecorder;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10094: MCP was the one wire surface that never tagged its request thread with {@link ProtocolContext}. The tag
 * is what tells a client request from the engine's own work: under HA, {@code RaftReplicatedDatabase} refuses a CLIENT
 * request on a database whose directory is being replaced from the leader's snapshot, and an untagged thread reads as
 * {@link ProtocolContext#INTERNAL} and is served from the copy the cluster is discarding. It is also what the per-protocol
 * query metrics and the trace spans attribute the work to, so an agent's queries were reported as {@code internal}
 * (stdio) or {@code http} (HTTP transport), never as MCP.
 * <p>
 * The tag is set in {@link MCPDispatcher#dispatch}, the one entry point both transports share, and the thread is handed
 * back as it was found: the HTTP transport runs inside the {@code http} tag of its handler, which must survive.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10094MCPProtocolTagTest extends BaseGraphServerTest {
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private final List<String>     recordedProtocols = new CopyOnWriteArrayList<>();
  private       QueryMetricsRecorder previousRecorder;
  private       MCPConfiguration   mcp;
  private       boolean            savedEnabled;
  private       List<String>       savedUsers;

  @BeforeEach
  void enableMCPAndRecordQueries() {
    mcp = MCPPlugin.of(getServer(0)).getConfiguration();
    savedEnabled = mcp.isEnabled();
    savedUsers = mcp.getAllowedUsers();
    mcp.setEnabled(true);
    mcp.setAllowedUsers(List.of("root"));

    previousRecorder = QueryMetricsRecorder.Holder.get();
    final String database = getDatabaseName();
    // Only what the tool ran on the test database: other threads of the fixture run queries of their own
    QueryMetricsRecorder.Holder.register((protocol, db, language, type, nanos) -> {
      if (database.equals(db))
        recordedProtocols.add(protocol);
    });
  }

  @AfterEach
  void restore() {
    QueryMetricsRecorder.Holder.register(previousRecorder);
    mcp.setEnabled(savedEnabled);
    mcp.setAllowedUsers(savedUsers);
    ProtocolContext.clear();
  }

  @Test
  void aToolRunsTaggedAsMcpWhateverTheTransport() throws Exception {
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final MCPStdioServer stdio = new MCPStdioServer(getServer(0), mcp, user,
        new ByteArrayInputStream((queryCall().toString() + "\n").getBytes(StandardCharsets.UTF_8)),
        new PrintStream(out, true, StandardCharsets.UTF_8));

    // The stdio loop runs on this thread, which no listener ever tagged: it used to read "internal"
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
    assertThat(stdio.run()).isTrue();

    assertThat(new JSONObject(out.toString(StandardCharsets.UTF_8).trim()).getJSONObject("result").getBoolean("isError")).isFalse();
    assertThat(recordedProtocols).as("the query the tool ran is attributed to MCP").isNotEmpty().containsOnly("mcp");
  }

  @Test
  void theHttpTransportIsAttributedToMcpNotToHttp() throws Exception {
    final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/mcp")))
        .header("Content-Type", "application/json")
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString(queryCall().toString())).build(), HttpResponse.BodyHandlers.ofString());

    assertThat(new JSONObject(response.body()).getJSONObject("result").getBoolean("isError")).isFalse();
    assertThat(recordedProtocols).isNotEmpty().contains("mcp").doesNotContain("http");
  }

  @Test
  void theThreadIsHandedBackAsItWasFound() {
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
    final MCPDispatcher dispatcher = new MCPDispatcher(getServer(0), mcp, "http");

    // The HTTP transport dispatches from inside the tag of its handler: it is restored, not cleared
    ProtocolContext.set("http");
    dispatcher.dispatch(queryCall(), user, MCPStdioServer.STDIO_CLIENT);
    assertThat(ProtocolContext.get()).isEqualTo("http");

    // A pooled worker thread is reused: nothing is left behind for the next request it serves
    ProtocolContext.clear();
    dispatcher.dispatch(queryCall(), user, MCPStdioServer.STDIO_CLIENT);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void aFailingToolAndAMalformedRequestAlsoHandTheThreadBack() {
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
    final MCPDispatcher dispatcher = new MCPDispatcher(getServer(0), mcp, "http");

    ProtocolContext.set("http");
    final JSONObject failing = new JSONObject().put("jsonrpc", "2.0").put("id", 2).put("method", "tools/call").put("params",
        new JSONObject().put("name", "query").put("arguments",
            new JSONObject().put("database", "noSuchDatabase10094").put("language", "sql").put("query", "select 1")));
    assertThat(dispatcher.dispatch(failing, user, MCPStdioServer.STDIO_CLIENT).json().getJSONObject("result").getBoolean("isError"))
        .isTrue();
    assertThat(ProtocolContext.get()).isEqualTo("http");

    dispatcher.dispatch(new JSONObject().put("jsonrpc", "2.0").put("id", 3).put("method", "no/such/method"), user,
        MCPStdioServer.STDIO_CLIENT);
    assertThat(ProtocolContext.get()).isEqualTo("http");
  }

  private JSONObject queryCall() {
    return new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call").put("params",
        new JSONObject().put("name", "query").put("arguments",
            new JSONObject().put("database", getDatabaseName()).put("language", "sql").put("query", "select from V1 limit 1")));
  }
}
