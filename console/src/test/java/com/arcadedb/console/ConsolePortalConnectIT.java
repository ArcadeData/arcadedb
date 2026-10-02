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
package com.arcadedb.console;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.serializer.json.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code connect portal}: the console twin of Studio's "Connect to ArcadeDB Portal", for servers that run without Studio. It
 * drives the server's {@code /api/v1/server/support/connect} routes against a local stand-in for the portal.
 */
class ConsolePortalConnectIT extends BaseGraphServerTest {
  private static final String DEVICE_CODE = "dgc_DEVICECODE0123456789abcdefghijklmnopqrstuvw";
  private static final String USER_CODE   = "WDJB-MJHT";
  private static final String KEY         = "wsk_MOCKKEY0123456789abcdefghijklmnopqrstuvwxy";

  private HttpServer           portal;
  private volatile Function<String, String[]> pollAnswer;
  private final List<String>   portalPaths = new CopyOnWriteArrayList<>();
  private Console              console;
  private final StringBuilder  out = new StringBuilder();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    try {
      portal = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    portal.createContext("/", this::handle);
    portal.start();
    config.setValue(GlobalConfiguration.SUPPORT_URL, "http://127.0.0.1:" + portal.getAddress().getPort());
  }

  @BeforeEach
  void openConsole() throws Exception {
    console = new Console();
    console.setOutput(out::append);
    // the server polls every second at most; the console only has to look often enough for the test to be quick
    console.setPortalPollIntervalMs(100);
  }

  @AfterEach
  void closeAll() {
    if (console != null)
      console.close();
    if (portal != null)
      portal.stop(0);
  }

  /** The platform's answers; the poll answer is [status, body] chosen by the call number. */
  private void handle(final HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    final String path = exchange.getRequestURI().getPath();
    portalPaths.add(path);
    int status = 404;
    String body = "{\"error\":\"not_found\"}";
    if (path.equals("/public/v1/support/connect/start")) {
      status = 200;
      body = new JSONObject().put("deviceCode", DEVICE_CODE).put("userCode", USER_CODE)
          .put("verifyUrl", "http://127.0.0.1:" + portal.getAddress().getPort() + "/#/connect?code=" + USER_CODE)
          .put("expiresIn", 600).put("interval", 1).toString();
    } else if (path.equals("/public/v1/support/connect/poll")) {
      final String[] answer = pollAnswer.apply(path);
      status = Integer.parseInt(answer[0]);
      body = answer[1];
    } else if (path.equals("/api/v1/support/whoami")) {
      status = 200;
      body = "{\"workspace\":{\"id\":\"ws-mock-1\",\"name\":\"Acme Corp\"},\"key\":{\"id\":\"k1\",\"label\":\"Studio: prod\","
          + "\"scopes\":[\"support:create\",\"support:read\"]},\"plan\":{\"entitled\":true,\"label\":\"Gold\",\"units\":2,"
          + "\"endsOn\":1893456000000},\"sla\":null,\"buyUrl\":\"https://arcadedb.com/pricing.html\"}";
    } else if (path.equals("/api/v1/process-execute")) {
      status = 200;
      body = "{\"output\":{\"data\":{\"status\":\"created\",\"installationId\":\"inst-1\",\"name\":\"arcadedb_0\","
          + "\"filled\":[],\"differs\":[]}}}";
    }
    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private static String[] pending() {
    return new String[] { "400", "{\"error\":\"authorization_pending\",\"interval\":1}" };
  }

  private static String[] approved() {
    return new String[] { "200", new JSONObject().put("clientId", "ws-mock-1").put("workspaceName", "Acme Corp").put("keyId", "k1")
        .put("label", "Studio: host").put("key", KEY).toString() };
  }

  private String url() {
    return "remote:localhost:" + getServerHttpPort() + " root " + DEFAULT_PASSWORD_FOR_TESTS;
  }

  @Test
  void anApprovedConnectionPrintsTheCodeAndTheOutcome() throws Exception {
    final AtomicInteger polls = new AtomicInteger();
    pollAnswer = p -> polls.getAndIncrement() == 0 ? pending() : approved();

    assertThat(console.parse("connect portal " + url())).isTrue();

    final String text = out.toString();
    assertThat(text).contains("Open http://127.0.0.1:" + portal.getAddress().getPort() + "/#/connect?code=" + USER_CODE
        + " and approve. Code: " + USER_CODE + ". Waiting...");
    assertThat(text).contains("Connected to workspace 'Acme Corp'");
    assertThat(text).contains("Registered this server as installation 'arcadedb_0'");
    // neither the device code nor the key reaches the output
    assertThat(text).doesNotContain(DEVICE_CODE).doesNotContain(KEY).doesNotContain("root " + DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(getServer(0).getSupportService().getConfiguration().get()).isNotNull();
  }

  @Test
  void itReusesTheRemoteConnectionTheConsoleAlreadyHas() throws Exception {
    pollAnswer = p -> approved();
    assertThat(console.parse("list databases " + url())).isTrue();
    out.setLength(0);

    assertThat(console.parse("connect portal")).isTrue();

    assertThat(out.toString()).contains("Connected to workspace 'Acme Corp'");
  }

  @Test
  void withoutARemoteServerItSaysHowToConnect() {
    assertThatThrownBy(() -> console.parse("connect portal")).isInstanceOf(ConsoleException.class)
        .hasMessageContaining("connect portal remote:<host>[:<port>] <user> [<password>]");
  }

  @Test
  void aDeniedConnectionIsAFailure() {
    pollAnswer = p -> new String[] { "400", "{\"error\":\"access_denied\"}" };

    assertThatThrownBy(() -> console.parse("connect portal " + url())).isInstanceOf(ConsoleException.class)
        .hasMessageContaining("denied");
    assertThat(getServer(0).getSupportService().getConfiguration().get()).isNull();
  }

  @Test
  void anExpiredCodeIsAFailure() {
    pollAnswer = p -> new String[] { "400", "{\"error\":\"expired_token\"}" };

    assertThatThrownBy(() -> console.parse("connect portal " + url())).isInstanceOf(ConsoleException.class)
        .hasMessageContaining("expired");
  }

  @Test
  void aSecondConnectWhileOneWaitsIsRefusedWithTheServersReason() throws Exception {
    pollAnswer = p -> pending();
    final Thread waiting = new Thread(() -> {
      try {
        console.parse("connect portal " + url());
      } catch (final Exception e) {
        // cancelled below
      }
    });
    waiting.start();
    final long deadline = System.currentTimeMillis() + 10_000;
    while (!out.toString().contains("Waiting...") && System.currentTimeMillis() < deadline)
      Thread.sleep(50);

    final Console second = new Console();
    try {
      assertThatThrownBy(() -> second.parse("connect portal " + url())).hasMessageContaining("connect_in_progress");
    } finally {
      second.close();
      new RemoteServer("localhost", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS).cancelPortalConnect();
      waiting.join(10_000);
    }
  }

  @Test
  void theHistoryNeverKeepsThePassword() {
    assertThat(ConsoleCredentials.mask("connect portal " + url())).doesNotContain(DEFAULT_PASSWORD_FOR_TESTS);
  }
}
