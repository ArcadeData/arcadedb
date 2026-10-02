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
package com.arcadedb.server.support;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * "Connect to ArcadeDB Portal" (the device authorization flow) against a real server and a local mock of the portal: what is
 * sent to start and to poll, that the key is stored once approved and the server registered, every way the wait can end, and that
 * neither the device code nor the key is in any response.
 */
class SupportConnectTest extends BaseGraphServerTest {
  private static final String DEVICE_CODE = "dgc_DEVICECODE0123456789abcdefghijklmnopqrstuvw";
  private static final String USER_CODE   = "WDJB-MJHT";

  private static MockPortal portal;

  private final List<String> everyBody = new ArrayList<>();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    try {
      if (portal == null)
        portal = new MockPortal();
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    config.setValue(GlobalConfiguration.SUPPORT_URL, portal.url());
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (getServer(0) != null) {
      service().getConnector().cancel();
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "");
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, "");
      call("DELETE", "/api/v1/server/support/register", null);
    }
    portal.close();
    portal = null;
  }

  private SupportService service() {
    return getServer(0).getSupportService();
  }

  private record Resp(int status, String body) {
    JSONObject json() {
      return new JSONObject(body);
    }
  }

  private Resp call(final String method, final String path, final String body) throws Exception {
    final int port = getServer(0).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URI("http://localhost:" + port + path).toURL().openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      if (body != null) {
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
      }
      final int status = connection.getResponseCode();
      final InputStream stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String text = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      everyBody.add(text);
      return new Resp(status, text);
    } finally {
      connection.disconnect();
    }
  }

  private static String startAnswer() {
    return new JSONObject().put("deviceCode", DEVICE_CODE).put("userCode", USER_CODE)
        .put("verifyUrl", portal.url() + "/#/connect?code=" + USER_CODE).put("expiresIn", 600).put("interval", 1).toString();
  }

  /** The portal as the platform answers the device flow; {@code pollAnswers} are consumed one per poll, the last one repeats. */
  private void portalPolls(final List<MockPortal.Response> pollAnswers) {
    final MockPortal contract = portal;
    final Function<MockPortal.Recorded, MockPortal.Response> base = contract.handler;
    final AtomicInteger polls = new AtomicInteger();
    portal.handler = r -> {
      if (r.path().equals(SupportConnector.START_PATH))
        return new MockPortal.Response(200, startAnswer());
      if (r.path().equals(SupportConnector.POLL_PATH))
        return pollAnswers.get(Math.min(polls.getAndIncrement(), pollAnswers.size() - 1));
      return base.apply(r);
    };
  }

  private static MockPortal.Response pending() {
    return new MockPortal.Response(400, "{\"error\":\"authorization_pending\",\"interval\":1}");
  }

  private static MockPortal.Response approved() {
    return new MockPortal.Response(200, new JSONObject().put("clientId", MockPortal.CLIENT_ID).put("workspaceName", "Acme Corp")
        .put("keyId", "k1").put("label", "Studio: host").put("key", MockPortal.KEY).toString());
  }

  private JSONObject waitFor(final String wanted) throws Exception {
    final long deadline = System.currentTimeMillis() + 15_000L;
    JSONObject status;
    do {
      status = call("GET", "/api/v1/server/support/connect", null).json();
      if (status.getString("status").equals(wanted))
        return status;
      Thread.sleep(100);
    } while (System.currentTimeMillis() < deadline);
    throw new AssertionError("the connection never became '" + wanted + "': " + status);
  }

  private void assertNoSecretServed() {
    for (final String body : everyBody)
      assertThat(body).doesNotContain(MockPortal.KEY).doesNotContain("wsk_MOCKKEY").doesNotContain(DEVICE_CODE);
  }

  @Test
  void anApprovedConnectionStoresTheKeyAndRegistersTheServer() throws Exception {
    portalPolls(List.of(pending(), approved()));

    final Resp start = call("POST", "/api/v1/server/support/connect", "{\"label\":\"prod\"}");
    assertThat(start.status()).isEqualTo(200);
    assertThat(start.json().getString("userCode")).isEqualTo(USER_CODE);
    assertThat(start.json().getString("verifyUrl")).isEqualTo(portal.url() + "/#/connect?code=" + USER_CODE);
    assertThat(start.json().getInt("expiresIn")).isEqualTo(600);
    assertThat(start.json().has("deviceCode")).isFalse();

    // what the portal was told: who is asking, never a credential
    final MockPortal.Recorded sent = portal.requests.get(0);
    assertThat(sent.method()).isEqualTo("POST");
    assertThat(sent.path()).isEqualTo(SupportConnector.START_PATH);
    assertThat(sent.header("authorization")).isNull();
    final JSONObject startBody = new JSONObject(sent.bodyText());
    assertThat(startBody.getString("instanceId")).isEqualTo(getServer(0).getInstanceId());
    assertThat(startBody.getString("host")).isNotBlank();
    assertThat(startBody.getString("version")).isNotBlank();
    assertThat(startBody.getString("label")).isEqualTo("prod");
    // the same facts as flat attributes the portal interprets; a standalone server has no cluster
    final JSONObject attributes = startBody.getJSONObject("attributes");
    assertThat(attributes.getString("host")).isEqualTo(startBody.getString("host"));
    assertThat(attributes.getString("version")).isEqualTo(startBody.getString("version"));
    assertThat(attributes.getString("serverName")).isEqualTo(getServer(0).getServerName());
    assertThat(attributes.has("clusterName")).isFalse();
    assertThat(attributes.has("haNodes")).isFalse();

    assertThat(call("GET", "/api/v1/server/support/connect", null).json().getString("status")).isIn("pending", "connected");

    final JSONObject done = waitFor("connected");
    assertThat(done.getString("workspaceName")).isEqualTo("Acme Corp");
    assertThat(done.getJSONObject("registration").getString("status")).isEqualTo("created");
    assertThat(done.getJSONObject("registration").getString("name")).isEqualTo("arcadedb_0");

    // the poll carried the device code and the instance id, and nothing was sent with a key before it was received
    final MockPortal.Recorded poll = portal.requests.stream().filter(r -> r.path().equals(SupportConnector.POLL_PATH)).findFirst()
        .orElseThrow();
    assertThat(poll.header("authorization")).isNull();
    assertThat(new JSONObject(poll.bodyText()).getString("deviceCode")).isEqualTo(DEVICE_CODE);
    assertThat(new JSONObject(poll.bodyText()).getString("instanceId")).isEqualTo(getServer(0).getInstanceId());

    // stored as a manual registration, then used: whoami, then the process that registers the installation
    final SupportConfiguration.Registration stored = service().getConfiguration().get();
    assertThat(stored).isNotNull();
    assertThat(stored.getClientId()).isEqualTo(MockPortal.CLIENT_ID);
    assertThat(stored.getKey()).isEqualTo(MockPortal.KEY);
    assertThat(portal.requests.stream().anyMatch(r -> r.path().equals("/api/v1/support/whoami"))).isTrue();
    final MockPortal.Recorded process = portal.requests.stream().filter(r -> r.path().equals("/api/v1/process-execute")).findFirst()
        .orElseThrow();
    assertThat(process.header("x-api-process")).isEqualTo("studio-register-instance");
    assertThat(call("GET", "/api/v1/server/support", null).json().getBoolean("registered")).isTrue();
    assertNoSecretServed();
  }

  @Test
  void aRefusedRegistrationOfTheInstallationDoesNotUndoTheConnection() throws Exception {
    final MockPortal.Response first = approved();
    portalPolls(List.of(first));
    final Function<MockPortal.Recorded, MockPortal.Response> inner = portal.handler;
    portal.handler = r -> r.path().equals("/api/v1/process-execute") ? new MockPortal.Response(500,
        MockPortal.error("process_failed", "Process 'studio-register-instance' failed: Error: instance_id.taken: held"))
        : inner.apply(r);

    call("POST", "/api/v1/server/support/connect", null);
    final JSONObject done = waitFor("connected");
    assertThat(done.getJSONObject("registration").getString("error")).isEqualTo("instance_id.taken");
    assertThat(service().getConfiguration().get()).isNotNull();
    assertNoSecretServed();
  }

  @Test
  void aDeniedConnectionStoresNothing() throws Exception {
    portalPolls(List.of(pending(), new MockPortal.Response(400, "{\"error\":\"access_denied\"}")));
    call("POST", "/api/v1/server/support/connect", null);
    assertThat(waitFor("denied").has("registration")).isFalse();
    assertThat(service().getConfiguration().get()).isNull();
    // nobody named the key: no label is sent, the portal derives one
    assertThat(new JSONObject(portal.requests.get(0).bodyText()).has("label")).isFalse();
  }

  @Test
  void anExpiredCodeIsReported() throws Exception {
    portalPolls(List.of(new MockPortal.Response(400, "{\"error\":\"expired_token\"}")));
    call("POST", "/api/v1/server/support/connect", null);
    waitFor("expired");
    assertThat(service().getConfiguration().get()).isNull();
  }

  @Test
  void aWorkspaceAtItsKeyLimitIsReportedAsAnErrorWithItsCode() throws Exception {
    portalPolls(List.of(new MockPortal.Response(409, "{\"error\":\"key_limit\"}")));
    call("POST", "/api/v1/server/support/connect", null);
    assertThat(waitFor("error").getJSONObject("error").getString("error")).isEqualTo("key_limit");
  }

  @Test
  void aKeyThatThePortalDoesNotConfirmIsNotStored() throws Exception {
    portalPolls(List.of(approved()));
    final Function<MockPortal.Recorded, MockPortal.Response> inner = portal.handler;
    portal.handler = r -> r.path().equals("/api/v1/support/whoami") ? new MockPortal.Response(401,
        MockPortal.error("invalid_key", "no")) : inner.apply(r);
    call("POST", "/api/v1/server/support/connect", null);
    assertThat(waitFor("error").getJSONObject("error").getString("error")).isEqualTo("invalid_key");
    assertThat(service().getConfiguration().get()).isNull();
    assertNoSecretServed();
  }

  @Test
  void onlyOneConnectionWaitsAtATimeAndItCanBeCancelled() throws Exception {
    portalPolls(List.of(pending()));
    assertThat(call("POST", "/api/v1/server/support/connect", null).status()).isEqualTo(200);
    final Resp second = call("POST", "/api/v1/server/support/connect", null);
    assertThat(second.status()).isEqualTo(409);
    assertThat(second.json().getString("error")).isEqualTo("connect_in_progress");

    assertThat(call("DELETE", "/api/v1/server/support/connect", null).status()).isEqualTo(204);
    assertThat(call("GET", "/api/v1/server/support/connect", null).json().getString("status")).isEqualTo("none");
    // and a new one can start
    assertThat(call("POST", "/api/v1/server/support/connect", null).status()).isEqualTo(200);
  }

  @Test
  void aServerRegisteredThroughTheSettingsCannotBeConnected() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, MockPortal.CLIENT_ID);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, MockPortal.KEY);
    portalPolls(List.of(pending()));
    final Resp resp = call("POST", "/api/v1/server/support/connect", null);
    assertThat(resp.status()).isEqualTo(409);
    assertThat(resp.json().getString("error")).isEqualTo("registered_by_settings");
    assertThat(portal.requests.stream().anyMatch(r -> r.path().equals(SupportConnector.START_PATH))).isFalse();
  }

  @Test
  void aPortalThatCannotConnectServersAndAnUnreachableOneAreReported() throws Exception {
    // a portal that has no such route
    portal.handler = r -> new MockPortal.Response(404, MockPortal.error("not_found", "No such route"));
    final Resp old = call("POST", "/api/v1/server/support/connect", null);
    assertThat(old.status()).isEqualTo(404);
    assertThat(old.json().getString("error")).isEqualTo("not_supported");

    portal.handler = r -> new MockPortal.Response(429, MockPortal.error("rate_limited", "x"));
    assertThat(call("POST", "/api/v1/server/support/connect", null).json().getString("error")).isEqualTo("rate_limited");

    portal.handler = r -> new MockPortal.Response(200, "{\"deviceCode\":\"x\",\"userCode\":\"y\",\"verifyUrl\":\"javascript:alert(1)\"}");
    assertThat(call("POST", "/api/v1/server/support/connect", null).json().getString("error")).isEqualTo("portal_error");
  }
}
