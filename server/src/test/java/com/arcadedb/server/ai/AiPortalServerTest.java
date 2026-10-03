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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The AI Assistant of a server connected to the customer portal: the HTTP surface Studio uses ({@code /api/v1/ai/*}) against a
 * fake portal, with the real tools running on the real test database.
 */
class AiPortalServerTest extends BaseGraphServerTest {
  private FakeAiPortal portal;
  private long         savedUsageTtl;
  private long         savedFailureTtl;

  @BeforeEach
  void connect() throws Exception {
    portal = new FakeAiPortal();
    AiSchemaDigest.clearCache();
    // Every request reads the plan again, so a test sees what it just set
    savedUsageTtl = AiPortal.usageTtlMs;
    savedFailureTtl = AiPortal.failureTtlMs;
    AiPortal.usageTtlMs = 0L;
    AiPortal.failureTtlMs = 0L;
    getServer(0).getSupportService().getConfiguration().save(portal.url(), FakeAiPortal.CLIENT_ID, FakeAiPortal.KEY);
  }

  @AfterEach
  void disconnect() throws IOException {
    AiPortal.usageTtlMs = savedUsageTtl;
    AiPortal.failureTtlMs = savedFailureTtl;
    Files.deleteIfExists(Path.of(getServer(0).getConfigPath(), "support.json"));
    portal.close();
    final File rootDir = new File("./target/chats/" + ChatStorage.hashUsername("root"));
    final File[] files = rootDir.listFiles();
    if (files != null)
      for (final File f : files)
        //noinspection ResultOfMethodCallIgnored
        f.delete();
  }

  private record Reply(int status, String body, String contentType) {
    JSONObject json() {
      return new JSONObject(body);
    }

    /** The {@code data: } events of a streamed answer. */
    List<JSONObject> events() {
      final List<JSONObject> events = new ArrayList<>();
      for (final String frame : body.split("\n\n"))
        if (frame.trim().startsWith("data: "))
          events.add(new JSONObject(frame.trim().substring(6)));
      return events;
    }
  }

  private Reply call(final String method, final String path, final JSONObject body) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ai" + path).toURL().openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (body != null) {
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(body.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
    final int status = conn.getResponseCode();
    final var stream = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
    final String text = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    final String type = conn.getContentType();
    conn.disconnect();
    return new Reply(status, text, type);
  }

  private JSONObject question() {
    return new JSONObject().put("database", getDatabaseName()).put("message", "What types exist?");
  }

  @Test
  void configSaysTheAssistantIsAvailableThroughThePortal() throws Exception {
    final JSONObject config = call("GET", "/config", null).json();

    assertThat(config.getBoolean("configured")).isTrue();
    assertThat(config.getString("source")).isEqualTo("portal");
    final JSONObject status = config.getJSONObject("portal");
    assertThat(status.getBoolean("connected")).isTrue();
    assertThat(status.getBoolean("enabled")).isTrue();
    assertThat(status.getString("tier")).isEqualTo("assistant");
    assertThat(status.getInt("turns")).isEqualTo(3);
    assertThat(status.getDouble("budget")).isEqualTo(20.0);
    assertThat(status.getDouble("spent")).isEqualTo(3.0);
    assertThat(status.getInt("percent")).isEqualTo(15);
    assertThat(status.has("limit")).isFalse();
    assertThat(status.getString("upgradeUrl")).isEqualTo(portal.url() + "/#/subscription");
    // The key is a credential: nothing that goes to the browser carries it
    assertThat(call("GET", "/config", null).body()).doesNotContain(FakeAiPortal.KEY).doesNotContain("wsk_");
  }

  @Test
  void aPlanWithoutTheAssistantIsNotConfiguredAndSaysWhy() throws Exception {
    portal.usage = new JSONObject().put("tier", "none").put("enabled", false);

    final JSONObject config = call("GET", "/config", null).json();

    assertThat(config.getBoolean("configured")).isFalse();
    assertThat(config.getString("source")).isEqualTo("none");
    assertThat(config.getJSONObject("portal").getBoolean("connected")).isTrue();
    assertThat(config.getJSONObject("portal").getBoolean("enabled")).isFalse();
    assertThat(config.getJSONObject("portal").getString("upgradeUrl")).isNotEmpty();
  }

  @Test
  void aPortalThatCannotBeReadIsReportedWithoutBreakingTheConfig() throws Exception {
    portal.routeFailure = process -> new String[] { "403", "{\"error\":\"scope_denied\",\"message\":\"no\"}" };

    final Reply reply = call("GET", "/config", null);

    assertThat(reply.status()).isEqualTo(200);
    final JSONObject status = reply.json().getJSONObject("portal");
    assertThat(status.getBoolean("enabled")).isFalse();
    assertThat(status.getString("code")).isEqualTo("scope_denied");
    assertThat(reply.json().getBoolean("configured")).isFalse();
  }

  @Test
  void aLegacyGatewayKeyKeepsWorkingWhenThePlanLacksTheAssistant() throws Exception {
    portal.usage = new JSONObject().put("tier", "none").put("enabled", false);
    getServer(0).getAiConfiguration().activate("legacy-key", "127.0.0.1", "hw", "v");
    try {
      final JSONObject config = call("GET", "/config", null).json();
      assertThat(config.getBoolean("configured")).isTrue();
      assertThat(config.getString("source")).isEqualTo("gateway");
    } finally {
      final File ai = new File(getServer(0).getConfigPath(), "ai.json");
      Files.deleteIfExists(ai.toPath());
      getServer(0).getAiConfiguration().activate("", "", "", "");
    }
  }

  @Test
  void theStreamedChatRunsToolsLocallyAndAnswersInTheStudioShape() throws Exception {
    portal.script = body -> body.getInt("round") == 0
        ? FakeAiPortal.answer("", List.of(FakeAiPortal.toolCall("t1", "get_type", new JSONObject().put("name", VERTEX1_TYPE_NAME))))
        : FakeAiPortal.answer("Here are the types",
            List.of(), new JSONArray().put(new JSONObject().put("purpose", "list").put("language", "sql").put("command", "select 1")));

    final Reply reply = call("POST", "/chat/stream", question());

    assertThat(reply.status()).isEqualTo(200);
    assertThat(reply.contentType()).contains("text/event-stream");
    final List<JSONObject> events = reply.events();
    assertThat(events).extracting(e -> e.getString("type")).containsSubsequence("tool_start", "tool_end", "delta", "done");
    final JSONObject done = events.get(events.size() - 1);
    assertThat(done.getString("type")).isEqualTo("done");
    assertThat(done.getString("response")).isEqualTo("Here are the types");
    assertThat(done.getString("chatId")).isNotBlank();
    assertThat(done.getJSONArray("commands").length()).isEqualTo(1);
    assertThat(done.getJSONObject("usage").getDouble("budget")).isEqualTo(20.0);
    assertThat(done.getJSONObject("usage").getDouble("spent")).isEqualTo(3.41);
    assertThat(events.stream().filter(e -> e.getString("type").equals("tool_end")).findFirst().orElseThrow().has("error")).isFalse();

    // the second turn carries what the REAL get_type returned on this database
    assertThat(portal.sends).hasSize(2);
    final JSONObject results = portal.sends.get(1).getJSONArray("toolResults").getJSONObject(0);
    assertThat(results.getString("name")).isEqualTo("get_type");
    assertThat(new JSONObject(results.getString("result")).getString("name")).isEqualTo(VERTEX1_TYPE_NAME);
    // the compact schema summary of this database goes with EVERY round
    for (final JSONObject sent : portal.sends) {
      assertThat(sent.getString("schemaDigest")).startsWith("Database " + getDatabaseName() + ": ").contains("vertex " + VERTEX1_TYPE_NAME + " ~");
      assertThat(sent.getString("schemaDigest").length()).isLessThanOrEqualTo(AiSchemaDigest.MAX_CHARS);
    }
    // the first carries the question, the database, no schema (the tools fetch it) and no timestamps in the history
    final JSONObject first = portal.sends.get(0);
    assertThat(first.getString("message")).isEqualTo("What types exist?");
    assertThat(first.getString("database")).isEqualTo(getDatabaseName());
    assertThat(first.has("schema")).isFalse();
    assertThat(first.getString("mode")).isEqualTo("chat");

    // and the chat was saved before the stream ended
    final JSONObject chats = call("GET", "/chats", null).json();
    assertThat(chats.getJSONArray("chats").length()).isEqualTo(1);
  }

  @Test
  void theReviewFirstChatLetsTheModelReadButNeverRunsAQuery() throws Exception {
    portal.script = body -> body.getInt("round") == 0
        ? FakeAiPortal.answer("", List.of(FakeAiPortal.toolCall("t1", "query_database",
            new JSONObject().put("language", "sql").put("command", "select from V1"))))
        : FakeAiPortal.answer("review-first reply", List.of());

    final Reply reply = call("POST", "/chat", question());

    assertThat(reply.status()).isEqualTo(200);
    assertThat(reply.contentType()).contains("application/json");
    assertThat(reply.json().getString("response")).isEqualTo("review-first reply");
    assertThat(reply.json().getString("chatId")).isNotBlank();
    // the query was refused here, with the sentence that makes the model hand the command back
    final JSONObject result = portal.sends.get(1).getJSONArray("toolResults").getJSONObject(0);
    assertThat(result.getString("result")).contains("Review-first mode");
  }

  @Test
  void aPlanWithoutTheAssistantAnswers402WithTheUpgradeAndNothingIsStreamed() throws Exception {
    portal.refusal = body -> "ai.not_entitled: Your plan does not include the AI Assistant";

    final Reply reply = call("POST", "/chat/stream", question());

    assertThat(reply.status()).isEqualTo(402);
    final JSONObject error = reply.json();
    assertThat(error.getString("code")).isEqualTo("ai.not_entitled");
    assertThat(error.getBoolean("upgrade")).isTrue();
    assertThat(error.getString("error")).isNotEmpty();
  }

  @Test
  void anExhaustedAllowanceAnswers429() throws Exception {
    portal.refusal = body -> "ai.allowance_exhausted: the assistant plan's AI allowance for this month ($20.00) is used up; it starts over on 2026-11-01";

    final Reply reply = call("POST", "/chat", question());

    assertThat(reply.status()).isEqualTo(429);
    assertThat(reply.json().getString("code")).isEqualTo("ai.allowance_exhausted");
    assertThat(reply.json().getBoolean("upgrade")).isTrue();
  }

  @Test
  void aKeyTheYouPortalRejectsIsNeverPassedOnAs401Or403() throws Exception {
    // the plan check passes, the turn itself is refused with the portal's own 403
    portal.routeFailure = process -> process.equals("ai-send")
        ? new String[] { "403", "{\"error\":\"scope_denied\",\"message\":\"no\"}" } : null;

    final Reply reply = call("POST", "/chat/stream", question());

    assertThat(reply.status()).isEqualTo(502);
    assertThat(reply.json().getString("code")).isEqualTo("scope_denied");
    assertThat(reply.body()).doesNotContain(FakeAiPortal.KEY);
  }

  @Test
  void aFailureAfterTheStreamStartedIsReportedInBand() throws Exception {
    portal.cutStream = true;

    final Reply reply = call("POST", "/chat/stream", question());

    assertThat(reply.status()).isEqualTo(200);
    final List<JSONObject> events = reply.events();
    assertThat(events.get(events.size() - 1).getString("type")).isEqualTo("error");
    assertThat(events.stream().anyMatch(e -> e.getString("type").equals("done"))).isFalse();
    assertThat(portal.stops).hasSize(1);
  }

  @Test
  void theProfilerAnalysisGoesThroughThePortalToo() throws Exception {
    portal.script = body -> FakeAiPortal.answer("The slow query is the scan", List.of());

    final Reply reply = call("POST", "/analyze-profiler", new JSONObject().put("profilerData",
        new JSONObject().put("queries", new JSONArray().put(new JSONObject().put("database", getDatabaseName())))));

    assertThat(reply.status()).isEqualTo(200);
    assertThat(reply.json().getString("response")).isEqualTo("The slow query is the scan");
    final JSONObject sent = portal.sends.get(0);
    assertThat(sent.getString("mode")).isEqualTo("profiler");
    assertThat(sent.getJSONObject("profiler").has("queries")).isTrue();
    assertThat(sent.getJSONObject("schemas").has(getDatabaseName())).isTrue();
  }

  @Test
  void aDisconnectedServerStillAnswersTheOldWay() throws Exception {
    Files.deleteIfExists(Path.of(getServer(0).getConfigPath(), "support.json"));

    final Reply reply = call("POST", "/chat", question());

    assertThat(reply.status()).isEqualTo(400);
    assertThat(reply.json().getString("error")).contains("not configured");
    assertThat(portal.sends).isEmpty();
  }
}
