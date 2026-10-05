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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.support.SupportConfiguration;
import com.arcadedb.server.support.SupportPortalClient;
import com.arcadedb.server.support.SupportPortalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The stateless turns against a fake portal: no server, no real tools (a recording dispatcher stands in). */
class AiPortalChatTest {
  @TempDir
  Path dir;

  private FakeAiPortal portal;
  private AiPortalClient client;

  /** What the sink saw. */
  private final List<JSONObject> events = new ArrayList<>();
  private       int              opened;
  private       boolean          browserGone;

  private final AiPortalChat.Sink sink = new AiPortalChat.Sink() {
    @Override
    public void open() {
      opened++;
    }

    @Override
    public void event(final JSONObject event) throws IOException {
      if (browserGone)
        throw new IOException("Broken pipe");
      events.add(event);
    }

    @Override
    public void heartbeat() {
    }
  };

  /** Runs nothing: answers what it is told and remembers what it was asked. */
  private static final class RecordingTools extends ToolDispatcher {
    final List<String> calls = new ArrayList<>();

    RecordingTools() {
      super(null, null, "db");
    }

    @Override
    public String execute(final String toolName, final JSONObject args) {
      calls.add(toolName + args);
      return toolName.equals("bad") ? "{\"error\":\"nope\"}" : "{\"result\":[{\"n\":1}]}";
    }
  }

  @BeforeEach
  void start() throws IOException {
    portal = new FakeAiPortal();
    final SupportConfiguration configuration = new SupportConfiguration(dir, new ContextConfiguration());
    configuration.save(portal.url(), FakeAiPortal.CLIENT_ID, FakeAiPortal.KEY);
    client = new AiPortalClient(new SupportPortalClient(configuration.get(), "adb-123e4567-e89b-12d3-a456-426614174000"));
  }

  @AfterEach
  void stop() {
    portal.close();
  }

  private static JSONObject request() {
    return new JSONObject().put("message", "How many users?").put("database", "db").put("mode", "chat")
        .put("schemaDigest", "Database db: 1 types, 1 shown in full\nvertex User ~7 rows, 1 buckets")
        .put("history", new JSONArray().put(new JSONObject().put("role", "user").put("content", "hi")));
  }

  @Test
  void theChartsOfTheResultAreKeptValidatedAndInvalidOnesAreDropped() throws IOException {
    final JSONArray charts = new JSONArray().put(FakeAiPortal.chart("bar", "SELECT s, count(*) AS n FROM B GROUP BY s", "s", "n"))
        .put(FakeAiPortal.chart("radar", "SELECT 1", "a", "b"));
    portal.script = body -> FakeAiPortal.answerWithCharts("Here is a chart", charts);

    final AiPortalChat.Answer answer = new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink);

    assertThat(answer.charts().length()).isEqualTo(1);
    assertThat(answer.charts().getJSONObject(0).getString("type")).isEqualTo("bar");
  }

  @Test
  void aPlainAnswerStreamsTextAndEndsWithTheResult() throws IOException {
    final JSONArray commands = new JSONArray().put(new JSONObject().put("purpose", "p").put("language", "sql").put("command", "select 1"));
    portal.script = body -> FakeAiPortal.answer("Forty two", List.of(), commands);

    final AiPortalChat.Answer answer = new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink);

    assertThat(answer.response()).isEqualTo("Forty two");
    assertThat(answer.commands().length()).isEqualTo(1);
    assertThat(answer.usage().getDouble("budget")).isEqualTo(20.0);
    assertThat(answer.usage().getDouble("spent")).isEqualTo(3.41);
    assertThat(answer.usage().getInt("percent")).isEqualTo(17);
    assertThat(events).extracting(e -> e.getString("type")).containsExactly("delta");
    assertThat(events.get(0).getString("text")).isEqualTo("Forty two");
    assertThat(opened).isEqualTo(1);

    assertThat(portal.sends).hasSize(1);
    final JSONObject sent = portal.sends.get(0);
    assertThat(sent.getString("turnId")).isNotEmpty();
    assertThat(sent.getInt("round")).isZero();
    assertThat(sent.getString("message")).isEqualTo("How many users?");
    assertThat(sent.getJSONArray("history").length()).isEqualTo(1);
    assertThat(sent.has("toolResults")).isFalse();
    assertThat(sent.getString("schemaDigest")).startsWith("Database db: ");
  }

  @Test
  void toolsRunHereAndTheNextTurnCarriesTheirResults() throws IOException {
    portal.script = body -> body.getInt("round") == 0
        ? FakeAiPortal.answer("", List.of(FakeAiPortal.toolCall("t1", "get_schema", new JSONObject().put("database", "db")),
        FakeAiPortal.toolCall("t2", "bad", new JSONObject())))
        : FakeAiPortal.answer("There are 7 types", List.of());
    final RecordingTools tools = new RecordingTools();

    final AiPortalChat.Answer answer = new AiPortalChat(client, 5_000).run(request(), tools, sink);

    assertThat(answer.response()).isEqualTo("There are 7 types");
    assertThat(tools.calls).hasSize(2);
    assertThat(events).extracting(e -> e.getString("type")).containsSubsequence("tool_start", "tool_end", "tool_start", "tool_end");
    final List<JSONObject> ends = events.stream().filter(e -> e.getString("type").equals("tool_end")).toList();
    assertThat(ends.get(0).getString("error", null)).isNull();
    assertThat(ends.get(1).getString("error")).isEqualTo("nope");

    assertThat(portal.sends).hasSize(2);
    final JSONObject second = portal.sends.get(1);
    assertThat(second.getInt("round")).isEqualTo(1);
    assertThat(second.getString("turnId")).isNotEqualTo(portal.sends.get(0).getString("turnId"));
    assertThat(second.getString("message")).isEqualTo("How many users?");
    // The schema summary goes with EVERY round, not only the first
    assertThat(second.getString("schemaDigest")).isEqualTo(portal.sends.get(0).getString("schemaDigest")).contains("vertex User ~7 rows");
    final JSONArray results = second.getJSONArray("toolResults");
    assertThat(results.length()).isEqualTo(2);
    assertThat(results.getJSONObject(0).getString("name")).isEqualTo("get_schema");
    assertThat(results.getJSONObject(0).getString("result")).contains("\"n\":1");
    assertThat(results.getJSONObject(1).getString("result")).contains("nope");
    assertThat(opened).isEqualTo(1);
  }

  @Test
  void aModelThatNeverStopsAskingForToolsIsStopped() throws IOException {
    portal.script = body -> FakeAiPortal.answer("again", List.of(FakeAiPortal.toolCall("t", "get_schema", new JSONObject())));

    final AiPortalChat.Answer answer = new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink);

    assertThat(portal.sends).hasSize(AiPortalChat.MAX_ROUNDS + 1);
    assertThat(portal.sends.get(AiPortalChat.MAX_ROUNDS).getInt("round")).isEqualTo(AiPortalChat.MAX_ROUNDS);
    assertThat(answer.response()).isEqualTo("again");
  }

  @Test
  void moreToolCallsThanThePortalTakesEndTheAnswerInsteadOfFailingIt() throws IOException {
    // eight calls a round: the fourth round would carry 32 + 8 results, more than the portal accepts
    final List<JSONObject> eight = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      eight.add(FakeAiPortal.toolCall("t" + i, "get_schema", new JSONObject()));
    portal.script = body -> FakeAiPortal.answer("so far", eight);

    final AiPortalChat.Answer answer = new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink);

    assertThat(answer.response()).isEqualTo("so far");
    assertThat(portal.sends.get(portal.sends.size() - 1).has("toolResults")).isTrue();
    for (final JSONObject sent : portal.sends)
      assertThat(sent.getJSONArray("toolResults", new JSONArray()).length())
          .isLessThanOrEqualTo(AiPortalChat.MAX_TOOL_RESULT_ENTRIES);
  }

  @Test
  void withoutAToolDispatcherToolCallsAreNotRun() throws IOException {
    portal.script = body -> FakeAiPortal.answer("review", List.of(FakeAiPortal.toolCall("t", "get_schema", new JSONObject())));

    final AiPortalChat.Answer answer = new AiPortalChat(client, 5_000).run(request(), null, sink);

    assertThat(answer.response()).isEqualTo("review");
    assertThat(portal.sends).hasSize(1);
    assertThat(portal.sends.get(0).getBoolean("tools", true)).isFalse();
  }

  @Test
  void aRefusedTurnIsReportedWithItsCodeBeforeAnythingIsOpened() {
    portal.refusal = body -> "ai.allowance_exhausted: the assistant plan's AI allowance for this month ($20.00) is used up; it starts over on 2026-11-01";

    assertThatThrownBy(() -> new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink))
        .isInstanceOfSatisfying(SupportPortalException.class, e -> {
          assertThat(e.getCode()).isEqualTo("ai.allowance_exhausted");
          assertThat(e.getMessage()).contains("allowance");
        });
    assertThat(opened).isZero();
    assertThat(portal.streams).isEmpty();
  }

  @Test
  void aRefusalInTheStreamKeepsItsCode() {
    portal.script = body -> List.of(new JSONObject().put("type", "event").put("name", "refused").put("data",
        new JSONObject().put("code", "ai.not_entitled")), new JSONObject().put("type", "error").put("text", "Plan without AI"));

    assertThatThrownBy(() -> new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink))
        .isInstanceOfSatisfying(SupportPortalException.class, e -> assertThat(e.getCode()).isEqualTo("ai.not_entitled"));
  }

  @Test
  void aStreamCutShortIsAnErrorAndTheTurnIsStopped() {
    portal.cutStream = true;

    assertThatThrownBy(() -> new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink))
        .isInstanceOf(EOFException.class);
    assertThat(portal.stops).containsExactly(portal.sends.get(0).getString("turnId"));
  }

  @Test
  void aBrowserThatLeavesStopsTheAnswerBeingWritten() {
    browserGone = true;

    assertThatThrownBy(() -> new AiPortalChat(client, 5_000).run(request(), new RecordingTools(), sink))
        .isInstanceOf(IOException.class).hasMessageContaining("Broken pipe");
    assertThat(portal.stops).containsExactly(portal.sends.get(0).getString("turnId"));
  }

  @Test
  void aWrongKeyIsRefusedAndNeverEchoed() throws IOException {
    final SupportConfiguration other = new SupportConfiguration(dir.resolve("x"), new ContextConfiguration());
    java.nio.file.Files.createDirectories(dir.resolve("x"));
    other.save(portal.url(), FakeAiPortal.CLIENT_ID, "wsk_WRONGKEY0123456789abcdefghijklmnopqrstuv");
    final AiPortalClient wrong = new AiPortalClient(new SupportPortalClient(other.get(), null));

    assertThatThrownBy(() -> new AiPortalChat(wrong, 5_000).run(request(), new RecordingTools(), sink))
        .isInstanceOfSatisfying(SupportPortalException.class, e -> {
          assertThat(e.getCode()).isEqualTo("invalid_key");
          assertThat(e.getMessage()).doesNotContain("wsk_");
        });
  }

  @Test
  void usageAndStopSpeakTheContract() {
    assertThat(client.usage().getBoolean("enabled")).isTrue();
    assertThat(client.usage().getString("tier")).isEqualTo("assistant");
    assertThat(client.stop("turn-9")).isTrue();
    assertThat(portal.stops).containsExactly("turn-9");
  }
}
