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

import com.arcadedb.network.BoundedHttpExchange;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.support.SupportPortalException;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * One answer of the AI Assistant through the portal: the stateless turns (docs/AI-ASSISTANT.md of the portal, section 12).
 * <p>
 * Each round sends the conversation and the results of the tools asked for so far, reads the stream of that turn, and either
 * ends with the answer or, when the model asked for tools, runs them HERE through the {@link ToolDispatcher} (read-only, as the
 * user) and sends the next round. Nothing waits on the portal while a tool runs. Rounds are bounded.
 */
public class AiPortalChat {
  /** The most rounds of one answer: the portal refuses more (contract), this stops the loop first. */
  public static final int MAX_ROUNDS = 8;
  /** The most of one tool result handed back to the model. */
  static final int MAX_TOOL_RESULT_CHARS = 50_000;
  /** The most of all the tool results of one answer: the portal takes a request of 1 MB, and each round sends them all again. */
  static final int MAX_TOOL_RESULTS_TOTAL = 400_000;
  /** What is kept of a result once the budget is spent. */
  static final int SPENT_BUDGET_RESULT_CHARS = 2_000;
  /** The most tool results the portal takes in one turn (four a round, over {@link #MAX_ROUNDS}). */
  static final int MAX_TOOL_RESULT_ENTRIES = MAX_ROUNDS * 4;

  /** What the caller does with the events of the answer. */
  public interface Sink {
    /** The first round was accepted: the answer is on its way. Called once, before any event. */
    void open() throws IOException;

    /** A browser-facing event ({@code delta}, {@code tool_start}, {@code tool_end}). */
    void event(JSONObject event) throws IOException;

    /** The portal is still working (a heartbeat). */
    void heartbeat() throws IOException;
  }

  /** The final answer. */
  public record Answer(String response, JSONArray commands, JSONArray charts, JSONObject usage) {
  }

  private final AiPortalClient client;
  private final long           silenceMs;

  public AiPortalChat(final AiPortalClient client, final long silenceMs) {
    this.client = client;
    this.silenceMs = silenceMs;
  }

  /**
   * @param request    {@code message}, {@code history}, {@code database}, ... as {@link AiPortalClient#send} takes them
   * @param dispatcher runs the tools the model asks for, or {@code null} for an answer without tools
   * @throws SupportPortalException when the portal refuses or its answer cannot be read as a whole
   * @throws IOException            when the connection to the portal or to the browser fails
   */
  public Answer run(final JSONObject request, final ToolDispatcher dispatcher, final Sink sink) throws IOException {
    final JSONArray toolResults = new JSONArray();
    final JSONObject base = new JSONObject(request.toString());
    boolean opened = false;
    int resultChars = 0;
    Answer last = null;

    for (int round = 0; round <= MAX_ROUNDS; round++) {
      final String turnId = AiPortalClient.newTurnId();
      final JSONObject turn = new JSONObject(base.toString());
      turn.put("round", round);
      if (toolResults.length() > 0)
        turn.put("toolResults", toolResults);
      if (dispatcher == null)
        turn.put("tools", false);

      client.send(turnId, turn);
      if (!opened) {
        sink.open();
        opened = true;
      }

      final JSONObject result;
      try {
        result = readTurn(turnId, sink);
      } catch (final IOException | RuntimeException e) {
        // The browser went away, or the stream broke: the portal may still be writing for nobody
        client.stop(turnId);
        throw e;
      }

      final JSONArray toolCalls = result.getJSONArray("toolCalls", new JSONArray());
      last = new Answer(result.getString("response", ""), result.getJSONArray("commands", new JSONArray()),
          AiCharts.clean(result.getJSONArray("charts", null)), result.getJSONObject("usage", new JSONObject()));
      // Out of rounds, or the next turn would carry more results than the portal takes: the answer so far is the answer
      if (toolCalls.length() == 0 || dispatcher == null || round == MAX_ROUNDS
          || toolResults.length() + toolCalls.length() > MAX_TOOL_RESULT_ENTRIES)
        return last;

      for (int i = 0; i < toolCalls.length(); i++) {
        final JSONObject call = toolCalls.getJSONObject(i);
        final String name = call.getString("name", "");
        // Arguments that are not a JSON object, and a tool that throws, are error results the model sees next round (#8721)
        final JSONObject callArgs = ToolDispatcher.arguments(call);
        final JSONObject args = callArgs != null ? callArgs : new JSONObject();
        sink.event(new JSONObject().put("type", "tool_start").put("tool", name).put("args", args));

        String output = dispatcher.executeSafely(name, callArgs);
        final JSONObject end = new JSONObject().put("type", "tool_end").put("tool", name).put("args", args);
        try {
          final String error = new JSONObject(output).getString("error", null);
          if (error != null && !error.isEmpty())
            end.put("error", error);
        } catch (final RuntimeException ignored) {
          // not a JSON object: a result, not an error
        }
        sink.event(end);

        if (output.length() > MAX_TOOL_RESULT_CHARS)
          output = output.substring(0, MAX_TOOL_RESULT_CHARS);
        if (resultChars + output.length() > MAX_TOOL_RESULTS_TOTAL && output.length() > SPENT_BUDGET_RESULT_CHARS)
          output = output.substring(0, SPENT_BUDGET_RESULT_CHARS);
        resultChars += output.length();
        toolResults.put(new JSONObject().put("name", name).put("arguments", args).put("result", output).put("round", round));
      }
    }
    return last;
  }

  /** Reads one turn to its end. @return the {@code result} event of the portal: {response, commands, charts, toolCalls, usage} */
  private JSONObject readTurn(final String turnId, final Sink sink) throws IOException {
    JSONObject result = null;
    String refusedCode = null;

    try (final InputStream body = BoundedHttpExchange.silenceBounded(client.openStream(turnId, silenceMs), silenceMs);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank())
          continue;
        final JSONObject envelope;
        try {
          envelope = new JSONObject(line);
        } catch (final RuntimeException e) {
          continue; // not an envelope: nothing to act on
        }
        switch (envelope.getString("type", "")) {
        case "heartbeat" -> sink.heartbeat();
        case "delta" -> sink.event(new JSONObject().put("type", "delta").put("text", envelope.getString("text", "")));
        case "reset" -> sink.event(new JSONObject().put("type", "reset"));
        case "event" -> {
          final String name = envelope.getString("name", "");
          if ("result".equals(name))
            result = envelope.getJSONObject("data", new JSONObject());
          else if ("refused".equals(name))
            refusedCode = envelope.getJSONObject("data", new JSONObject()).getString("code", null);
        }
        case "error" -> throw new SupportPortalException(refusedCode != null ? refusedCode : "portal_error", 0,
            envelope.getString("text", "The AI Assistant could not answer"), 0);
        case "none" -> throw new SupportPortalException("portal_error", 0, "The AI Assistant has no answer for this request", 0);
        case "done" -> {
          if (result == null)
            throw new SupportPortalException("portal_error", 0, "The AI Assistant finished without an answer", 0);
          return result;
        }
        default -> {
          // an envelope this server does not know: ignored (forward compatible)
        }
        }
      }
    }
    throw new EOFException("the AI service closed the stream without completing the answer");
  }
}
