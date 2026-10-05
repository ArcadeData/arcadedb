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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.support.SupportPortalClient;
import com.arcadedb.server.support.SupportPortalException;

import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * The AI Assistant's side of the wire contract with the ArcadeData customer portal (portal repository,
 * {@code docs/AI-ASSISTANT.md}, section 12). EVERYTHING that names a path, a process or a field of that contract is in this
 * class, so a later change of the contract is a change of this one file.
 * <p>
 * A turn is stateless: {@link #send} hands the portal the whole conversation and the results of the tools it asked for so far,
 * the answer is read from {@link #openStream} (NDJSON, one stream per {@code turnId}), and the portal keeps nothing.
 */
public class AiPortalClient {
  static final String PROCESS_PATH = "/api/v1/process-execute";
  static final String STREAM_PATH  = "/api/v1/key/chat-stream/";
  static final String SEND         = "ai-send";
  static final String STOP         = "ai-stop";
  static final String USAGE        = "ai-usage";

  public static final long CALL_TIMEOUT_MS = 30_000L;

  private final SupportPortalClient portal;

  public AiPortalClient(final SupportPortalClient portal) {
    this.portal = portal;
  }

  /** The body the platform's process endpoint takes: the parameters of the process under {@code parameters}. */
  private static String envelope(final JSONObject parameters) {
    return new JSONObject().put("parameters", parameters).toString();
  }

  /** A new id for a turn: the key of its stream. */
  public static String newTurnId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Hands one turn to the portal. {@code request} carries {@code message}, {@code database}, {@code schemaDigest} (the compact schema summary, section 13),
   * {@code history}, {@code toolResults}, {@code round}, {@code language}, {@code mode}, {@code profiler} as the contract lists
   * them; the {@code turnId} is added here.
   *
   * @throws SupportPortalException {@code ai.not_entitled}, {@code ai.allowance_exhausted}, {@code ai.busy}, {@code ai.invalid} or a
   *                                transport/key failure
   */
  public void send(final String turnId, final JSONObject request) {
    final JSONObject parameters = new JSONObject(request.toString());
    parameters.put("turnId", turnId);
    portal.runProcess(PROCESS_PATH, SEND, envelope(parameters), CALL_TIMEOUT_MS);
  }

  /** Reads the events of a turn; the caller closes the stream and wraps it with {@link BoundedHttpExchange#silenceBounded}. */
  public InputStream openStream(final String turnId, final long silenceMs) {
    return portal.openStream(STREAM_PATH + URLEncoder.encode(turnId, StandardCharsets.UTF_8), silenceMs).body();
  }

  /** Asks the portal to stop a running answer; the partial text still arrives on the stream. Never throws. */
  public boolean stop(final String turnId) {
    try {
      final JSONObject answer = new JSONObject(
          portal.runProcess(PROCESS_PATH, STOP, envelope(new JSONObject().put("turnId", turnId)), CALL_TIMEOUT_MS));
      return answer.getBoolean("stopped", false);
    } catch (final RuntimeException e) {
      return false;
    }
  }

  /** {@code {enabled, tier, turns, spent, budget, percent, resetsOn}} of the workspace; spent and budget are billed dollars this month. */
  public JSONObject usage() {
    return new JSONObject(portal.runProcess(PROCESS_PATH, USAGE, envelope(new JSONObject()), CALL_TIMEOUT_MS));
  }
}
