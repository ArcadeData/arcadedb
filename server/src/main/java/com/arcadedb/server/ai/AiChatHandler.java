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

import com.arcadedb.Constants;
import com.arcadedb.Profiler;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.BoundedHttpExchange;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.info.SchemaInfo;
import com.arcadedb.server.info.ServerInfo;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.support.SupportPortalClient;
import com.arcadedb.server.support.SupportPortalException;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;

/**
 * Main AI chat endpoint, split across two operations so each has exactly one response content
 * type (issue #6558: OpenAPI's content map selects on the Accept header, not on a request-body
 * field, so a single operation that streamed or not depending on a body field broke generated
 * clients that bound {@code application/json}).
 *
 * <p><b>{@code POST /api/v1/ai/chat/stream}</b> uses a client-orchestrated streaming protocol so
 * the gateway never has to open an inbound HTTP connection into the user's network. The gateway
 * emits SSE events ({@code session}, {@code tool_call}, {@code done}); this handler executes each
 * tool locally via {@link ToolDispatcher} and POSTs the result to
 * {@code /api/chat/tool_result/:sessionId} so the LLM loop resumes. Studio sees the usual
 * {@code tool_start}/{@code tool_end} events, synthesized locally.
 *
 * <p><b>{@code POST /api/v1/ai/chat}</b> (review-first) embeds the schema directly in the prompt
 * and uses a single non-streaming request to the gateway (no tool calls), always answering with
 * a JSON body.
 */
public class AiChatHandler extends AbstractServerHttpHandler {
  // Static so all server instances in the JVM share one client. Each instance spawns
  // a SelectorManager NIO thread that survives until the client is GC'd; per-instance
  // clients leaked dozens of threads per server start under the integration-test suite.
  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  /**
   * How long the gateway has to answer a buffered chat request, body included. Enforced by
   * {@link BoundedHttpExchange#send} rather than by the request timeout alone, which on JDK 21-25 stops once the
   * response headers arrive (issue #8473).
   * Mutable and package-private only so a test can shorten it; JVM-wide, so such a test restores it afterwards and
   * relies on the module's tests not running in parallel.
   */
  static volatile long gatewayTimeoutMs = 120_000L;

  /** The same bound on the POST that hands a tool result back to the gateway; mutable for the same reason. */
  static volatile long toolResultTimeoutMs = 15_000L;

  /**
   * The streamed chat: how long the gateway may take to send its headers, and then how long it may stay silent while
   * this server waits on the next event. A bound on silence, not on length: a conversation that keeps producing events
   * runs as long as it needs to. The request carries no timeout of its own, since on JDK 26+ that would cap the whole
   * stream (issue #8473).
   * Mutable and package-private only so a test can shorten it; JVM-wide, so such a test restores it afterwards and
   * relies on the module's tests not running in parallel.
   */
  static volatile long streamSilenceMs = 5 * 60_000L;

  private static final byte[] HEARTBEAT_FRAME = ": keepalive\n\n".getBytes(StandardCharsets.UTF_8);

  private final ArcadeDBServer server;
  private final AiConfiguration config;
  private final ChatStorage     chatStorage;
  private final boolean         streaming;
  private final AiPortal        portal;

  /**
   * @param streaming {@code true} to back {@code POST /api/v1/ai/chat/stream} (always SSE),
   *                  {@code false} to back {@code POST /api/v1/ai/chat} (always JSON)
   */
  public AiChatHandler(final HttpServer httpServer, final ArcadeDBServer server, final AiConfiguration config,
      final ChatStorage chatStorage, final boolean streaming) {
    this(httpServer, server, config, chatStorage, streaming, new AiPortal(server, config));
  }

  /** @param portal where the answers come from when the server is connected to the customer portal */
  public AiChatHandler(final HttpServer httpServer, final ArcadeDBServer server, final AiConfiguration config,
      final ChatStorage chatStorage, final boolean streaming, final AiPortal portal) {
    super(httpServer);
    this.server = server;
    this.config = config;
    this.chatStorage = chatStorage;
    this.streaming = streaming;
    this.portal = portal;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final AiPortal.Source source = portal.source();
    if (source == AiPortal.Source.NONE)
      return new ExecutionResponse(400,
          new JSONObject().put("error", "AI assistant is not configured. Please configure config/ai.json.").toString());

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    // Default to v1 when the client doesn't send a version (oldest Studio bundles).
    // Once we publish v2 we keep accepting unversioned requests as v1 here and only
    // bump the default when v1 is dropped from SUPPORTED_VERSIONS.
    final int protocolVersion = payload.getInt("protocolVersion", 1);
    if (!AiProtocol.isSupported(protocolVersion))
      return new ExecutionResponse(400, new JSONObject()
          .put("error", "Unsupported AI protocol version: " + protocolVersion
              + ". Server supports: " + AiProtocol.SUPPORTED_VERSIONS + ". Please update Studio.")
          .put("code", "protocol_unsupported")
          .put("currentProtocolVersion", AiProtocol.CURRENT_VERSION)
          .put("supportedProtocolVersions", AiProtocol.supportedVersionsArray())
          .toString());

    final String database = payload.getString("database", null);
    final String message = payload.getString("message", null);
    final String chatId = payload.getString("chatId", null);

    if (database == null || database.isEmpty())
      return new ExecutionResponse(400, new JSONObject().put("error", "Database name is required").toString());
    if (message == null || message.isEmpty())
      return new ExecutionResponse(400, new JSONObject().put("error", "Message is required").toString());

    // Verify user has access to the database
    if (!user.canAccessToDatabase(database))
      return new ExecutionResponse(403,
          new JSONObject().put("error", "User '" + user.getName() + "' is not authorized to access database '" + database + "'")
              .toString());

    try {
      // Load or create chat
      final String username = user.getName();
      JSONObject chat;
      if (chatId != null && !chatId.isEmpty()) {
        chat = chatStorage.getChat(username, chatId);
        if (chat == null)
          return new ExecutionResponse(404, new JSONObject().put("error", "Chat not found").toString());
      } else {
        chat = ChatStorage.createNewChat(database, ChatStorage.generateTitle(message));
      }

      // Add user message to chat
      final JSONArray messages = chat.getJSONArray("messages", new JSONArray());
      final JSONObject userMsg = new JSONObject();
      userMsg.put("role", "user");
      userMsg.put("content", message);
      userMsg.put("timestamp", Instant.now().toString());
      messages.put(userMsg);

      // Build history for gateway (last 20 messages max to keep context manageable)
      final JSONArray history = new JSONArray();
      final int start = Math.max(0, messages.length() - 21); // -21 because we already added current msg
      for (int i = start; i < messages.length() - 1; i++)
        history.put(messages.getJSONObject(i));

      if (source == AiPortal.Source.PORTAL)
        return handlePortalRequest(exchange, user, payload, database, message, chat, messages, history, username);

      // Forward to gateway
      final JSONObject gatewayRequest = new JSONObject();
      gatewayRequest.put("message", message);
      gatewayRequest.put("history", history);
      gatewayRequest.put("database", database);
      gatewayRequest.put("hardwareId", AiActivateHandler.getHardwareId());
      gatewayRequest.put("serverVersion", Constants.getVersion());

      if (streaming) {
        // Client-orchestrated streaming: we deliberately do NOT send arcadedb.url to the
        // gateway. The gateway emits tool_call SSE events; we execute each tool locally
        // via ToolDispatcher and POST the result back to /api/chat/tool_result/:sessionId.
        // No inbound connectivity from the gateway to this server is required.
        gatewayRequest.put("schema", new JSONObject()); // gateway still validates presence
        gatewayRequest.put("stream", true);

        final ToolDispatcher dispatcher = new ToolDispatcher(server, user, database);
        return handleStreamingRequest(exchange, gatewayRequest, chat, messages, username, dispatcher);
      } else {
        // Review-first path: embed schema/serverInfo in prompt
        final JSONObject schema = SchemaInfo.forUser(server, user, database);
        final JSONObject serverInfo = ServerInfo.toJSON(server, user::canAccessToDatabase, false);
        serverInfo.put("metrics", Profiler.INSTANCE.toJSON());
        gatewayRequest.put("schema", schema);
        gatewayRequest.put("serverInfo", serverInfo);
      }

      final JSONObject gatewayResponse = callGateway(gatewayRequest);
      return buildResponse(gatewayResponse, chat, messages, username);

    } catch (final SecurityException e) {
      throw e; // Let AbstractServerHttpHandler handle security exceptions
    } catch (final AiTokenException e) {
      return new ExecutionResponse(e.getHttpStatus(), e.getJsonResponse());
    } catch (final SupportPortalException e) {
      return portalError(e);
    } catch (final ConnectException | HttpConnectTimeoutException e) {
      LogManager.instance().log(this, Level.WARNING, "AI gateway unreachable: %s", e.getMessage());
      return new ExecutionResponse(503, new JSONObject()//
          .put("error", "AI service is temporarily unreachable. Please try again later.")//
          .put("code", "gateway_unreachable").toString());
    } catch (final HttpTimeoutException e) {
      LogManager.instance().log(this, Level.WARNING, "AI gateway timeout: %s", e.getMessage());
      return new ExecutionResponse(504, new JSONObject()//
          .put("error", "AI service took too long to respond. Please try again later.")//
          .put("code", "gateway_timeout").toString());
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "AI gateway I/O error: %s", e.getMessage());
      return new ExecutionResponse(503, new JSONObject()//
          .put("error", "AI service is temporarily unavailable. Please try again later.")//
          .put("code", "gateway_unreachable").toString());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error processing AI chat request: %s", e.getMessage());
      return new ExecutionResponse(500, new JSONObject()//
          .put("error", "An unexpected error occurred. Please try again later.").toString());
    }
  }

  /**
   * Handles SSE streaming in the client-orchestrated tool-use flow.
   *
   * <p>The gateway emits SSE events on the open response stream:
   * <ul>
   *   <li>{@code {type:"session", sessionId}} once at the start - we remember it so we
   *   know where to address tool_result POSTs.</li>
   *   <li>{@code {type:"tool_call", id, name, arguments}} when the LLM wants a tool -
   *   we synthesize a {@code tool_start} for Studio, execute locally via
   *   {@link ToolDispatcher}, synthesize a {@code tool_end} for Studio, and POST
   *   {@code {id, result}} back to {@code /api/chat/tool_result/:sessionId}, which
   *   resumes the gateway's LLM loop.</li>
   *   <li>{@code {type:"done", ...}} - final event; we enrich with chatId, save chat
   *   history, and forward.</li>
   * </ul>
   * Returns {@code null} to signal that the response was already sent via the exchange.
   */
  private ExecutionResponse handleStreamingRequest(final HttpServerExchange exchange, final JSONObject gatewayRequest,
      final JSONObject chat, final JSONArray messages, final String username, final ToolDispatcher dispatcher) throws Exception {

    final HttpRequest request = HttpRequest.newBuilder()//
        .uri(URI.create(config.getGatewayUrl() + "/api/chat"))//
        .header("Content-Type", "application/json")//
        .header("Authorization", "Bearer " + config.getSubscriptionToken())//
        .POST(HttpRequest.BodyPublishers.ofString(gatewayRequest.toString()))//
        .build();

    final HttpResponse<InputStream> response = BoundedHttpExchange.send(HTTP_CLIENT, request,
        HttpResponse.BodyHandlers.ofInputStream(), streamSilenceMs);
    final InputStream responseBody = BoundedHttpExchange.silenceBounded(response.body(), streamSilenceMs);

    if (response.statusCode() == 401 || response.statusCode() == 403) {
      try (InputStream body = responseBody) {
        final String bodyStr = new String(body.readAllBytes(), StandardCharsets.UTF_8);
        final JSONObject errBody = new JSONObject(bodyStr);
        final String code = errBody.getString("code", "token_invalid");
        final String errorMsg = errBody.getString("error", "Invalid or expired subscription token");
        final JSONObject errorResponse = new JSONObject();
        errorResponse.put("error", errorMsg);
        errorResponse.put("code", code);
        throw new AiTokenException(response.statusCode(), errorResponse.toString());
      }
    }

    if (response.statusCode() != 200) {
      try (InputStream body = responseBody) {
        final String bodyStr = new String(body.readAllBytes(), StandardCharsets.UTF_8);
        throw new RuntimeException("Gateway returned status " + response.statusCode() + ": " + bodyStr);
      }
    }

    // Set SSE headers and start streaming
    exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
    exchange.getResponseHeaders().put(new HttpString("Cache-Control"), "no-cache");
    exchange.getResponseHeaders().put(new HttpString("X-Accel-Buffering"), "no");
    exchange.setStatusCode(200);

    // Every write bounded (issue #7806): a Studio tab that stops reading - closed mid-answer behind a proxy that keeps
    // the connection open, a suspended laptop - would otherwise hold this worker thread blocked in write().
    final OutputStream output = streamedResponseOutput(exchange, () -> "the streamed AI chat answer");
    String gatewaySessionId = null;
    boolean finished = false;

    try (InputStream body = responseBody;
         BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(":")) {
          // An SSE comment: a heartbeat the gateway sends while the model is working. Relayed, so the client's hop
          // carries bytes too - a proxy or CDN in front of this server drops a connection idle for ~100s, and a
          // long answer can be silent for longer (issue #8642). The client ignores comment lines. A fixed frame, not the
          // gateway's line: nothing it puts in a comment is forwarded. Heartbeats also reset streamSilenceMs, which
          // bounds silence rather than total length (see ArcadeData/arcadedb-ai-gateway#161).
          output.write(HEARTBEAT_FRAME);
          output.flush();
          continue;
        }
        if (!line.startsWith("data: "))
          continue;

        final String data = line.substring(6);

        JSONObject event = null;
        String type = "";
        try {
          event = new JSONObject(data);
          type = event.getString("type", "");
        } catch (final Exception ignored) {
          // Not valid JSON: forward verbatim, can't act on it locally.
          output.write(("data: " + data + "\n\n").getBytes(StandardCharsets.UTF_8));
          output.flush();
          continue;
        }

        switch (type) {
          case "session" -> {
            gatewaySessionId = event.getString("sessionId", null);
            // Internal control event - do not forward to Studio.
          }
          case "tool_call" -> {
            final String toolId = event.getString("id", null);
            final String toolName = event.getString("name", "");
            final JSONObject toolArgs = event.getJSONObject("arguments", new JSONObject());

            // Synthesize tool_start for Studio's live UI (keeps Studio's existing renderer happy).
            forwardEvent(output, new JSONObject()
                .put("type", "tool_start")
                .put("tool", toolName)
                .put("args", toolArgs));

            // Execute locally. Returns JSON string (success or {"error":"..."}).
            final String toolResult = dispatcher.execute(toolName, toolArgs);

            // Synthesize tool_end. If the result encodes an error, propagate it.
            final JSONObject toolEnd = new JSONObject()
                .put("type", "tool_end")
                .put("tool", toolName)
                .put("args", toolArgs);
            String toolError = null;
            try {
              final JSONObject parsed = new JSONObject(toolResult);
              toolError = parsed.getString("error", null);
            } catch (final Exception ignored) { /* result is not a JSON object */ }
            if (toolError != null && !toolError.isEmpty()) {
              toolEnd.put("error", toolError);
              LogManager.instance().log(this, Level.WARNING,
                  "AI tool '%s' failed locally: %s", toolName, toolError);
            }
            forwardEvent(output, toolEnd);

            // POST the result back to the gateway so the paused LLM loop resumes.
            if (gatewaySessionId == null) {
              LogManager.instance().log(this, Level.WARNING,
                  "AI gateway sent tool_call before session event; cannot deliver result");
              break;
            }
            postToolResult(gatewaySessionId, toolId, toolResult);
          }
          case "done" -> {
            // Inject chatId before forwarding the done event
            event.put("chatId", chat.getString("id"));

            // Persist chat history BEFORE forwarding/closing the stream: once the client sees the
            // stream end it may immediately call GET /chats, and that read must already see this
            // chat. Saving after close() (the previous approach) raced the client's own read of the
            // saved chat against the server finishing the write.
            final JSONObject assistantMsg = new JSONObject();
            assistantMsg.put("role", "assistant");
            assistantMsg.put("content", event.getString("response", ""));
            assistantMsg.put("timestamp", Instant.now().toString());

            final JSONArray commands = event.getJSONArray("commands", null);
            if (commands != null && commands.length() > 0)
              assistantMsg.put("commands", commands);

            messages.put(assistantMsg);
            chat.put("messages", messages);
            chat.put("updated", Instant.now().toString());
            try {
              chatStorage.saveChat(username, chat);
            } catch (final RuntimeException e) {
              LogManager.instance().log(this, Level.WARNING,
                  "Failed to persist chat history after streaming response (chatId=%s): %s",
                  chat.getString("id", null), e.getMessage());
            }

            forwardEvent(output, event);
            finished = true;
          }
          default -> {
            // Forward any other event types unchanged (forward-compat). An 'error' from the gateway is terminal too.
            forwardEvent(output, event);
            if ("error".equals(type))
              finished = true;
          }
        }
        // Nothing follows 'done': a drop or silence after it must not add an 'error' to a delivered answer
        if (finished)
          break;
      }
      // A stream the gateway closed cleanly but without 'done' is cut short just the same
      if (!finished)
        endStreamWithError(output, new EOFException("the AI service closed the stream without completing the answer"),
            chat.getString("id", null));
    } catch (final Exception e) {
      // The 200 and every event relayed so far are already on the wire, so this can no longer be answered with a
      // status code: returning a 503/504 from here made the caller set one on a started response, which Undertow
      // refuses with "UT000002: The response has already been started" - and the client saw a stream that simply
      // stopped (issue #8642). Same rule as PostServerCommandHandler's progress stream: report it in band.
      endStreamWithError(output, e, chat.getString("id", null));
    } finally {
      try { output.close(); } catch (final Exception ignored) {}
    }

    return null; // response already sent
  }

  /**
   * The answer through the customer portal (stateless turns, see {@link AiPortalChat}). The first round is sent before anything
   * is written, so a refusal (plan without the AI Assistant, allowance used) is an ordinary HTTP answer; what fails after the
   * stream has started is reported in band, as for the gateway.
   */
  private ExecutionResponse handlePortalRequest(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload, final String database, final String message, final JSONObject chat, final JSONArray messages,
      final JSONArray history, final String username) throws Exception {
    final AiPortalClient client = portal.client();
    if (client == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "AI assistant is not configured.").toString());

    final JSONObject request = new JSONObject().put("message", message).put("database", database)
        .put("history", portalHistory(history)).put("mode", "chat");
    // The compact schema summary goes with EVERY round (most questions need it; the model asks get_type for the rest). Built once per
    // answer here, reused across questions for a short while. A failure to build it must not stop the assistant from answering.
    try {
      request.put("schemaDigest", AiSchemaDigest.forUser(server, user, database));
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING, "AI Assistant: could not build the schema summary of '%s': %s", database,
          e.getMessage());
    }

    // Auto: the tools run here, as the user. Review first: the model may read the schema and the server, but a query is
    // refused with the sentence that makes it hand the command back to the user to review and run (the portal's tools are
    // the same in both modes, so the difference is made here, where the tools run)
    final ToolDispatcher dispatcher = streaming ? new ToolDispatcher(server, user, database)
        : new ToolDispatcher(server, user, database) {
          @Override
          public String execute(final String toolName, final JSONObject args) {
            if (toolName.equals("query_database"))
              return new JSONObject().put("error", "Review-first mode: queries are not run for the user. Return the command as a "
                  + "fenced code block for the user to review and run.").toString();
            return super.execute(toolName, args);
          }
        };

    final AiPortalChat turns = new AiPortalChat(client, streamSilenceMs);
    if (!streaming) {
      final AiPortalChat.Answer answer = turns.run(request, dispatcher, new AiPortalChat.Sink() {
        @Override
        public void open() {
        }

        @Override
        public void event(final JSONObject event) {
        }

        @Override
        public void heartbeat() {
        }
      });
      final JSONObject gatewayLike = new JSONObject().put("response", answer.response());
      if (answer.commands().length() > 0)
        gatewayLike.put("commands", answer.commands());
      return buildResponse(gatewayLike, chat, messages, username);
    }

    final OutputStream[] output = new OutputStream[1];
    final String chatId = chat.getString("id", null);
    try {
      final AiPortalChat.Answer answer = turns.run(request, dispatcher, new AiPortalChat.Sink() {
        @Override
        public void open() {
          exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
          exchange.getResponseHeaders().put(new HttpString("Cache-Control"), "no-cache");
          exchange.getResponseHeaders().put(new HttpString("X-Accel-Buffering"), "no");
          exchange.setStatusCode(200);
          output[0] = streamedResponseOutput(exchange, () -> "the streamed AI chat answer");
        }

        @Override
        public void event(final JSONObject event) throws IOException {
          forwardEvent(output[0], event);
        }

        @Override
        public void heartbeat() throws IOException {
          output[0].write(HEARTBEAT_FRAME);
          output[0].flush();
        }
      });

      final JSONObject done = new JSONObject().put("type", "done").put("response", answer.response()).put("chatId", chatId);
      if (answer.commands().length() > 0)
        done.put("commands", answer.commands());
      if (answer.usage().length() > 0)
        done.put("usage", answer.usage());

      // Saved BEFORE the stream ends: the client may read the chat list the moment it sees 'done'
      final JSONObject assistantMsg = new JSONObject().put("role", "assistant").put("content", answer.response())
          .put("timestamp", Instant.now().toString());
      if (answer.commands().length() > 0)
        assistantMsg.put("commands", answer.commands());
      messages.put(assistantMsg);
      chat.put("messages", messages);
      chat.put("updated", Instant.now().toString());
      try {
        chatStorage.saveChat(username, chat);
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to persist chat history after the AI answer (chatId=%s): %s", chatId,
            e.getMessage());
      }
      forwardEvent(output[0], done);
    } catch (final SupportPortalException e) {
      if (output[0] == null)
        return portalError(e);
      endPortalStream(output[0], e, chatId);
    } catch (final Exception e) {
      if (output[0] == null)
        throw e;
      endStreamWithError(output[0], e, chatId);
    } finally {
      if (output[0] != null)
        try {
          output[0].close();
        } catch (final Exception ignored) {
        }
    }
    return null; // response already sent
  }

  /** The conversation as the portal takes it: role and content only, never a timestamp or a command list. */
  private static JSONArray portalHistory(final JSONArray history) {
    final JSONArray out = new JSONArray();
    for (int i = 0; i < history.length(); i++) {
      final JSONObject m = history.getJSONObject(i);
      final String role = m.getString("role", "");
      if (role.equals("user") || role.equals("assistant"))
        out.put(new JSONObject().put("role", role).put("content", m.getString("content", "")));
    }
    return out;
  }

  /**
   * A refusal or failure of the portal as an HTTP answer. Never 401 or 403, whatever the portal said: those are reserved for the
   * user's own session and would log Studio out (the rule of {@link AiTokenException}); {@code code} tells Studio what happened.
   */
  static ExecutionResponse portalError(final SupportPortalException e) {
    final int status = switch (e.getCode()) {
      case SupportPortalClient.AI_NOT_ENTITLED -> 402;
      case SupportPortalClient.AI_ALLOWANCE_EXHAUSTED, SupportPortalClient.AI_BUSY, "rate_limited" -> 429;
      case "portal_unreachable" -> 503;
      case SupportPortalClient.AI_INVALID, "bad_request", "too_large" -> 400;
      default -> 502;
    };
    final JSONObject json = new JSONObject().put("error", e.getMessage()).put("code", e.getCode());
    if (e.getCode().startsWith("ai."))
      json.put("upgrade", true);
    return new ExecutionResponse(status, json.toString());
  }

  private void endPortalStream(final OutputStream output, final SupportPortalException e, final String chatId) {
    LogManager.instance().log(this, Level.WARNING, "AI answer through the portal failed (chatId=%s, %s): %s", chatId, e.getCode(), e.getMessage());
    try {
      forwardEvent(output, new JSONObject().put("type", "error").put("code", e.getCode()).put("error", e.getMessage())
          .put("upgrade", e.getCode().startsWith("ai.")));
    } catch (final Exception ignored) {
      // The client is gone as well
    }
  }

  /**
   * Ends a stream that has already started with an {@code error} event, the only way left to tell the client the
   * answer is lost. Best effort: when the failure is the client's own connection, the write fails too and there is
   * nobody left to tell.
   */
  private void endStreamWithError(final OutputStream output, final Exception e, final String chatId) {
    if (e instanceof InterruptedException)
      Thread.currentThread().interrupt();

    final String code;
    final String message;
    if (e instanceof HttpTimeoutException) {
      code = "gateway_timeout";
      message = "AI service stopped responding before the answer was complete. Please try again later.";
    } else if (e instanceof IOException) {
      code = "gateway_interrupted";
      message = "The connection to the AI service was interrupted before the answer was complete. Please try again.";
    } else {
      code = "internal_error";
      message = "An unexpected error occurred. Please try again later.";
    }

    // Neutral wording: the IOException may come from the client's side of the relay as well as from the gateway's
    if (e instanceof IOException)
      LogManager.instance().log(this, Level.WARNING, "AI chat stream ended before the answer was complete (chatId=%s): %s",
          chatId, e.toString());
    else
      LogManager.instance().log(this, Level.WARNING, "AI chat stream ended before the answer was complete (chatId=%s)", e,
          chatId);

    try {
      forwardEvent(output, new JSONObject().put("type", "error").put("code", code).put("error", message));
    } catch (final Exception ignored) {
      // The client is gone as well
    }
  }

  private static void forwardEvent(final OutputStream output, final JSONObject event) throws IOException {
    output.write(("data: " + event + "\n\n").getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  /**
   * Delivers a tool execution result to the gateway's pending-tool registry so the
   * paused LLM loop can continue. Errors here are logged but not re-thrown - the
   * SSE reader keeps draining whatever the gateway sends (typically the gateway's
   * own timeout-driven error response).
   */
  private void postToolResult(final String sessionId, final String toolId, final String resultJson) {
    final JSONObject body = new JSONObject().put("id", toolId).put("result", resultJson);
    final HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(config.getGatewayUrl() + "/api/chat/tool_result/" + sessionId))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + config.getSubscriptionToken())
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .timeout(Duration.ofMillis(toolResultTimeoutMs))
        .build();
    try {
      final HttpResponse<String> resp = BoundedHttpExchange.send(HTTP_CLIENT, req, HttpResponse.BodyHandlers.ofString(),
          toolResultTimeoutMs);
      if (resp.statusCode() != 200) {
        LogManager.instance().log(this, Level.WARNING,
            "AI gateway tool_result POST returned %d: %s", resp.statusCode(), resp.body());
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Failed to deliver tool_result to AI gateway (session=%s, id=%s): %s",
          sessionId, toolId, e.getMessage());
    }
  }

  /**
   * Builds a standard (non-streaming) response and saves chat history.
   */
  private ExecutionResponse buildResponse(final JSONObject gatewayResponse, final JSONObject chat,
      final JSONArray messages, final String username) {
    // Add assistant response to chat
    final JSONObject assistantMsg = new JSONObject();
    assistantMsg.put("role", "assistant");
    assistantMsg.put("content", gatewayResponse.getString("response", ""));
    assistantMsg.put("timestamp", Instant.now().toString());

    final JSONArray commands = gatewayResponse.getJSONArray("commands", null);
    if (commands != null && commands.length() > 0)
      assistantMsg.put("commands", commands);

    messages.put(assistantMsg);

    // Update and save chat
    chat.put("messages", messages);
    chat.put("updated", Instant.now().toString());
    chatStorage.saveChat(username, chat);

    // Return response to Studio
    final JSONObject result = new JSONObject();
    result.put("chatId", chat.getString("id"));
    result.put("response", gatewayResponse.getString("response", ""));
    if (commands != null && commands.length() > 0)
      result.put("commands", commands);

    final JSONArray toolCalls = gatewayResponse.getJSONArray("toolCalls", null);
    if (toolCalls != null && toolCalls.length() > 0)
      result.put("toolCalls", toolCalls);

    return new ExecutionResponse(200, result.toString());
  }

  private JSONObject callGateway(final JSONObject requestBody) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()//
        .uri(URI.create(config.getGatewayUrl() + "/api/chat"))//
        .header("Content-Type", "application/json")//
        .header("Authorization", "Bearer " + config.getSubscriptionToken())//
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))//
        .timeout(Duration.ofMillis(gatewayTimeoutMs))//
        .build();

    final HttpResponse<String> response = BoundedHttpExchange.send(HTTP_CLIENT, request,
        HttpResponse.BodyHandlers.ofString(), gatewayTimeoutMs);

    if (response.statusCode() == 401 || response.statusCode() == 403) {
      // Parse the gateway error to get the specific code (token_invalid, token_expired, etc.)
      final JSONObject errBody = new JSONObject(response.body());
      final String code = errBody.getString("code", "token_invalid");
      final String errorMsg = errBody.getString("error", "Invalid or expired subscription token");
      final JSONObject errorResponse = new JSONObject();
      errorResponse.put("error", errorMsg);
      errorResponse.put("code", code);
      throw new AiTokenException(response.statusCode(), errorResponse.toString());
    }

    if (response.statusCode() != 200)
      throw new RuntimeException("Gateway returned status " + response.statusCode() + ": " + response.body());

    return new JSONObject(response.body());
  }
}
