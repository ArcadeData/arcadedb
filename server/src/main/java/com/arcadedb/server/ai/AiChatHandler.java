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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

import java.io.*;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;

/**
 * POST /api/v1/ai/chat - Main AI chat endpoint.
 * Collects schema context, loads chat history, and forwards to the central gateway.
 *
 * <p><b>Auto mode</b> uses a client-orchestrated streaming protocol so the gateway
 * never has to open an inbound HTTP connection into the user's network. The gateway
 * emits SSE events ({@code session}, {@code tool_call}, {@code done}); this handler
 * executes each tool locally via {@link ToolDispatcher} and POSTs the result to
 * {@code /api/chat/tool_result/:sessionId} so the LLM loop resumes. Studio sees the
 * usual {@code tool_start}/{@code tool_end} events, synthesized locally.
 *
 * <p><b>Review-first mode</b> embeds the schema directly in the prompt and uses a
 * single non-streaming request to the gateway (no tool calls).
 */
public class AiChatHandler extends AbstractServerHttpHandler {
  // Static so all server instances in the JVM share one client. Each instance spawns
  // a SelectorManager NIO thread that survives until the client is GC'd; per-instance
  // clients leaked dozens of threads per server start under the integration-test suite.
  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private final ArcadeDBServer server;
  private final AiConfiguration config;
  private final ChatStorage     chatStorage;

  public AiChatHandler(final HttpServer httpServer, final ArcadeDBServer server, final AiConfiguration config,
      final ChatStorage chatStorage) {
    super(httpServer);
    this.server = server;
    this.config = config;
    this.chatStorage = chatStorage;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    if (!config.isConfigured())
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

    final String mode = payload.getString("mode", "auto");

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

      // Forward to gateway
      final JSONObject gatewayRequest = new JSONObject();
      gatewayRequest.put("message", message);
      gatewayRequest.put("history", history);
      gatewayRequest.put("database", database);
      gatewayRequest.put("hardwareId", AiActivateHandler.getHardwareId());
      gatewayRequest.put("serverVersion", Constants.getVersion());

      if ("auto".equals(mode)) {
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
        final MCPConfiguration mcpConfig = server.getMCPConfiguration();
        final JSONObject schemaArgs = new JSONObject().put("database", database);
        final JSONObject schema = GetSchemaTool.execute(server, user, schemaArgs, mcpConfig);
        final JSONObject serverInfo = ServerStatusTool.execute(server, user, new JSONObject(), mcpConfig);
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
        .timeout(Duration.ofMinutes(5))//
        .build();

    final HttpResponse<InputStream> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() == 401 || response.statusCode() == 403) {
      try (InputStream body = response.body()) {
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
      try (InputStream body = response.body()) {
        final String bodyStr = new String(body.readAllBytes(), StandardCharsets.UTF_8);
        throw new RuntimeException("Gateway returned status " + response.statusCode() + ": " + bodyStr);
      }
    }

    // Set SSE headers and start streaming
    exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
    exchange.getResponseHeaders().put(new HttpString("Cache-Control"), "no-cache");
    exchange.getResponseHeaders().put(new HttpString("X-Accel-Buffering"), "no");
    exchange.setStatusCode(200);

    // Ensure we're in blocking mode for OutputStream access
    if (!exchange.isBlocking())
      exchange.startBlocking();

    final OutputStream output = exchange.getOutputStream();
    JSONObject doneData = null;
    String gatewaySessionId = null;

    try (InputStream body = response.body();
         BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8)); output) {
      String line;
      while ((line = reader.readLine()) != null) {
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
            doneData = event;
            forwardEvent(output, event);
          }
          default ->
            // Forward any other event types unchanged (forward-compat).
            forwardEvent(output, event);
        }
      }
    }

    // Save chat history from the done event
    if (doneData != null) {
      final JSONObject assistantMsg = new JSONObject();
      assistantMsg.put("role", "assistant");
      assistantMsg.put("content", doneData.getString("response", ""));
      assistantMsg.put("timestamp", Instant.now().toString());

      final JSONArray commands = doneData.getJSONArray("commands", null);
      if (commands != null && commands.length() > 0)
        assistantMsg.put("commands", commands);

      messages.put(assistantMsg);
      chat.put("messages", messages);
      chat.put("updated", Instant.now().toString());
      // The SSE response has already been fully streamed and the output closed above, so the
      // exchange is committed: we can no longer turn a persistence failure into an HTTP error for
      // the client. Unlike the non-streaming path (buildResponse), let saveChat's failure only be
      // logged here - re-throwing would bubble up to execute()'s catch and attempt a second
      // response on the already-committed exchange.
      try {
        chatStorage.saveChat(username, chat);
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Failed to persist chat history after streaming response (chatId=%s): %s",
            chat.getString("id", null), e.getMessage());
      }
    }

    return null; // response already sent
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
        .timeout(Duration.ofSeconds(15))
        .build();
    try {
      final HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
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
        .timeout(Duration.ofSeconds(120))//
        .build();

    final HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

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
