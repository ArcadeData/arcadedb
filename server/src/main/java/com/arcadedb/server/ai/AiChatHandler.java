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
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.logging.Level;

/**
 * POST /api/v1/ai/chat - Main AI chat endpoint.
 * Collects schema context, loads chat history, and forwards to the central gateway.
 */
public class AiChatHandler extends AbstractServerHttpHandler {
  private final ArcadeDBServer server;
  private final AiConfiguration config;
  private final ChatStorage     chatStorage;
  private final HttpClient      httpClient;

  public AiChatHandler(final HttpServer httpServer, final ArcadeDBServer server, final AiConfiguration config,
      final ChatStorage chatStorage) {
    super(httpServer);
    this.server = server;
    this.config = config;
    this.chatStorage = chatStorage;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
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
      userMsg.put("timestamp", java.time.Instant.now().toString());
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
        // Tool-calling path: create session for gateway to call back
        final HttpAuthSession authSession = httpServer.getAuthSessionManager().createSession(user);
        final String serverUrl = getServerUrl(exchange);
        gatewayRequest.put("arcadedb", new JSONObject()
            .put("url", serverUrl)
            .put("sessionId", authSession.token));
        gatewayRequest.put("schema", new JSONObject()); // minimal, required by gateway validation
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

      // Add assistant response to chat
      final JSONObject assistantMsg = new JSONObject();
      assistantMsg.put("role", "assistant");
      assistantMsg.put("content", gatewayResponse.getString("response", ""));
      assistantMsg.put("timestamp", java.time.Instant.now().toString());

      final JSONArray commands = gatewayResponse.getJSONArray("commands", null);
      if (commands != null && commands.length() > 0)
        assistantMsg.put("commands", commands);

      messages.put(assistantMsg);

      // Update and save chat
      chat.put("messages", messages);
      chat.put("updated", java.time.Instant.now().toString());
      chatStorage.saveChat(username, chat);

      // Return response to Studio
      final JSONObject result = new JSONObject();
      result.put("chatId", chat.getString("id"));
      result.put("response", gatewayResponse.getString("response", ""));
      if (commands != null && commands.length() > 0)
        result.put("commands", commands);

      return new ExecutionResponse(200, result.toString());

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

  private String getServerUrl(final HttpServerExchange exchange) {
    final String scheme = exchange.getRequestScheme();
    final String host = exchange.getHostAndPort();
    return scheme + "://" + host;
  }

  private JSONObject callGateway(final JSONObject requestBody) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()//
        .uri(URI.create(config.getGatewayUrl() + "/api/chat"))//
        .header("Content-Type", "application/json")//
        .header("Authorization", "Bearer " + config.getSubscriptionToken())//
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))//
        .timeout(Duration.ofSeconds(120))//
        .build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

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
