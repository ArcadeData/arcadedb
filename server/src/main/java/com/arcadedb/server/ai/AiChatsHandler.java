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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.PathTemplateMatch;

import java.util.List;

/**
 * Handles chat CRUD operations:
 * - GET /api/v1/ai/chats - list all chats for the current user
 * - GET /api/v1/ai/chats/{id} - get a specific chat
 * - PUT /api/v1/ai/chats/{id} - update chat messages (e.g., after deleting a message)
 * - DELETE /api/v1/ai/chats/{id} - delete a specific chat
 */
public class AiChatsHandler extends AbstractServerHttpHandler {
  private final ChatStorage chatStorage;

  public AiChatsHandler(final HttpServer httpServer, final ChatStorage chatStorage) {
    super(httpServer);
    this.chatStorage = chatStorage;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final String username = user.getName();
    final String method = exchange.getRequestMethod().toString();

    // Extract chat ID from path if present
    final String chatId = getChatIdFromPath(exchange);

    if ("GET".equals(method)) {
      if (chatId != null)
        return getChat(username, chatId);
      else
        return listChats(username);
    } else if ("PUT".equals(method)) {
      if (chatId != null)
        return updateChat(username, chatId, payload);
      else
        return new ExecutionResponse(400, errorJson("Chat ID is required for PUT"));
    } else if ("DELETE".equals(method)) {
      if (chatId != null)
        return deleteChat(username, chatId);
      else
        return new ExecutionResponse(400, errorJson("Chat ID is required for DELETE"));
    }

    return new ExecutionResponse(405, errorJson("Method not allowed"));
  }

  private ExecutionResponse listChats(final String username) {
    final List<JSONObject> chats = chatStorage.listChats(username);
    final JSONObject result = new JSONObject();
    result.put("chats", new JSONArray(chats));
    return new ExecutionResponse(200, result.toString());
  }

  private ExecutionResponse getChat(final String username, final String chatId) {
    final JSONObject chat = chatStorage.getChat(username, chatId);
    if (chat == null)
      return new ExecutionResponse(404, errorJson("Chat not found"));
    return new ExecutionResponse(200, chat.toString());
  }

  private ExecutionResponse updateChat(final String username, final String chatId, final JSONObject payload) {
    if (payload == null)
      return new ExecutionResponse(400, errorJson("Request body is required"));

    final JSONObject chat = chatStorage.getChat(username, chatId);
    if (chat == null)
      return new ExecutionResponse(404, errorJson("Chat not found"));

    final JSONArray messages = payload.getJSONArray("messages", null);
    if (messages != null) {
      chat.put("messages", messages);
      chat.put("updated", java.time.Instant.now().toString());
      chatStorage.saveChat(username, chat);
    }

    return new ExecutionResponse(200, chat.toString());
  }

  private ExecutionResponse deleteChat(final String username, final String chatId) {
    final boolean deleted = chatStorage.deleteChat(username, chatId);
    if (!deleted)
      return new ExecutionResponse(404, errorJson("Chat not found"));
    return new ExecutionResponse(200, new JSONObject().put("deleted", true).toString());
  }

  private String getChatIdFromPath(final HttpServerExchange exchange) {
    final PathTemplateMatch match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
    if (match != null) {
      final String id = match.getParameters().get("id");
      if (id != null && !id.isEmpty())
        return id;
    }
    return null;
  }

  private static String errorJson(final String message) {
    return new JSONObject().put("error", message).toString();
  }
}
