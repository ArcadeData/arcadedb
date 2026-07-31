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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the AI assistant endpoints. Chat and profiler analysis proxy a remote gateway, so their
 * contract includes the gateway's unreachable and timeout outcomes as well as the local validation
 * failures. Chat additionally negotiates a protocol version and reports a mismatch in its own body
 * shape, carrying the versions the server accepts.
 */
public class AiApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/ai/config", createConfigPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/activate", createActivatePath());
    openAPI.getPaths().addPathItem("/api/v1/ai/chat", createChatPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/analyze-profiler", createAnalyzeProfilerPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/chats", createChatsPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/chats/{id}", createChatByIdPath());

    openAPI.getComponents().addSchemas("AiConfig", createConfigSchema());
    openAPI.getComponents().addSchemas("AiActivateRequest", createActivateRequestSchema());
    openAPI.getComponents().addSchemas("AiActivateResponse", createActivateResponseSchema());
    openAPI.getComponents().addSchemas("AiChatRequest", createChatRequestSchema());
    openAPI.getComponents().addSchemas("AiChatResponse", createChatResponseSchema());
    openAPI.getComponents().addSchemas("AiProtocolError", createProtocolErrorSchema());
    openAPI.getComponents().addSchemas("AiAnalyzeProfilerRequest", createAnalyzeProfilerRequestSchema());
    openAPI.getComponents().addSchemas("AiAnalyzeProfilerResponse", createAnalyzeProfilerResponseSchema());
    openAPI.getComponents().addSchemas("AiChatList", createChatListSchema());
    openAPI.getComponents().addSchemas("AiChat", createChatSchema());
    openAPI.getComponents().addSchemas("AiChatDeleted", createChatDeletedSchema());
  }

  private PathItem createConfigPath() {
    final Operation get = SpecBuilders.operation("getAiConfig", "AI",
        "Read the AI assistant configuration",
        """
            Reports whether the AI assistant is configured and which protocol versions this server \
            speaks. A client reads 'currentProtocolVersion' at start-up and either matches it or \
            picks the highest version it shares with 'supportedProtocolVersions'.""");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Assistant configuration", "AiConfig"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createActivatePath() {
    final Operation post = SpecBuilders.operation("activateAi", "AI",
        "Activate the AI assistant with a subscription key",
        """
            Exchanges a subscription key for an activation held by the server. The server sends its \
            version and a derived hardware id to the gateway as part of the exchange, and restricts \
            this operation to the root user because it writes server-wide configuration \
            (config/ai.json).""");
    post.setRequestBody(SpecBuilders.jsonBody("Subscription key", "AiActivateRequest", true));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Assistant activated", "AiActivateResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request: the request body or the subscription key is missing"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user may activate"));
    responses.addApiResponse("500", SpecBuilders.errorResponse(
        "Internal server error, including a failure to reach the gateway"));
    responses.addApiResponse("502", SpecBuilders.errorResponse(
        "The gateway rejected the subscription key. Its own 401 or 403 is remapped to 502 here."));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "Passed through verbatim when the gateway itself answers 503"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createChatPath() {
    final Operation post = SpecBuilders.operation("chatWithAi", "AI",
        "Send a message to the AI assistant",
        """
            Sends one message in the context of a database, optionally continuing an existing chat by \
            'chatId'. The server supplies the database schema and, in the richer modes, server \
            metrics to the gateway. The reply may carry SQL commands the assistant proposes and the \
            tool calls it made.

            In the default 'auto' mode the 200 response is a 'text/event-stream', not a JSON body; \
            the JSON response described below is returned only when 'mode' is set to something other \
            than 'auto'.

            The assistant is a remote dependency: 503 means the gateway was unreachable and 504 that \
            it did not answer in time. Both are retryable. A rejected subscription token answers 502, \
            remapped from the gateway's own 401 or 403 so it cannot be mistaken for this request's own \
            authentication failing.""");
    post.setRequestBody(SpecBuilders.jsonBody("Chat message", "AiChatRequest", true));

    final ApiResponse ok = SpecBuilders.jsonResponse(
        """
            Assistant reply, as a single JSON body. Sent only when 'mode' is not 'auto' (the \
            review-first path). In the default 'auto' mode the 200 response is instead a \
            'text/event-stream' of 'session', 'tool_call', 'tool_start', 'tool_end', and 'done' \
            events; the closing 'done' event carries the same 'response', 'commands', and 'chatId' \
            fields as this JSON body.""",
        "AiChatResponse");
    final MediaType sseMediaType = new MediaType();
    sseMediaType.setSchema(new Schema<>().type("string").description(
        "Server-Sent Events stream: 'session', 'tool_call', 'tool_start', 'tool_end', 'done'"));
    ok.getContent().addMediaType("text/event-stream", sseMediaType);

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", ok);
    responses.addApiResponse("400", SpecBuilders.jsonResponse(
        """
            Bad request: the assistant is not configured, the body or a required field is missing, \
            or the requested protocol version is unsupported. On a version mismatch the body carries \
            'code' set to 'protocol_unsupported' plus the versions this server accepts; the other \
            causes carry only 'error'.""",
        "AiProtocolError"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse(
        "Forbidden: the user cannot access the requested database"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Chat not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("502", SpecBuilders.errorResponse(
        "The gateway rejected the stored subscription token; remapped from the gateway's own 401 "
            + "or 403"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "AI gateway unreachable, reported with code 'gateway_unreachable'"));
    responses.addApiResponse("504", SpecBuilders.errorResponse(
        "AI gateway timed out, reported with code 'gateway_timeout'"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createAnalyzeProfilerPath() {
    final Operation post = SpecBuilders.operation("analyzeProfilerWithAi", "AI",
        "Analyse a profiler snapshot",
        """
            Submits a profiler snapshot and returns the assistant's analysis plus any SQL commands it \
            proposes. The server derives the schema of every database referenced inside \
            'profilerData' and forwards it to the assistant automatically; the client does not supply \
            schemas directly.""");
    post.setRequestBody(SpecBuilders.jsonBody(
        "Profiler snapshot", "AiAnalyzeProfilerRequest", true));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(
        "Analysis", "AiAnalyzeProfilerResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request, or the assistant is not configured"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("502", SpecBuilders.errorResponse(
        "The gateway rejected the stored subscription token; remapped from the gateway's own 401 "
            + "or 403"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "AI gateway unreachable, reported with code 'gateway_unreachable'"));
    responses.addApiResponse("504", SpecBuilders.errorResponse(
        "AI gateway timed out, reported with code 'gateway_timeout'"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createChatsPath() {
    final Operation get = SpecBuilders.operation("listAiChats", "AI",
        "List stored chats",
        "Lists the chat transcripts this server has stored for the current user, newest first.");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Stored chats", "AiChatList"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createChatByIdPath() {
    final PathItem pathItem = new PathItem();

    final Operation get = SpecBuilders.operation("getAiChat", "AI",
        "Read one stored chat",
        "Returns one stored chat transcript with its messages.");
    get.addParametersItem(SpecBuilders.pathParam("id", "Chat identifier"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Chat transcript", "AiChat"),
        "401", "403", "404", "500"));
    pathItem.setGet(get);

    final Operation put = SpecBuilders.operation("updateAiChat", "AI",
        "Replace the messages of a stored chat",
        """
            Replaces the message list of a stored chat and stamps its update time. Only 'messages' is \
            read from the body; 'id', 'title', 'database', and 'created' are ignored if present.""");
    put.addParametersItem(SpecBuilders.pathParam("id", "Chat identifier"));
    put.setRequestBody(SpecBuilders.jsonBody(
        "Replacement messages", "AiChat", true));
    put.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Updated chat", "AiChat"),
        "400", "401", "403", "404", "500"));
    pathItem.setPut(put);

    final Operation delete = SpecBuilders.operation("deleteAiChat", "AI",
        "Delete a stored chat",
        "Deletes one stored chat transcript.");
    delete.addParametersItem(SpecBuilders.pathParam("id", "Chat identifier"));
    delete.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Chat deleted", "AiChatDeleted"),
        "401", "403", "404", "500"));
    pathItem.setDelete(delete);

    return pathItem;
  }

  private Schema<?> createConfigSchema() {
    final Schema<Object> schema = SpecBuilders.object("AI assistant configuration");
    schema.addProperty("configured", SpecBuilders.bool(
        "True once a subscription has been activated"));
    schema.addProperty("gatewayUrl", SpecBuilders.string("AI gateway endpoint"));
    schema.addProperty("currentProtocolVersion", SpecBuilders.integer(
        "Protocol version this server prefers"));
    schema.addProperty("supportedProtocolVersions", SpecBuilders.arrayOf(
        SpecBuilders.integer("Protocol version"), "Every version this server accepts"));
    return schema;
  }

  private Schema<?> createActivateRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Activation request");
    schema.addProperty("subscriptionKey", SpecBuilders.string("Subscription key"));
    schema.setRequired(List.of("subscriptionKey"));
    return schema;
  }

  private Schema<?> createActivateResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Activation result");
    schema.addProperty("activated", SpecBuilders.bool("Always true on a 200"));
    return schema;
  }

  private Schema<?> createChatRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Chat message");
    schema.addProperty("database", SpecBuilders.string(
        "Database the question is about. The caller must be authorized on it."));
    schema.addProperty("message", SpecBuilders.string("User message"));
    schema.addProperty("chatId", SpecBuilders.string(
        "Existing chat to continue. A new chat is created when omitted."));
    schema.addProperty("mode", SpecBuilders.string(
        "How the response is delivered. 'auto' (the default) streams Server-Sent Events, executing "
            + "tools locally as the assistant requests them. Any other value returns a single "
            + "non-streaming JSON body matching AiChatResponse."));
    schema.addProperty("protocolVersion", SpecBuilders.integer(
        "Protocol version the client speaks. Rejected with 'protocol_unsupported' when unknown. "
            + "Defaults to 1 when omitted."));
    schema.setRequired(List.of("database", "message"));
    return schema;
  }

  private Schema<?> createChatResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Assistant reply");
    schema.addProperty("chatId", SpecBuilders.string(
        "Chat this exchange belongs to, for continuing the conversation"));
    schema.addProperty("response", SpecBuilders.string("Assistant message"));
    schema.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.object("A proposed command"),
        "SQL commands the assistant proposes. Absent when it proposes none."));
    schema.addProperty("toolCalls", SpecBuilders.arrayOf(
        SpecBuilders.object("A tool invocation"),
        "Tools the assistant invoked while answering. Absent when it invoked none."));
    return schema;
  }

  private Schema<?> createProtocolErrorSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "Rejected chat request. Carries the negotiation fields when the protocol version is at fault.");
    schema.addProperty("error", SpecBuilders.string("Why the request was rejected"));
    schema.addProperty("code", SpecBuilders.string(
        "Machine-readable cause, 'protocol_unsupported' for a version mismatch"));
    schema.addProperty("currentProtocolVersion", SpecBuilders.integer(
        "Protocol version this server prefers"));
    schema.addProperty("supportedProtocolVersions", SpecBuilders.arrayOf(
        SpecBuilders.integer("Protocol version"), "Every version this server accepts"));
    return schema;
  }

  private Schema<?> createAnalyzeProfilerRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Profiler analysis request");
    schema.addProperty("profilerData", SpecBuilders.object("Profiler snapshot to analyse"));
    schema.setRequired(List.of("profilerData"));
    return schema;
  }

  private Schema<?> createAnalyzeProfilerResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Profiler analysis");
    schema.addProperty("response", SpecBuilders.string("Assistant analysis"));
    schema.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.object("A proposed command"),
        "Commands the assistant proposes. Absent when it proposes none."));
    return schema;
  }

  private Schema<?> createChatListSchema() {
    final Schema<Object> schema = SpecBuilders.object("Stored chats");
    schema.addProperty("chats", SpecBuilders.arrayOf(
        SpecBuilders.ref("AiChat"), "Stored chat transcripts, metadata only (no 'messages')"));
    return schema;
  }

  private Schema<?> createChatSchema() {
    final Schema<Object> message = SpecBuilders.object("One chat message");
    message.addProperty("role", SpecBuilders.string("'user' or the assistant role"));
    message.addProperty("content", SpecBuilders.string("Message text"));
    message.addProperty("timestamp", SpecBuilders.string("ISO-8601 instant"));
    message.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.object("A proposed command"),
        "SQL commands the assistant proposed with this reply. Present only on an assistant message "
            + "that proposed at least one."));

    final Schema<Object> schema = SpecBuilders.object(
        "One chat transcript. GET /api/v1/ai/chats returns this shape without 'messages'; "
            + "GET /api/v1/ai/chats/{id} returns it in full.");
    schema.addProperty("id", SpecBuilders.string("Chat identifier"));
    schema.addProperty("title", SpecBuilders.string("Chat title, generated from the first user message"));
    schema.addProperty("database", SpecBuilders.string("Database this chat is about"));
    schema.addProperty("created", SpecBuilders.string("ISO-8601 instant the chat was created"));
    schema.addProperty("updated", SpecBuilders.string("ISO-8601 instant of the last change"));
    schema.addProperty("messages", SpecBuilders.arrayOf(message,
        "Messages, oldest first. Omitted from the /chats list response."));
    return schema;
  }

  private Schema<?> createChatDeletedSchema() {
    final Schema<Object> schema = SpecBuilders.object("Deletion result");
    schema.addProperty("deleted", SpecBuilders.bool("Always true on a 200"));
    return schema;
  }
}
