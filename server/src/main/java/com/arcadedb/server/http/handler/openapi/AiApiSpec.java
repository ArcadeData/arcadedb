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
import io.swagger.v3.oas.models.media.Content;
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
    openAPI.getPaths().addPathItem("/api/v1/ai/chat/stream", createChatStreamPath());
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
    openAPI.getComponents().addSchemas("AiChatStreamEvent", createChatStreamEventSchema());
    openAPI.getComponents().addSchemas("AiCommand", createCommandSchema());
    openAPI.getComponents().addSchemas("AiToolCall", createToolCallSchema());
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
            'chatId'. The server embeds the database schema and server metrics in the prompt \
            (review-first) and always answers with a single JSON body; the reply may carry SQL \
            commands the assistant proposes. For the client-orchestrated streaming protocol instead, \
            use POST /api/v1/ai/chat/stream.

            The assistant is a remote dependency: 503 means the gateway was unreachable and 504 that \
            it did not answer in time. Both are retryable. A rejected subscription token answers 502, \
            remapped from the gateway's own 401 or 403 so it cannot be mistaken for this request's own \
            authentication failing.""");
    post.setRequestBody(SpecBuilders.jsonBody("Chat message", "AiChatRequest", true));

    final ApiResponses responses = chatResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(
        "Assistant reply, as a single JSON body.", "AiChatResponse"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createChatStreamPath() {
    final Operation post = SpecBuilders.operation("streamChatWithAi", "AI",
        "Send a message to the AI assistant, streaming the reply",
        """
            Sends one message in the context of a database, optionally continuing an existing chat by \
            'chatId', using a client-orchestrated streaming protocol so the AI gateway never has to \
            open an inbound connection into the caller's network: the gateway emits 'tool_call' \
            events on the response stream, the server executes each tool locally, and posts the \
            result back to the gateway to resume the loop. The 200 response is always \
            'text/event-stream', never a JSON body; the closing 'done' event carries the same \
            'response', 'commands', and 'chatId' fields as POST /api/v1/ai/chat's JSON response. For \
            a single non-streaming JSON reply instead, use POST /api/v1/ai/chat.

            The gateway's own 'session' and 'tool_call' events are NOT forwarded: this server consumes \
            both - the first to learn where to post tool results, the second to run the tool - and emits \
            a 'tool_start'/'tool_end' pair around each run in their place. See AiChatStreamEvent.

            The assistant is a remote dependency: 503 means the gateway was unreachable and 504 that \
            it did not answer in time. Both are retryable. A rejected subscription token answers 502, \
            remapped from the gateway's own 401 or 403 so it cannot be mistaken for this request's own \
            authentication failing.""");
    post.setRequestBody(SpecBuilders.jsonBody("Chat message", "AiChatRequest", true));

    // Issue #7573. This used to be one 'type: string' whose whole specification was five event names in a
    // sentence, so no client could be generated for the stream at all - and two of the five names were wrong:
    // 'session' and 'tool_call' are the GATEWAY's events, which this server consumes and answers itself. What
    // reaches the caller is the pair it synthesizes around each tool it runs, plus the terminal event.
    final MediaType sseMediaType = new MediaType();
    sseMediaType.setSchema(SpecBuilders.ref("AiChatStreamEvent"));
    sseMediaType.setExample("""
        data: {"type":"tool_start","tool":"query","args":{"database":"demo","query":"SELECT FROM Person"}}

        data: {"type":"tool_end","tool":"query","args":{"database":"demo","query":"SELECT FROM Person"}}

        data: {"type":"done","response":"There are 42 people.","commands":[],"chatId":"c-17"}
        """);
    final ApiResponse ok = new ApiResponse();
    ok.setDescription("""
        Server-Sent Events stream. Each event is one 'data: ' line carrying a JSON object, followed by a blank \
        line; the schema below is the schema of that object. A complete stream ends with a 'done' event, and \
        exactly one: a stream that ends without it was cut short, and the reply it would have carried was never \
        persisted.""");
    ok.setContent(new Content().addMediaType("text/event-stream", sseMediaType));

    final ApiResponses responses = chatResponses();
    responses.addApiResponse("200", ok);
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  /** Error responses shared by both chat operations; the caller adds its own 200. */
  private ApiResponses chatResponses() {
    final ApiResponses responses = new ApiResponses();
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
    return responses;
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
    // GetAiConfigHandler writes all four on every answer, including when 'configured' is false (issue #7578).
    schema.setRequired(List.of("configured", "gatewayUrl", "currentProtocolVersion", "supportedProtocolVersions"));
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
    schema.setRequired(List.of("activated"));
    return schema;
  }

  private Schema<?> createChatRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Chat message");
    schema.addProperty("database", SpecBuilders.string(
        "Database the question is about. The caller must be authorized on it."));
    schema.addProperty("message", SpecBuilders.string("User message"));
    schema.addProperty("chatId", SpecBuilders.string(
        "Existing chat to continue. A new chat is created when omitted."));
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
        SpecBuilders.ref("AiCommand"),
        "SQL commands the assistant proposes. Absent when it proposes none."));
    schema.addProperty("toolCalls", SpecBuilders.arrayOf(
        SpecBuilders.ref("AiToolCall"),
        "Tools the assistant invoked while answering. Absent when it invoked none."));
    // The two arrays are written only when the assistant produced something to put in them; the reply itself
    // and the chat it belongs to are on every 200 (issue #7578).
    schema.setRequired(List.of("chatId", "response"));
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
    // Only 'error' is on every 400 from these two operations. The other three are written together, and only
    // on the protocol-version branch, which is what 'code' identifies (issue #7578).
    schema.setRequired(List.of("error"));
    return schema;
  }

  private Schema<?> createAnalyzeProfilerRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Profiler analysis request");
    schema.addProperty("profilerData", SpecBuilders.freeFormObject("""
        Profiler snapshot to analyse. An open map: the server forwards it to the assistant as it stands and \
        derives the schema of every database named inside it, rather than reading a fixed set of keys out of \
        it - so the shape follows whatever the profiler produced."""));
    schema.setRequired(List.of("profilerData"));
    return schema;
  }

  private Schema<?> createAnalyzeProfilerResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Profiler analysis");
    schema.addProperty("response", SpecBuilders.string("Assistant analysis"));
    schema.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.ref("AiCommand"),
        "Commands the assistant proposes. Absent when it proposes none."));
    schema.setRequired(List.of("response"));
    return schema;
  }

  private Schema<?> createChatListSchema() {
    final Schema<Object> schema = SpecBuilders.object("Stored chats");
    schema.addProperty("chats", SpecBuilders.arrayOf(
        SpecBuilders.ref("AiChat"), "Stored chat transcripts, metadata only (no 'messages'). Empty when this "
            + "user has stored none"));
    schema.setRequired(List.of("chats"));
    return schema;
  }

  private Schema<?> createChatSchema() {
    final Schema<Object> message = SpecBuilders.object("One chat message");
    final Schema<String> role = SpecBuilders.string("Who wrote the message");
    role.setEnum(List.of("user", "assistant"));
    message.addProperty("role", role);
    message.addProperty("content", SpecBuilders.string("Message text"));
    message.addProperty("timestamp", SpecBuilders.string("ISO-8601 instant"));
    message.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.ref("AiCommand"),
        "SQL commands the assistant proposed with this reply. Present only on an assistant message "
            + "that proposed at least one."));
    message.setRequired(List.of("role", "content", "timestamp"));

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
    // 'messages' is the one member the list response drops, which is the whole difference between the two
    // shapes this schema describes (issue #7578).
    schema.setRequired(List.of("id", "title", "database", "created", "updated"));
    return schema;
  }

  private Schema<?> createChatDeletedSchema() {
    final Schema<Object> schema = SpecBuilders.object("Deletion result");
    schema.addProperty("deleted", SpecBuilders.bool("Always true on a 200"));
    schema.setRequired(List.of("deleted"));
    return schema;
  }

  /**
   * One event of the streaming chat response (issue #7573).
   * <p>
   * Modelled on {@code CoreApiSpec.createNdJsonQueryEventSchema}, with one difference the transport forces:
   * an NDJSON line names its kind by which of three keys is present, while an SSE event names it in a
   * {@code type} member, so the discriminator is a property with a closed value set rather than a choice of
   * keys. The fields of every kind are declared side by side and the description says which kind carries
   * which, because a {@code oneOf} keyed on {@code type} would refuse the forward-compatibility case below.
   * <p>
   * The value set is closed for what THIS server synthesizes and open for what it relays: the default arm of
   * the handler's switch forwards any event kind the gateway sends on unchanged, so a client must ignore a
   * {@code type} it does not know rather than fail on it. That is stated here instead of being left for a
   * client author to discover when the gateway gains an event.
   */
  private Schema<?> createChatStreamEventSchema() {
    final Schema<String> type = SpecBuilders.string("""
        Which event this is. 'tool_start' and 'tool_end' bracket one tool the server ran locally, and 'done' \
        terminates a complete stream. The gateway's own 'session' and 'tool_call' events never appear: the \
        server consumes both and synthesizes the pair above in their place. Any OTHER value is an event the \
        gateway added and this server relays unchanged - ignore what you do not recognise rather than failing \
        on it.""");
    type.setEnum(List.of("tool_start", "tool_end", "done"));

    final Schema<Object> schema = SpecBuilders.object("""
        One event of the chat stream. 'type' says which one; the other members below belong to the kinds their \
        descriptions name, and an event carries only its own.""");
    schema.addProperty("type", type);
    schema.addProperty("tool", SpecBuilders.string(
        "Name of the tool being run, on 'tool_start' and 'tool_end'. The same name appears on both, which is "
            + "how a consumer pairs them"));
    schema.addProperty("args", SpecBuilders.freeFormObject("""
        Arguments the assistant passed to the tool, echoed identically on 'tool_start' and 'tool_end'. An open \
        map: the keys are the tool's own parameters."""));
    schema.addProperty("error", SpecBuilders.string("""
        Why the tool failed, on 'tool_end' only, and only when it did. Its absence is what says the run \
        succeeded - the stream does not carry the tool's result, which goes back to the gateway rather than to \
        the caller."""));
    schema.addProperty("response", SpecBuilders.string(
        "The assistant's reply, on 'done'. The same value POST /api/v1/ai/chat returns under this name"));
    schema.addProperty("commands", SpecBuilders.arrayOf(SpecBuilders.ref("AiCommand"),
        "SQL commands the assistant proposes, on 'done'. Absent or empty when it proposes none"));
    schema.addProperty("chatId", SpecBuilders.string("""
        Chat this exchange belongs to, on 'done'. Added by this server, not by the gateway, and the chat is \
        persisted before this event is written - so a client that has seen it can read the chat back \
        immediately."""));
    // Only 'type' is on every event: everything else belongs to one kind, and an event relayed from the
    // gateway carries neither its fields nor ours.
    schema.setRequired(List.of("type"));
    return schema;
  }

  /**
   * One SQL command the assistant proposes. Was a bare {@code type: object} at all four of its occurrences,
   * which a strict generator emits as an empty model - so the command a caller is meant to review and run was
   * unreachable through typed access (issue #7577).
   */
  private Schema<?> createCommandSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "One command the assistant proposes. Proposed only: the server never runs it, the caller does");
    schema.addProperty("command", SpecBuilders.string("The statement text"));
    schema.addProperty("language", SpecBuilders.string(
        "Query language the statement is written in. Treated as 'sql' when absent"));
    schema.addProperty("purpose", SpecBuilders.string(
        "One line saying what the statement is for, shown above it. Absent when the assistant gave none"));
    schema.setRequired(List.of("command"));
    return schema;
  }

  /** One tool the assistant invoked, as relayed in the buffered reply. */
  private Schema<?> createToolCallSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "One tool invocation, reported after the fact. The same pair of members the stream's 'tool_start' "
            + "carries");
    schema.addProperty("tool", SpecBuilders.string("Name of the tool that was run"));
    schema.addProperty("args", SpecBuilders.freeFormObject(
        "Arguments it was run with, keyed by the tool's own parameter names"));
    schema.addProperty("error", SpecBuilders.string("Why it failed. Absent on a run that succeeded"));
    schema.setRequired(List.of("tool"));
    return schema;
  }
}
