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

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AiApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new AiApiSpec().contribute(openAPI);
  }

  @Test
  void allNineOperationsArePresentAndTaggedAi() {
    final Map<String, String> expected = Map.of(
        "/api/v1/ai/config", "getAiConfig",
        "/api/v1/ai/activate", "activateAi",
        "/api/v1/ai/chat", "chatWithAi",
        "/api/v1/ai/chat/stream", "streamChatWithAi",
        "/api/v1/ai/analyze-profiler", "analyzeProfilerWithAi",
        "/api/v1/ai/chats", "listAiChats");
    expected.forEach((path, operationId) -> {
      final PathItem item = openAPI.getPaths().get(path);
      final Operation op = item.getGet() != null ? item.getGet() : item.getPost();
      assertThat(op.getOperationId()).as("%s", path).isEqualTo(operationId);
      assertThat(op.getTags()).as("%s", path).containsExactly("AI");
    });

    final PathItem chat = openAPI.getPaths().get("/api/v1/ai/chats/{id}");
    assertThat(chat.getGet().getOperationId()).isEqualTo("getAiChat");
    assertThat(chat.getPut().getOperationId()).isEqualTo("updateAiChat");
    assertThat(chat.getDelete().getOperationId()).isEqualTo("deleteAiChat");
    assertThat(chat.getPost()).as("the chats resource exposes no POST").isNull();
  }

  @Test
  void configReportsProtocolNegotiationFields() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiConfig");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "configured", "gatewayUrl", "currentProtocolVersion", "supportedProtocolVersions");
  }

  @Test
  void activateRequiresASubscriptionKey() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiActivateRequest");
    assertThat(schema.getRequired()).containsExactly("subscriptionKey");
  }

  @Test
  void chatRequiresDatabaseAndMessage() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiChatRequest");
    assertThat(schema.getRequired()).containsExactlyInAnyOrder("database", "message");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "database", "message", "chatId", "protocolVersion");
  }

  @Test
  void chatResponseCarriesChatIdAndOptionalCommands() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiChatResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "chatId", "response", "commands", "toolCalls");
  }

  @Test
  void chatDeclaresTheGatewayFailureStatuses() {
    for (final String path : new String[] { "/api/v1/ai/chat", "/api/v1/ai/chat/stream" }) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      assertThat(post.getResponses().keySet())
          .as("%s: the gateway is a remote dependency, so 503 and 504 are part of the contract", path)
          .contains("200", "400", "403", "404", "503", "504");
    }
  }

  @Test
  void protocolMismatchIsItsOwnSchemaNotTheGenericError() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiProtocolError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "code", "currentProtocolVersion", "supportedProtocolVersions");
    assertThat(openAPI.getPaths().get("/api/v1/ai/chat").getPost().getResponses().get("400")
        .getContent().get("application/json").getSchema().get$ref())
        .isEqualTo("#/components/schemas/AiProtocolError");
  }

  @Test
  void analyzeProfilerRequiresProfilerData() {
    // AiAnalyzeProfilerHandler (server/src/main/java/com/arcadedb/server/ai/AiAnalyzeProfilerHandler.java)
    // reads only "profilerData" from the client payload (line 79). "schemas" is a server-computed
    // field it adds to the *outbound* gateway request (lines 85-90) from the databases referenced
    // inside profilerData - it is never read from the client's request. So the client-facing request
    // schema has exactly one property, not two.
    final Schema<?> request = openAPI.getComponents().getSchemas()
        .get("AiAnalyzeProfilerRequest");
    assertThat(request.getRequired()).containsExactly("profilerData");
    assertThat(request.getProperties().keySet())
        .containsExactlyInAnyOrder("profilerData");
    assertThat(openAPI.getComponents().getSchemas().get("AiAnalyzeProfilerResponse")
        .getProperties().keySet()).containsExactlyInAnyOrder("response", "commands");
  }

  @Test
  void chatsByIdTakeThePathParameter() {
    final PathItem item = openAPI.getPaths().get("/api/v1/ai/chats/{id}");
    for (final Operation op : item.readOperations()) {
      assertThat(op.getParameters().stream().map(Parameter::getName).toList())
          .as("%s", op.getOperationId()).contains("id");
    }
  }

  @Test
  void deleteAnswersADeletedFlag() {
    assertThat(openAPI.getComponents().getSchemas().get("AiChatDeleted")
        .getProperties().keySet()).containsExactly("deleted");
  }

  @Test
  void gatewayProxyingOperationsDeclare502ForARejectedToken() {
    // AiTokenException (server/src/main/java/com/arcadedb/server/ai/AiTokenException.java, lines
    // 43-46) always maps the gateway's own 401/403 to a client-facing 502, on all three operations
    // that talk to the gateway: chatWithAi, analyzeProfilerWithAi (both catch AiTokenException
    // directly) and activateAi (which inlines the same 401/403 -> 502 remap).
    assertThat(openAPI.getPaths().get("/api/v1/ai/chat").getPost().getResponses().keySet())
        .as("chatWithAi").contains("502");
    assertThat(openAPI.getPaths().get("/api/v1/ai/chat/stream").getPost().getResponses().keySet())
        .as("streamChatWithAi").contains("502");
    assertThat(openAPI.getPaths().get("/api/v1/ai/analyze-profiler").getPost().getResponses().keySet())
        .as("analyzeProfilerWithAi").contains("502");
    assertThat(openAPI.getPaths().get("/api/v1/ai/activate").getPost().getResponses().keySet())
        .as("activateAi").contains("502");
  }

  @Test
  void aiChatDeclaresAllSixStoredFields() {
    // ChatStorage.createNewChat (server/src/main/java/com/arcadedb/server/ai/ChatStorage.java,
    // lines 157-167) stores id, title, database, created, updated, and messages.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiChat");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "id", "title", "database", "created", "updated", "messages");
  }

  @Test
  void chatMessagesCarryOptionalCommands() {
    // AiChatHandler puts "commands" onto a persisted assistant message when the gateway proposed
    // at least one (buildResponse, lines 433-435 and 448-449; handleStreamingRequest, lines 365-367).
    final Schema<?> chat = openAPI.getComponents().getSchemas().get("AiChat");
    final Schema<?> messages = chat.getProperties().get("messages");
    final Schema<?> message = messages.getItems();
    assertThat(message.getProperties().keySet()).contains("commands");
  }

  @Test
  void chatDeclaresOnlyJsonContentOnItsTwoHundred() {
    // #6558: OpenAPI's content map selects on the Accept header, not on a request-body field
    // ('mode' used to pick JSON vs SSE for the SAME operation, which no generator can bind
    // correctly). POST /api/v1/ai/chat is now always JSON, so its 200 must declare exactly one
    // content type.
    final ApiResponse ok = openAPI.getPaths().get("/api/v1/ai/chat").getPost().getResponses().get("200");
    assertThat(ok.getContent().keySet()).containsExactly("application/json");
  }

  @Test
  void chatStreamDeclaresOnlyEventStreamContent() {
    // The other half of the #6558 split: POST /api/v1/ai/chat/stream is always SSE, so its 200
    // must declare exactly one content type too, and it must not be application/json.
    final ApiResponse ok = openAPI.getPaths().get("/api/v1/ai/chat/stream").getPost().getResponses().get("200");
    assertThat(ok.getContent().keySet()).containsExactly("text/event-stream");
  }

  @Test
  void analyzeProfilerRequestNeverDeclaresSchemas() {
    // AiAnalyzeProfilerHandler.execute (line 79) reads only "profilerData" from the client payload.
    // "schemas" is computed server-side (collectDatabaseSchemas, lines 133-176) and added only to
    // the outbound gateway request (lines 85-90); it is never read from the client's request, so it
    // must not appear as a request property.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiAnalyzeProfilerRequest");
    assertThat(schema.getProperties().keySet()).containsExactly("profilerData");
  }
}
