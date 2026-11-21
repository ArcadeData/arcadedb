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
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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
    // AiConfiguration.toJSON writes the first four; AiConfigHandler adds 'source' and 'portal' when the server has the
    // customer portal client.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiConfig");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "configured", "gatewayUrl", "currentProtocolVersion", "supportedProtocolVersions", "source", "portal");
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
        "chatId", "response", "commands", "charts", "toolCalls");
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
  void chatMessagesDeclareEveryMemberTheHandlerStores() {
    // AiChatHandler stores role, content and timestamp on every message, and commands and charts (AiCharts.clean) on an
    // assistant message that has any: buildResponse and the portal streaming path both persist the charts, so a
    // GET /api/v1/ai/chats/{id} returns them.
    final Schema<?> chat = openAPI.getComponents().getSchemas().get("AiChat");
    final Schema<?> messages = chat.getProperties().get("messages");
    final Schema<?> message = messages.getItems();
    assertThat(message.getProperties().keySet()).containsExactlyInAnyOrder("role", "content", "timestamp", "commands", "charts");
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

  /*
   * Issue #7573: the streaming chat endpoint declared its whole 200 as one {@code type: string} whose entire
   * specification was five event names in a sentence. Nothing said what any of them carried - no field names for a
   * {@code tool_call}, no shape for {@code session}, no statement of which event is terminal - so no client could
   * be generated for the stream at all.
   * <p>
   * Two of those five names were also wrong, which is what reading the handler to schematize it turned up:
   * {@code session} and {@code tool_call} are the <em>gateway's</em> events. {@code AiChatHandler} consumes both -
   * the first to learn where to post tool results, the second to run the tool locally - and emits a
   * {@code tool_start}/{@code tool_end} pair in their place. A client author following the old description waited
   * for events that never arrive.
   * <p>
   * {@code AiChatHandlerStreamingTest} is the behaviour half: it drives the endpoint against a fake gateway and
   * checks that every event it actually reads is one this schema describes.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7573">issue #7573</a>
   */
  private MediaType sseMediaType() {
    return openAPI.getPaths().get("/api/v1/ai/chat/stream").getPost()
        .getResponses().get("200").getContent().get("text/event-stream");
  }

  private Schema<?> eventSchema() {
    return openAPI.getComponents().getSchemas().get("AiChatStreamEvent");
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /** The defect itself: the payload was one opaque string. */
  @Test
  void theStreamNamesAComponentSchemaInsteadOfBeingOneOpaqueString() {
    final Schema<?> declared = sseMediaType().getSchema();

    assertThat(declared.getType())
        .as("a 'type: string' says nothing a client can parse against")
        .isNotEqualTo("string");
    assertThat(declared.get$ref())
        .as("the media type names the event schema, the way NdJsonQueryEvent is named on /query")
        .isEqualTo("#/components/schemas/AiChatStreamEvent");
    assertThat(eventSchema()).as("and that component exists").isNotNull();
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /**
   * The event kinds are the ones this server synthesizes, not the gateway's. The two removed names are asserted
   * by absence, because that is the half of the fix a future edit would most easily undo by copying the old
   * sentence back.
   */
  @Test
  void theEventKindsAreTheOnesThisServerEmits() {
    final List<Object> kinds = new ArrayList<>(
        ((Schema<?>) eventSchema().getProperties().get("type")).getEnum());

    // 'error' ends a stream cut short after it started (issue #8642). 'delta' and 'reset' are the incremental reply
    // AiPortalChat relays when the answer comes through the customer portal.
    assertThat(kinds).containsExactly("tool_start", "tool_end", "delta", "reset", "done", "error");
    assertThat(kinds)
        .as("'session' and 'tool_call' are the gateway's events: AiChatHandler consumes both and they never "
            + "reach the caller, so naming them here sends a client author waiting for events that never arrive")
        .doesNotContain("session", "tool_call");
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /**
   * Every member each kind carries is declared. The list is what the handler writes: {@code tool_start} and
   * {@code tool_end} are built from {@code tool} and {@code args} with {@code error} added only on a failure,
   * and {@code done} is the gateway's event with {@code chatId} injected. Through the customer portal,
   * {@code delta} carries {@code text}, {@code done} adds {@code charts} and {@code usage}, and a plan refusal's
   * {@code error} adds {@code upgrade}.
   */
  @Test
  void everyFieldTheHandlerWritesIsDeclared() {
    assertThat(eventSchema().getProperties().keySet())
        .containsExactlyInAnyOrder("type", "tool", "args", "error", "response", "commands", "chatId", "code", "text", "upgrade",
            "usage", "charts");
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /**
   * Only {@code type} is on every event, and that is a consequence of the forward-compatibility rule rather than
   * an oversight: the handler's default arm relays an event kind it does not know unchanged, so such an event
   * carries neither our members nor a {@code type} from the enum. Requiring anything else would make the
   * document refuse a stream the server legitimately produces.
   */
  @Test
  void onlyTheDiscriminatorIsRequiredBecauseUnknownKindsAreRelayedUnchanged() {
    assertThat(eventSchema().getRequired()).containsExactly("type");

    assertThat(((Schema<?>) eventSchema().getProperties().get("type")).getDescription())
        .as("and the relaying has to be stated, or a client fails on the first event the gateway adds")
        .contains("relays");
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /**
   * Which event terminates a complete stream, which the old description did not say at all. A consumer that does
   * not know cannot tell a finished answer from a connection that dropped - and the reply a cut stream would
   * have carried was never persisted, so the difference matters.
   */
  @Test
  void theResponseSaysWhichEventTerminatesACompleteStream() {
    assertThat(openAPI.getPaths().get("/api/v1/ai/chat/stream").getPost()
        .getResponses().get("200").getDescription())
        .contains("'done'")
        .contains("cut short");
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /**
   * An example, because the SSE framing - {@code data: } plus a blank line - is not something an OpenAPI schema
   * can express, and the schema alone would leave a reader to guess it.
   */
  @Test
  void theMediaTypeCarriesAnExampleShowingTheSseFraming() {
    assertThat((String) sseMediaType().getExample())
        .contains("data: ")
        .contains("\"type\":\"tool_start\"")
        .contains("\"type\":\"done\"");
  }

  // Regression: #7573 the AI chat SSE stream is schematized with the events this server emits
  /**
   * The proposed commands were a bare {@code object} at four separate places. Modelling the stream meant
   * modelling them, so the buffered reply, the stored transcript and the {@code done} event now name one
   * component instead of four empty models (issue #7577).
   */
  @Test
  void theProposedCommandsAreOneSchemaSharedByEveryPlaceTheyAppear() {
    final Schema<?> command = openAPI.getComponents().getSchemas().get("AiCommand");
    assertThat(command.getProperties().keySet()).containsExactlyInAnyOrder("command", "language", "purpose");
    assertThat(command.getRequired())
        .as("a proposal with no statement in it is not a proposal")
        .containsExactly("command");

    for (final Schema<?> owner : List.of(
        (Schema<?>) eventSchema().getProperties().get("commands"),
        (Schema<?>) openAPI.getComponents().getSchemas().get("AiChatResponse").getProperties().get("commands"),
        (Schema<?>) openAPI.getComponents().getSchemas().get("AiAnalyzeProfilerResponse").getProperties()
            .get("commands")))
      assertThat(owner.getItems().get$ref())
          .as("every commands array names the same component")
          .isEqualTo("#/components/schemas/AiCommand");
  }
}
