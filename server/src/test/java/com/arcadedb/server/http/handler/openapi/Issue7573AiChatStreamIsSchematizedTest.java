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
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7573AiChatStreamIsSchematizedTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new AiApiSpec().contribute(openAPI);
  }

  private MediaType sseMediaType() {
    return openAPI.getPaths().get("/api/v1/ai/chat/stream").getPost()
        .getResponses().get("200").getContent().get("text/event-stream");
  }

  private Schema<?> eventSchema() {
    return openAPI.getComponents().getSchemas().get("AiChatStreamEvent");
  }

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

  /**
   * The event kinds are the ones this server synthesizes, not the gateway's. The two removed names are asserted
   * by absence, because that is the half of the fix a future edit would most easily undo by copying the old
   * sentence back.
   */
  @Test
  void theEventKindsAreTheOnesThisServerEmits() {
    final List<Object> kinds = new ArrayList<>(
        ((Schema<?>) eventSchema().getProperties().get("type")).getEnum());

    assertThat(kinds).containsExactly("tool_start", "tool_end", "done");
    assertThat(kinds)
        .as("'session' and 'tool_call' are the gateway's events: AiChatHandler consumes both and they never "
            + "reach the caller, so naming them here sends a client author waiting for events that never arrive")
        .doesNotContain("session", "tool_call");
  }

  /**
   * Every member each kind carries is declared. The list is what the handler writes: {@code tool_start} and
   * {@code tool_end} are built from {@code tool} and {@code args} with {@code error} added only on a failure,
   * and {@code done} is the gateway's event with {@code chatId} injected.
   */
  @Test
  void everyFieldTheHandlerWritesIsDeclared() {
    assertThat(eventSchema().getProperties().keySet())
        .containsExactlyInAnyOrder("type", "tool", "args", "error", "response", "commands", "chatId");
  }

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
