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
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7570: everything <em>about</em> a bulk load was documented - non-atomicity, the partial-commit counters,
 * {@code bytesRead} verification, the streaming acknowledgement protocol - except what to send. All three request
 * media types declared {@code type: string}, so none of {@code @type}, {@code @class}, {@code @id}, {@code @from} or
 * {@code @to} appeared anywhere in the contract and every client in every language had to reverse-engineer the
 * payload. The gRPC sibling {@code GraphBatchRecord} has been schematized since it shipped; only the HTTP encoding of
 * the same model was not.
 * <p>
 * The two line shapes are now a {@code oneOf} discriminated on {@code @type}, following the convention the spec
 * already uses for its NDJSON <em>responses</em> ({@code NdJsonQueryEvent}, {@code NdJsonBatchEvent}): the media-type
 * schema is the schema of one line, because OpenAPI 3.0 cannot say "newline-delimited instances of this" for a
 * request body.
 */
class Issue7570BatchRequestBodySchemaTest {
  private static final String BATCH_PATH = "/api/v1/batch/{database}";

  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  /** The defect as reported: three media types, one {@code type: string} between them. */
  @Test
  void theJsonLineMediaTypesReferenceTheLineSchemaInsteadOfAnOpaqueString() {
    final Map<String, MediaType> content = batchOperation().getRequestBody().getContent();

    assertThat(content).containsKeys("application/x-ndjson", "application/jsonl", "text/csv");

    for (final String mediaType : List.of("application/x-ndjson", "application/jsonl")) {
      final Schema<?> schema = content.get(mediaType).getSchema();
      assertThat(schema.get$ref())
          .as("%s must name the line schema so a generated client can type the payload", mediaType)
          .isEqualTo("#/components/schemas/BatchLine");
    }
  }

  /** A generated client picks the branch by the discriminator, so the mapping has to name every accepted spelling. */
  @Test
  void theLineSchemaIsAOneOfDiscriminatedOnAtType() {
    final Schema<?> line = schema("BatchLine");

    assertThat(line.getOneOf()).as("a vertex line and an edge line are different shapes").hasSize(2);
    assertThat(line.getOneOf()).extracting(Schema::get$ref)
        .containsExactly("#/components/schemas/BatchVertexLine", "#/components/schemas/BatchEdgeLine");

    assertThat(line.getDiscriminator()).isNotNull();
    assertThat(line.getDiscriminator().getPropertyName()).isEqualTo("@type");
    assertThat(line.getDiscriminator().getMapping())
        .as("the parsers accept the short spellings too, so the contract has to map them")
        .containsEntry("vertex", "#/components/schemas/BatchVertexLine")
        .containsEntry("v", "#/components/schemas/BatchVertexLine")
        .containsEntry("edge", "#/components/schemas/BatchEdgeLine")
        .containsEntry("e", "#/components/schemas/BatchEdgeLine");
  }

  /** The control keys the issue could not find anywhere in the contract. */
  @Test
  void theVertexLineNamesItsControlKeysAndRequiresTheOnesTheParserRequires() {
    final Schema<?> vertex = schema("BatchVertexLine");

    assertThat(vertex.getProperties()).containsKeys("@type", "@class", "@id");
    assertThat(vertex.getRequired())
        .as("JsonlBatchRecordStream.parseLine throws when either is missing")
        .containsExactlyInAnyOrder("@type", "@class");
    assertThat(vertex.getProperties().get("@type").getEnum()).containsExactly("vertex", "v");
  }

  @Test
  void theEdgeLineNamesItsEndpointsAndRequiresThem() {
    final Schema<?> edge = schema("BatchEdgeLine");

    assertThat(edge.getProperties()).containsKeys("@type", "@class", "@from", "@to");
    assertThat(edge.getRequired())
        .as("an edge with no @from or no @to is refused at the line")
        .containsExactlyInAnyOrder("@type", "@class", "@from", "@to");
    assertThat(edge.getProperties().get("@type").getEnum()).containsExactly("edge", "e");
  }

  /**
   * The half of the contract that turns the silent-corruption report into a document a client can read: properties
   * sit flat beside the control keys, and the nested form the {@code GraphBatchRecord} {@code properties} map invites
   * is refused rather than stored.
   */
  @Test
  void bothLineShapesSayPropertiesAreFlatAndThatTheNestedFormIsRefused() {
    for (final String name : List.of("BatchVertexLine", "BatchEdgeLine")) {
      final Schema<?> line = schema(name);

      assertThat(line.getAdditionalProperties())
          .as("%s must allow the arbitrary property keys a schemaless load carries", name)
          .isNotNull();
      assertThat(line.getDescription())
          .as("%s must say where the properties go", name)
          .contains("flat")
          .contains("properties");
    }
  }

  /** An unrecognised control key is refused now, so the contract has to say the namespace is reserved. */
  @Test
  void theRequestBodyDescribesTheReservedAtPrefixAndTheCsvGrammar() {
    final String description = batchOperation().getRequestBody().getDescription();

    assertThat(description)
        .as("the @ namespace is reserved and an unknown key in it is a 400")
        .contains("@");

    // Both parsers match the control keys with equals() and an exact switch, while only the CSV boolean literals
    // go through equalsIgnoreCase. A client cannot guess that asymmetry, and under the new refusal '@Type' is a
    // 400 rather than a silently stored property, so the contract has to state it.
    assertThat(description)
        .as("control-key matching is case-sensitive and the contract must say so")
        .contains("case-sensitiv");

    final Schema<?> csv = batchOperation().getRequestBody().getContent().get("text/csv").getSchema();
    assertThat(csv.getDescription())
        .as("'Header row followed by data rows' did not say how a header names @class or an edge's endpoints")
        .contains("@type")
        .contains("@class")
        .contains("---");
  }

  /** The 400 already existed; it now has to name this cause, or a client cannot tell it from a malformed line. */
  @Test
  void theBadRequestResponseNamesTheReservedKeyRefusal() {
    assertThat(batchOperation().getResponses().get("400").getDescription())
        .contains("reserved");
  }

  private Operation batchOperation() {
    return openAPI.getPaths().get(BATCH_PATH).getPost();
  }

  private Schema<?> schema(final String name) {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get(name);
    assertThat(schema).as("component schema %s", name).isNotNull();
    return schema;
  }
}
