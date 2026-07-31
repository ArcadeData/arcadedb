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
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CoreApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  @Test
  void batchDeclaresStreamingBodyMediaTypes() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getOperationId()).isEqualTo("executeBatch");
    assertThat(post.getTags()).containsExactly("Batch");
    assertThat(post.getRequestBody().getContent().keySet())
        .as("the body is streamed JSONL or CSV and is never parsed as JSON")
        .containsExactlyInAnyOrder("application/x-ndjson", "application/jsonl", "text/csv");
  }

  @Test
  void batchDeclaresTheDocumentedQueryParameters() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getParameters().stream().map(Parameter::getName).toList())
        .contains("database", "batchSize", "refMode", "commitEvery", "vertexBatchSize",
            "expectedRecords", "ordinalBase", "idMapping", "lightEdges", "wal", "parallelFlush",
            "preAllocateEdgeChunks", "edgeListInitialSize", "bidirectional", "expectedEdgeCount",
            "commitRetries", "commitRetryDelayMs", "expectedVertexCount");
  }

  @Test
  void batchDeclaresTruncationTimeout() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getResponses().keySet())
        .as("a body that ends early is answered 408 with the partial-commit counts, never 200")
        .contains("200", "400", "408");
  }

  @Test
  void batchResponseCarriesTheCountsAndIdMapping() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BatchResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "verticesCreated", "edgesCreated", "elapsedMs", "bytesRead",
        "idMapping", "idMappingOmitted", "idMappingSize");
  }

  @Test
  void batchFailuresCarryThePartialCommitCountsNotTheGenericError() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BatchError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "exception", "requestId", "verticesCreated", "edgesCreated", "partialCommit");

    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    for (final String code : List.of("400", "408")) {
      assertThat(post.getResponses().get(code).getContent().get("application/json")
          .getSchema().get$ref())
          .as("a client cannot reconcile after a failed load without these counts, so %s "
              + "must not point at the generic error body", code)
          .isEqualTo("#/components/schemas/BatchError");
    }
  }

  @Test
  void progressResponseMatchesOperationProgressJson() {
    final Operation get = openAPI.getPaths().get("/api/v1/progress/{database}").getGet();
    assertThat(get.getOperationId()).isEqualTo("getOperationProgress");

    final Schema<?> entry = openAPI.getComponents().getSchemas().get("ProgressResponse");
    assertThat(entry.getProperties()).containsKey("result");
    final Schema<?> item = entry.getProperties().get("result").getItems();
    assertThat(item.getProperties().keySet()).containsExactlyInAnyOrder(
        "id", "database", "operation", "stepName", "stepIndex", "totalSteps",
        "done", "total", "percentage", "startedOn", "elapsedMs");
  }

  @Test
  void existingOperationIdsAreUnchanged() {
    assertThat(openAPI.getPaths().get("/api/v1/query/{database}").getPost().getOperationId())
        .isEqualTo("executeQueryPost");
    assertThat(openAPI.getPaths().get("/api/v1/server").getGet().getOperationId())
        .isEqualTo("getServerInfo");
  }

  @Test
  void checkDatabaseExistsDeclaresOnlyTheStatusCodesTheHandlerCanReturn() {
    final Operation get = openAPI.getPaths().get("/api/v1/exists/{database}").getGet();
    assertThat(get.getOperationId()).isEqualTo("checkDatabaseExists");

    assertThat(get.getResponses().keySet())
        .as("the handler returns 200 always, and 400 only when the database parameter is missing; "
            + "it never returns 404")
        .containsExactlyInAnyOrder("200", "400", "401", "500");

    final Schema<?> okSchema = get.getResponses().get("200").getContent().get("application/json").getSchema();
    assertThat(okSchema.get$ref())
        .as("a client needs a typed boolean to read the result, not just the status code")
        .isEqualTo("#/components/schemas/DatabaseExists");

    final Schema<?> databaseExists = openAPI.getComponents().getSchemas().get("DatabaseExists");
    assertThat(databaseExists.getProperties().keySet()).containsExactly("result");
  }

  @Test
  void checkHealthNeverDeclaresA200() {
    final Operation get = openAPI.getPaths().get("/api/v1/health").getGet();
    assertThat(get.getOperationId()).isEqualTo("checkHealth");
    assertThat(get.getResponses().keySet())
        .as("GetHealthHandler.execute only ever returns 204")
        .containsExactly("204");
  }

  @Test
  void checkReadyNeverDeclaresA200() {
    final Operation get = openAPI.getPaths().get("/api/v1/ready").getGet();
    assertThat(get.getOperationId()).isEqualTo("checkReady");
    assertThat(get.getResponses().keySet())
        .as("GetReadyHandler.execute only ever returns 204 or 503")
        .containsExactlyInAnyOrder("204", "503");
  }
}
