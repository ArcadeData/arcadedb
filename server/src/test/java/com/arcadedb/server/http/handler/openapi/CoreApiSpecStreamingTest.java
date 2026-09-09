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
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7306: the streamed response is selected by {@code Accept}, and a negotiated alternative that the document
 * does not mention is invisible to every generated client - which would leave the feature as unreachable through
 * the published contract as it was before it existed.
 */
class CoreApiSpecStreamingTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  @Test
  void allThreeQueryAndCommandOperationsDeclareTheStreamingAcceptHeader() {
    for (final Operation operation : queryAndCommandOperations()) {
      final Parameter accept = operation.getParameters().stream()
          .filter(p -> "Accept".equals(p.getName()) && "header".equals(p.getIn()))
          .findFirst()
          .orElseThrow(() -> new AssertionError(operation.getOperationId() + " declares no Accept parameter"));

      assertThat(accept.getRequired())
          .as("%s must keep working with no Accept header at all", operation.getOperationId())
          .isFalse();
      assertThat(accept.getSchema().getEnum())
          .containsExactly("application/json", "application/x-ndjson", "application/jsonl");
    }
  }

  @Test
  void theSuccessResponseOffersTheStreamedAlternativeAlongsideTheBufferedObject() {
    for (final Operation operation : queryAndCommandOperations())
      assertThat(operation.getResponses().get("200").getContent().keySet())
          .as("%s must keep application/json as an answer, not replace it", operation.getOperationId())
          .containsExactlyInAnyOrder("application/json", "application/x-ndjson", "application/jsonl");
  }

  @Test
  void theStreamedAlternativeIsDescribedAsLinesNotAsAnObject() {
    final Operation post = openAPI.getPaths().get("/api/v1/query/{database}").getPost();
    assertThat(post.getResponses().get("200").getContent().get("application/x-ndjson").getSchema().getType())
        .isEqualTo("string");
    assertThat(post.getResponses().get("200").getContent().get("application/x-ndjson").getSchema().getDescription())
        .contains("summary")
        .contains("error");
  }

  private List<Operation> queryAndCommandOperations() {
    return List.of(
        openAPI.getPaths().get("/api/v1/query/{database}").getPost(),
        openAPI.getPaths().get("/api/v1/command/{database}").getPost(),
        openAPI.getPaths().get("/api/v1/query/{database}/{language}/{command}").getGet());
  }
}
