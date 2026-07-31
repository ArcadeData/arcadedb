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
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class McpApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new McpApiSpec().contribute(openAPI);
  }

  @Test
  void mcpEndpointExposesPostOnly() {
    final PathItem item = openAPI.getPaths().get("/api/v1/mcp");
    assertThat(item.getPost().getOperationId()).isEqualTo("invokeMcp");
    assertThat(item.getPost().getTags()).containsExactly("MCP");
    assertThat(item.getGet()).as("the MCP handler rejects every method but POST").isNull();
    assertThat(item.getDelete()).isNull();
    assertThat(item.getPut()).isNull();
  }

  @Test
  void configExposesGetAndPostButNoOtherMethod() {
    final PathItem item = openAPI.getPaths().get("/api/v1/mcp/config");
    assertThat(item.getGet().getOperationId()).isEqualTo("getMcpConfig");
    assertThat(item.getPost().getOperationId()).isEqualTo("updateMcpConfig");
    assertThat(item.getPut()).isNull();
    assertThat(item.getDelete()).isNull();
  }

  @Test
  void configSchemaCarriesEveryPermissionFlagAndTheProfile() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("McpConfig");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "enabled", "allowReads", "allowInsert", "allowUpdate", "allowDelete",
        "allowSchemaChange", "allowAdmin", "profile", "allowedUsers", "allowedOrigins",
        "principalProfiles", "databases");
  }

  @Test
  void databaseOverridesAreKeyedByDatabaseName() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("McpConfig");
    final Schema<?> databases = schema.getProperties().get("databases");
    final Schema<?> perDatabase = (Schema<?>) databases.getAdditionalProperties();
    assertThat(perDatabase)
        .as("databases is keyed by database name")
        .isNotNull();
    assertThat(openAPI.getComponents().getSchemas().get("McpDatabaseOverride")
        .getProperties().keySet()).containsExactlyInAnyOrder(
        "allowReads", "allowInsert", "allowUpdate", "allowDelete",
        "allowSchemaChange", "allowAdmin", "allowedUsers");
  }

  @Test
  void configUpdateIsPartialAndAllOrNothing() {
    final Operation post = openAPI.getPaths().get("/api/v1/mcp/config").getPost();
    assertThat(post.getDescription())
        .as("a rejected field must leave the whole configuration untouched, and the description must say so")
        .contains("partial")
        .contains("rejected");
    assertThat(post.getResponses()).containsKeys("200", "400");
  }

  @Test
  void bothPathsDeclare405() {
    for (final String path : List.of("/api/v1/mcp", "/api/v1/mcp/config")) {
      final PathItem item = openAPI.getPaths().get(path);
      assertThat(item.readOperations()).allSatisfy(op ->
          assertThat(op.getResponses()).as("%s %s", path, op.getOperationId()).containsKey("405"));
    }
  }

  @Test
  void invokeMcpResponseCodesAreExact() {
    final Operation post = openAPI.getPaths().get("/api/v1/mcp").getPost();
    assertThat(post.getResponses().keySet())
        .as("invokeMcp must not regain 400 (no reachable path throws IllegalArgumentException past the "
            + "dispatcher's own catch blocks) and must keep 503 (MCP server disabled) and 202 "
            + "(notification-only batch accepted with no body)")
        .containsExactlyInAnyOrder("200", "202", "401", "403", "405", "500", "503");
  }

  @Test
  void configResponseCodesDifferBetweenGetAndPost() {
    final PathItem item = openAPI.getPaths().get("/api/v1/mcp/config");
    assertThat(item.getGet().getResponses().keySet())
        .as("getMcpConfig never validates a body, so it has no 400")
        .containsExactlyInAnyOrder("200", "401", "403", "405", "500");
    assertThat(item.getPost().getResponses().keySet())
        .as("updateMcpConfig rejects a missing body or a field that fails validation with 400")
        .containsExactlyInAnyOrder("200", "400", "401", "403", "405", "500");
  }

  @Test
  void invokeMcpDistinguishesJsonRpcErrorsFromTheStandardErrorSchema() {
    final Operation post = openAPI.getPaths().get("/api/v1/mcp").getPost();
    for (final String jsonRpcErrorCode : List.of("403", "405", "503"))
      assertThat(refOf(post.getResponses().get(jsonRpcErrorCode)))
          .as("%s is raised by the JSON-RPC dispatcher as a JSON-RPC error envelope, not the standard "
              + "ErrorResponse", jsonRpcErrorCode)
          .isNull();
    for (final String frameworkErrorCode : List.of("401", "500"))
      assertThat(refOf(post.getResponses().get(frameworkErrorCode)))
          .as("%s is raised by the shared HTTP framework and uses the standard ErrorResponse",
              frameworkErrorCode)
          .isEqualTo("#/components/schemas/ErrorResponse");
  }

  @Test
  void configOperationsAreRestrictedToTheRootUser() {
    final PathItem item = openAPI.getPaths().get("/api/v1/mcp/config");
    assertThat(item.getGet().getDescription())
        .as("getMcpConfig must state the root-user restriction, or the restriction can be silently dropped")
        .containsIgnoringCase("root user");
    assertThat(item.getPost().getDescription())
        .as("updateMcpConfig must state the root-user restriction, or the restriction can be silently dropped")
        .containsIgnoringCase("root user");
  }

  private static String refOf(final ApiResponse response) {
    return response.getContent().get(SpecBuilders.JSON).getSchema().get$ref();
  }
}
