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
}
